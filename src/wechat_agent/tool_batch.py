"""Framework-independent validation of independent commit fields and batch items."""
import inspect
from typing import get_args, get_origin, get_type_hints

from pydantic import TypeAdapter


async def invoke_commit(function, args, kwargs):
    bound = inspect.signature(function).bind_partial(*args, **kwargs)
    values = dict(bound.arguments)
    hints = get_type_hints(function)
    slots, field_errors = {}, {}
    for key, value in list(values.items()):
        if key == 'tool_context' or value is None:
            continue
        annotation = hints.get(key, str)
        branches = get_args(annotation)
        list_type = next((item for item in (annotation, *branches) if get_origin(item) is list), None)
        if list_type is None:
            try:
                TypeAdapter(annotation).validate_python(value, strict=True)
            except ValueError as exc:
                field_errors[key] = {'status': 'rejected', 'reason': str(exc)}
                values.pop(key)
            continue
        if not isinstance(value, list):
            field_errors[key] = {'status': 'rejected', 'reason': 'expected_array'}
            values.pop(key)
            continue
        item_type = get_args(list_type)[0]
        valid = []
        mapping: list[dict[str, object] | None] = []
        for index, entry in enumerate(value):
            try:
                if index >= 20:
                    raise ValueError('batch_limit_20')
                TypeAdapter(item_type).validate_python(entry, strict=True)
                if isinstance(entry, str) and not entry:
                    raise ValueError('empty_record_id')
            except ValueError as exc:
                mapping.append({'index': index, 'status': 'rejected', 'reason': str(exc)})
            else:
                mapping.append(None)
                valid.append(entry)
        values[key], slots[key] = valid, mapping
    result = await function(**values)
    if not isinstance(result, dict):
        return result
    for key, mapping in slots.items():
        response = result.get(key, [])
        source = response.get('results', []) if isinstance(response, dict) else response
        source = source if isinstance(source, list) else []
        iterator = iter(source)
        failure = (response if isinstance(response, dict) and response.get('status') in
                   ('failed', 'rejected', 'unavailable') else {'status': 'unknown'})
        restored = [slot if slot is not None else {**next(iterator, failure), 'index': index}
                    for index, slot in enumerate(mapping)]
        result[key] = {**response, 'results': restored} if isinstance(response, dict) else restored
    def has_failure(value):
        if isinstance(value, dict):
            return value.get('status') in ('failed', 'rejected', 'unavailable', 'ambiguous') or any(
                has_failure(item) for item in value.values()
            )
        return isinstance(value, list) and any(has_failure(item) for item in value)
    if field_errors or has_failure(result):
        result['status'] = 'partial'
        result['invalid_fields'] = field_errors
    return result
