"""Bounded, offline text/Office file reader shared by MediaToolset and WorkspaceToolset."""

from __future__ import annotations

import hashlib
import zipfile
from pathlib import Path
from xml.etree import ElementTree as ET

MAX_TEXT_READ_BYTES = 2_000_000
MAX_OFFICE_ZIP_BYTES = 10_000_000
MAX_OFFICE_XML_BYTES = 2_000_000
MAX_PDF_EXTRACT_BYTES = 14_000_000
MAX_PDF_EXTRACT_PAGES = 80
MAX_RESPONSE_UTF8_BYTES = 24_000

_DOCX_NS = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}


def _safe_xml_root(raw_xml: bytes, *, max_bytes: int = MAX_OFFICE_XML_BYTES) -> ET.Element:
    if len(raw_xml) > max_bytes:
        raise ValueError("office_xml_entry_too_large")
    head = raw_xml[:4096].upper()
    if b"<!DOCTYPE" in head or b"<!ENTITY" in head:
        raise ValueError("unsafe_xml_entity_declaration")
    return ET.fromstring(raw_xml)


def _read_zip_member(zf: zipfile.ZipFile, name: str, *, max_bytes: int = MAX_OFFICE_XML_BYTES) -> bytes:
    info = zf.getinfo(name)
    if info.file_size > max_bytes:
        raise ValueError("office_xml_entry_too_large")
    with zf.open(info, "r") as handle:
        data = handle.read(max_bytes + 1)
    if len(data) > max_bytes:
        raise ValueError("office_xml_entry_too_large")
    return data


def extract_docx_lines(path: Path) -> list[str]:
    """Extract paragraph lines from a `.docx` container without external dependencies."""
    if path.stat().st_size > MAX_OFFICE_ZIP_BYTES:
        raise ValueError("office_file_too_large")
    lines: list[str] = []
    with zipfile.ZipFile(path, "r") as zf:
        if "word/document.xml" not in zf.namelist():
            raise ValueError("invalid_docx_missing_document_xml")
        total_uncompressed = 0
        for member in ("word/document.xml", "word/header1.xml", "word/footer1.xml"):
            if member not in zf.namelist():
                continue
            raw = _read_zip_member(zf, member)
            total_uncompressed += len(raw)
            if total_uncompressed > MAX_OFFICE_XML_BYTES * 3:
                raise ValueError("office_zip_uncompressed_limit")
            root = _safe_xml_root(raw)
            for para in root.findall(".//w:p", _DOCX_NS):
                text = "".join(node.text or "" for node in para.findall(".//w:t", _DOCX_NS)).strip()
                if text:
                    lines.append(text)
    return lines or ["(文档正文为空)"]


def _xlsx_shared_strings(zf: zipfile.ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in zf.namelist():
        return []
    root = _safe_xml_root(_read_zip_member(zf, "xl/sharedStrings.xml"))
    strings: list[str] = []
    for item in root:
        if item.tag.endswith("}si") or item.tag == "si":
            strings.append(
                "".join(node.text or "" for node in item.iter() if (node.tag.endswith("}t") or node.tag == "t"))
            )
    return strings


def extract_xlsx_lines(path: Path, *, max_sheets: int = 12, max_rows_per_sheet: int = 500) -> list[str]:
    """Extract readable worksheet tables from an `.xlsx` workbook safely."""
    if path.stat().st_size > MAX_OFFICE_ZIP_BYTES:
        raise ValueError("office_file_too_large")
    lines: list[str] = []
    with zipfile.ZipFile(path, "r") as zf:
        sheets = sorted(
            name for name in zf.namelist()
            if name.startswith("xl/worksheets/sheet") and name.endswith(".xml")
        )[:max_sheets]
        if not sheets:
            raise ValueError("invalid_xlsx_missing_worksheets")
        shared = _xlsx_shared_strings(zf)
        total_uncompressed = 0
        for idx, sheet_name in enumerate(sheets, start=1):
            raw = _read_zip_member(zf, sheet_name)
            total_uncompressed += len(raw)
            if total_uncompressed > MAX_OFFICE_XML_BYTES * 4:
                raise ValueError("office_zip_uncompressed_limit")
            root = _safe_xml_root(raw)
            lines.append(f"=== Sheet {idx} ({sheet_name.rsplit('/', 1)[-1]}) ===")
            rows_emitted = 0
            for elem in root.iter():
                if not (elem.tag.endswith("}row") or elem.tag == "row"):
                    continue
                if rows_emitted >= max_rows_per_sheet:
                    lines.append("... (sheet row cap reached)")
                    break
                cells: list[str] = []
                for cell in elem:
                    if not (cell.tag.endswith("}c") or cell.tag == "c"):
                        continue
                    cell_type = cell.attrib.get("t", "")
                    val_text = ""
                    for sub in cell:
                        tag = sub.tag.rsplit("}", 1)[-1]
                        if cell_type == "inlineStr" and tag == "is":
                            val_text = "".join(n.text or "" for n in sub.iter() if n.tag.rsplit("}", 1)[-1] == "t")
                            break
                        if tag == "v" and sub.text is not None:
                            val_text = sub.text
                            if cell_type == "s" and val_text.isdigit():
                                s_idx = int(val_text)
                                val_text = shared[s_idx] if 0 <= s_idx < len(shared) else val_text
                            break
                    cells.append(val_text.replace("\t", " ").replace("\r", " ").replace("\n", " ").strip())
                if any(cells):
                    lines.append("\t".join(cells))
                    rows_emitted += 1
    return lines or ["(工作簿为空)"]


def _text_quality_metrics(text: str) -> tuple[int, int]:
    """Count non-whitespace characters and suspicious mojibake/control/PUA characters."""
    non_ws = 0
    bad = 0
    for ch in text:
        if ch.isspace():
            continue
        non_ws += 1
        code = ord(ch)
        if ch == "\ufffd" or code < 32 or 0xE000 <= code <= 0xF8FF:
            bad += 1
    return non_ws, bad


def extract_pdf_lines(
    path: Path,
    *,
    max_pages: int = MAX_PDF_EXTRACT_PAGES,
    max_bytes: int = MAX_PDF_EXTRACT_BYTES,
) -> list[str] | None:
    """Extract page-delimited text lines from a text-based PDF, or None if multimodal is needed."""
    try:
        from pypdf import PdfReader
    except ImportError:
        return None
    if path.stat().st_size > max_bytes:
        return None
    try:
        reader = PdfReader(str(path))
        if reader.is_encrypted and reader.decrypt("") == 0:
            return None
        total_pages = len(reader.pages)
        if total_pages == 0 or total_pages > max_pages:
            return None
        page_texts = [page.extract_text() or "" for page in reader.pages]
    except Exception:  # noqa: BLE001
        return None

    total_non_ws = 0
    total_bad = 0
    for page_text in page_texts:
        non_ws, bad = _text_quality_metrics(page_text)
        total_non_ws += non_ws
        total_bad += bad
    if total_non_ws < max(20, total_pages * 15) or (total_bad / total_non_ws) > 0.15:
        return None

    lines: list[str] = []
    for idx, page_text in enumerate(page_texts, start=1):
        lines.append(f"=== Page {idx}/{total_pages} ===")
        extracted_lines = [line.strip() for line in page_text.splitlines() if line.strip()]
        if extracted_lines:
            lines.extend(extracted_lines)
        else:
            lines.append("(本页未提取到可复制文本，可能为图片/扫描页)")
    return lines


def decode_text_bytes(raw: bytes) -> tuple[str, str]:
    """Decode text/CSV/code bytes while rejecting binary payloads."""
    if b"\x00" in raw[:8192]:
        raise ValueError("binary_file_not_readable_as_text")
    for encoding in ("utf-8-sig", "utf-8", "gb18030"):
        try:
            return raw.decode(encoding), encoding
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace"), "utf-8-replace"


def read_paginated_file_lines(
    path: Path,
    *,
    start_line: int = 1,
    line_count: int = 150,
    max_lines_per_page: int = 300,
    max_utf8_bytes: int = MAX_RESPONSE_UTF8_BYTES,
) -> dict[str, object]:
    """Read a text, `.docx`, `.xlsx`, or text-based `.pdf` with stable line pagination and SHA-256."""
    if start_line < 1 or not 1 <= line_count <= max_lines_per_page:
        raise ValueError("invalid_line_range")
    stat = path.stat()
    raw = path.read_bytes()
    sha256 = hashlib.sha256(raw).hexdigest()
    suffix = path.suffix.lower()

    if suffix == ".docx":
        lines = extract_docx_lines(path)
        kind = "docx_extracted_text"
        encoding = "docx-xml"
    elif suffix == ".xlsx":
        lines = extract_xlsx_lines(path)
        kind = "xlsx_extracted_table"
        encoding = "xlsx-xml"
    elif suffix == ".pdf":
        pdf_lines = extract_pdf_lines(path)
        if pdf_lines is None:
            raise ValueError("pdf_requires_multimodal_read")
        lines = pdf_lines
        kind = "pdf_extracted_text"
        encoding = "pdf-text"
    else:
        if stat.st_size > MAX_TEXT_READ_BYTES:
            raise ValueError("text_file_too_large_for_direct_read")
        text, encoding = decode_text_bytes(raw)
        lines = text.splitlines()
        kind = "text"

    chosen = lines[start_line - 1 : start_line - 1 + line_count]
    joined = "\n".join(chosen)
    truncated = len(joined.encode("utf-8")) > max_utf8_bytes
    if truncated:
        joined = joined.encode("utf-8")[:max_utf8_bytes].decode("utf-8", "ignore")
    more_lines = (start_line - 1 + len(chosen)) < len(lines) or truncated
    return {
        "kind": kind,
        "encoding": encoding,
        "byte_count": stat.st_size,
        "sha256": sha256,
        "start_line": start_line,
        "total_lines": len(lines),
        "content": joined,
        "truncated": truncated,
        "more_lines": more_lines,
    }
