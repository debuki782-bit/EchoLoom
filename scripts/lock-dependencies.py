"""Lock the installed default + dev dependency closure for EchoLoom.

Run after installing this project with [dev] in its isolated environment.
This checks every installed version against its declared requirements.
"""
from importlib.metadata import distribution
from pathlib import Path

from packaging.markers import default_environment
from packaging.requirements import Requirement
from packaging.utils import canonicalize_name


def main():
    pending = [Requirement('echoloom[dev]')]
    visited, versions = set(), {}
    while pending:
        requirement = pending.pop()
        name = canonicalize_name(requirement.name)
        installed = distribution(name)
        if not requirement.specifier.contains(installed.version, prereleases=True):
            raise ValueError(f'{name} {installed.version} does not satisfy {requirement.specifier}')
        extras = requirement.extras or {''}
        for extra in extras:
            if (name, extra) in visited:
                continue
            visited.add((name, extra))
            for text in installed.requires or []:
                dependency = Requirement(text)
                if not dependency.marker or dependency.marker.evaluate({**default_environment(), 'extra': extra}):
                    pending.append(dependency)
        if name != 'echoloom':
            versions[name] = installed.version
    path = Path(__file__).resolve().parents[1]/'requirements.lock'
    path.write_text(''.join(f'{name}=={version}\n' for name, version in sorted(versions.items())), 'utf-8')
    print(f'Locked {len(versions)} default/dev dependencies.')


if __name__ == '__main__':
    main()
