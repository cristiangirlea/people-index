#!/usr/bin/env bash
set -euo pipefail
[[ $# == 1 ]] || { echo "usage: $0 vX.Y.Z" >&2; exit 1; }
python=python3
if ! command -v "$python" >/dev/null 2>&1; then python=python; fi
"$python" - "$1" <<'PY'
import pathlib, re, sys, tomllib

sys.stdout.reconfigure(newline='\n')
tag = sys.argv[1]
version = tomllib.loads(pathlib.Path('Cargo.toml').read_text(encoding='utf-8'))['workspace']['package']['version']
if not re.fullmatch(r'v[0-9]+\.[0-9]+\.[0-9]+', tag) or tag != 'v' + version:
    sys.exit(f'Tag {tag} must be v followed by the Cargo.toml version ({version})')
text = pathlib.Path('CHANGELOG.md').read_text(encoding='utf-8')
match = re.search(r'^## \[' + re.escape(version) + r'\](?:[ \t]+-[ \t]+[^\n]+)?[ \t]*\n(.*?)(?=^## |\Z)', text, re.M | re.S)
if not match or not match.group(1).strip():
    sys.exit(f'CHANGELOG.md has no non-empty section for [{version}]')
print(match.group(1).strip())
PY
