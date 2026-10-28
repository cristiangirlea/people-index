#!/usr/bin/env bash
set -euo pipefail
script="$(cd "$(dirname "$0")" && pwd)/release-notes.sh"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
cd "$tmp"
printf '[workspace.package]\nversion = "0.1.0"\n' > Cargo.toml
check() {
  local expected="$1"
  diff -u <(printf '%s\n' "$expected") <(bash "$script" v0.1.0)
}
printf '## [0.1.0]\n\n- first\n- second\n\n## [0.0.9]\n- old\n' > CHANGELOG.md
check $'- first\n- second'
printf '## [0.1.0] - 2026-10-28\n\n- dated\n' > CHANGELOG.md
check '- dated'
printf '## [0.1.0]\n\n### Added\n\n- feature\n' > CHANGELOG.md
check $'### Added\n\n- feature'
printf '## [0.0.9]\n- old\n\n## [0.1.0]\n- last' > CHANGELOG.md
check '- last'
printf '## [0.0.9]\n- old\n' > CHANGELOG.md
if bash "$script" v0.1.0 >/dev/null 2>&1; then echo 'missing section accepted' >&2; exit 1; fi
printf '## [0.1.0]\n\n## [0.0.9]\n- old\n' > CHANGELOG.md
if bash "$script" v0.1.0 >/dev/null 2>&1; then echo 'empty section accepted' >&2; exit 1; fi
printf '## [0.1.0]
- first
' > CHANGELOG.md
check '- first'
if bash "$script" v0.1.0-rc1 >/dev/null 2>&1; then echo 'prerelease tag accepted' >&2; exit 1; fi
if bash "$script" v0.2.0 >/dev/null 2>&1; then echo 'mismatched tag accepted' >&2; exit 1; fi
echo 'release notes tests passed'
