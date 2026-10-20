#!/usr/bin/env bash
# Fails when a hook under .githooks/ or a *.sh anywhere in the repository is not stored as
# 100755. Git skips a hook that is not executable (it only prints a hint), and scripts called
# directly fail with "Permission denied" on Linux and macOS. Git for Windows ignores file modes,
# so this reads the modes git stores in the index, not the files on disk.
# Fix one with: git update-index --chmod=+x <path>
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
# quotePath pinned: a raw non-UTF-8 name would make grep treat the listing as binary.
listed="$(git -c core.quotePath=true ls-files -s -- .githooks '*.sh')"
# An empty listing means the pathspecs matched nothing, not that every mode is right.
[[ -n "$listed" ]] || { echo "check-modes: no hooks or scripts found" >&2; exit 1; }
bad="$(grep -a -v '^100755 ' <<<"$listed" || true)"
if [[ -n "$bad" ]]; then
  echo "check-modes: not stored as 100755 (git update-index --chmod=+x <path>):" >&2
  cut -f2- <<<"$bad" >&2
  exit 1
fi
echo "check-modes: ok"
