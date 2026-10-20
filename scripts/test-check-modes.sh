#!/usr/bin/env bash
# Tests scripts/check-modes.sh in a throwaway repository: a hook or script stored as 100644
# fails and is named, 100755 passes, and a repository with neither fails instead of passing.
set -euo pipefail
src="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
fail() { echo "test-check-modes: $*" >&2; exit 1; }
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_COMMON_DIR GIT_OBJECT_DIRECTORY
export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1

git init -q -b main "$tmp/repo"
cd "$tmp/repo"
check() { bash "$src/scripts/check-modes.sh" 2>&1; }

out="$(check)" && fail "an empty repository must fail, got: $out"
[[ "$out" == *"no hooks or scripts found"* ]] || fail "unexpected empty-repository message: $out"

mkdir -p .githooks deploy/sub
printf '#!/usr/bin/env bash\n' >.githooks/pre-commit
printf '#!/usr/bin/env bash\n' >"deploy/sub/a b.sh"
printf 'not a script\n' >notes.txt
git add -A
git update-index --chmod=-x .githooks/pre-commit "deploy/sub/a b.sh"
out="$(check)" && fail "100644 files must fail, got: $out"
[[ "$out" == *".githooks/pre-commit"* ]] || fail "the hook was not named: $out"
[[ "$out" == *"deploy/sub/a b.sh"* ]] || fail "the nested script was not named: $out"
[[ "$out" != *"notes.txt"* ]] || fail "a file that is neither hook nor script was named: $out"

git update-index --chmod=+x .githooks/pre-commit
out="$(check)" && fail "one 100644 script must still fail, got: $out"
[[ "$out" != *".githooks/pre-commit"* ]] || fail "an executable hook was named: $out"

git update-index --chmod=+x "deploy/sub/a b.sh"
out="$(check)" || fail "all 100755 must pass, got: $out"

# From a subdirectory the whole repository is checked.
git update-index --chmod=-x .githooks/pre-commit
out="$(cd deploy/sub && check)" && fail "a subdirectory run missed a hook, got: $out"
[[ "$out" == *".githooks/pre-commit"* ]] || fail "the subdirectory run did not name the hook: $out"

echo "test-check-modes: ok"
