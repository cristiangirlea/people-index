#!/usr/bin/env bash
# Tests scripts/scrub-check.sh and the hooks in a throwaway repository with its own word list:
# the list beside the main checkout is found from the checkout and from a worktree that lives
# somewhere else, a commit carrying a listed word is refused in both, and a missing list skips.
set -euo pipefail
src="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
fail() { echo "test-scrub-check: $*" >&2; exit 1; }
# Sealed from the caller: no inherited repository, list override or user/system git config
# (a global commit.gpgsign or hooksPath must not change the outcome).
unset SCRUB_WORDS_FILE GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_COMMON_DIR GIT_OBJECT_DIRECTORY
export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1

git init -q -b main "$tmp/repo"
cd "$tmp/repo"
git config user.name test
git config user.email test@example.invalid
mkdir -p scripts .githooks
cp "$src/scripts/scrub-check.sh" scripts/
cp "$src/.githooks/pre-commit" "$src/.githooks/commit-msg" .githooks/
chmod +x scripts/scrub-check.sh .githooks/*
git config core.hooksPath .githooks
git add -A && git commit -q -m "hooks"

# No list yet: the check skips and says where it looked, for the tree and for a message.
# The folder holding the repository, spelled the way git spells paths on this platform.
parent="$(dirname "$(git rev-parse --path-format=absolute --show-toplevel)")"
out="$(bash scripts/scrub-check.sh 2>&1)" || fail "a missing list must skip, got: $out"
[[ "$out" == *"no word list at $parent/people-index.scrub-words"* ]] || fail "unexpected skip message: $out"
echo "zebraword" >"$tmp/msg"
bash scripts/scrub-check.sh --message "$tmp/msg" 2>/dev/null || fail "--message must skip without a list"

# SCRUB_WORDS_FILE overrides the default: a listed word is refused, a missing file skips by name.
printf 'zebraword
' >"$tmp/other-list"
echo "a zebraword here" >leak.txt
SCRUB_WORDS_FILE="$tmp/other-list" bash scripts/scrub-check.sh >/dev/null 2>&1 && fail "the override list was not used"
out="$(SCRUB_WORDS_FILE="$tmp/missing-list" bash scripts/scrub-check.sh 2>&1)" || fail "a missing override must skip"
[[ "$out" == *"missing-list"* ]] || fail "the skip did not name the override: $out"
rm leak.txt

printf 'zebraword\n' >"$tmp/people-index.scrub-words"
bash scripts/scrub-check.sh >/dev/null || fail "a clean tree must pass"

# From the checkout: a listed word in a file is refused at commit time.
echo "a zebraword here" >leak.txt
git add leak.txt
if git commit -q -m "leak" 2>/dev/null; then fail "the checkout accepted a listed word"; fi
git reset -q --hard

# From a worktree outside the checkout's folder: same list, same refusal, file and message.
git worktree add -q -b wt "$tmp/elsewhere/wt" main
cd "$tmp/elsewhere/wt"
echo "a zebraword here" >leak.txt
out="$(bash scripts/scrub-check.sh 2>&1)" && fail "the worktree check passed a listed word"
[[ "$out" == *"leak.txt"* ]] || fail "the worktree check did not name the file: $out"
git add leak.txt
if git commit -q -m "leak" 2>/dev/null; then fail "the worktree accepted a listed word in a file"; fi
git reset -q --hard
echo "clean" >ok.txt
git add ok.txt
if git commit -q -m "mentions zebraword" 2>/dev/null; then fail "the worktree accepted a listed word in a message"; fi
git commit -q -m "clean message" || fail "a clean commit was refused in the worktree"

# From a subdirectory of the outside worktree the same list is found.
mkdir -p sub && echo "a zebraword here" >sub/leak.txt
(cd sub && bash ../scripts/scrub-check.sh >/dev/null 2>&1) && fail "a subdirectory run missed the list"
rm -r sub

# A message check that cannot run fails closed: an invalid pattern in the list, or no file.
printf '(
' >"$tmp/people-index.scrub-words"
echo "fine" >"$tmp/msg"
rc=0; bash scripts/scrub-check.sh --message "$tmp/msg" 2>/dev/null || rc=$?
[[ "$rc" -eq 2 ]] || fail "an invalid pattern must fail the message check with 2, got $rc"
printf 'zebraword
' >"$tmp/people-index.scrub-words"
rc=0; bash scripts/scrub-check.sh --message "$tmp/no-such-file" 2>/dev/null || rc=$?
[[ "$rc" -eq 2 ]] || fail "an unreadable message must fail with 2, got $rc"

echo "test-scrub-check: ok"
