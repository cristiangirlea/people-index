#!/usr/bin/env bash
# Tests scripts/check-pins-resolve.sh with a stub docker on PATH and, for the default path, a
# stub check-deploy.sh beside a copy of it: every reference resolving passes; missing ones fail
# and are all named, wherever they sort; a reference is tried three times and passes when a try
# succeeds; duplicates are asked once; an empty list fails; the default path takes the
# references check-deploy.sh writes to PINNED_REFS_OUT and fails when check-deploy.sh fails.
set -euo pipefail
src="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
fail() { echo "test-check-pins-resolve: $*" >&2; exit 1; }

# docker buildx imagetools inspect <ref>: fails for refs listed in $FAKE_MISSING; a ref listed
# in $FAKE_FLAKY as "<ref> <n>" fails its first n tries. Every call is logged to $FAKE_LOG.
mkdir "$tmp/bin"
cat >"$tmp/bin/docker" <<'EOF'
#!/usr/bin/env bash
[[ "$1 $2 $3" == "buildx imagetools inspect" ]] || { echo "stub docker: unexpected: $*" >&2; exit 3; }
ref="$4"
echo "$ref" >>"$FAKE_LOG"
if grep -qxF -- "$ref" "$FAKE_MISSING"; then echo "ERROR: $ref: not found" >&2; exit 1; fi
fails="$(awk -v r="$ref" '$1 == r { print $2 }' "$FAKE_FLAKY")"
if [[ -n "$fails" ]] && (("$(grep -cxF -- "$ref" "$FAKE_LOG")" <= fails)); then
  echo "ERROR: 503" >&2
  exit 1
fi
EOF
# No real sleeping between tries.
printf '#!/bin/sh\nexit 0\n' >"$tmp/bin/sleep"
chmod +x "$tmp/bin/docker" "$tmp/bin/sleep"
export PATH="$tmp/bin:$PATH" FAKE_LOG="$tmp/log" FAKE_MISSING="$tmp/missing" FAKE_FLAKY="$tmp/flaky"

ref() { printf 'registry.example/%s:1@sha256:%s' "$1" "$(printf "$1%.0s" {1..64})"; }
a="$(ref a)"
b="$(ref b)"
c="$(ref c)"
printf '%s\n' "$b" "$c" "$a" "$b" >"$tmp/refs"

reset() { : >"$FAKE_LOG"; : >"$FAKE_MISSING"; : >"$FAKE_FLAKY"; }
tries() { grep -cxF -- "$1" "$FAKE_LOG" || true; }
check() { bash "$src/scripts/check-pins-resolve.sh" "$@" >"$tmp/out" 2>&1; }

reset
check "$tmp/refs" || fail "all resolving must pass: $(cat "$tmp/out")"
grep -q "3 pinned images resolve" "$tmp/out" || fail "unexpected output: $(cat "$tmp/out")"
[[ "$(tries "$b")" == 1 ]] || fail "a duplicate reference was asked $(tries "$b") times"

# The first reference in sort order missing: the rest are still asked, only it is named.
reset
echo "$a" >"$FAKE_MISSING"
check "$tmp/refs" && fail "a missing image must fail"
grep -qF "does not resolve: $a" "$tmp/out" || fail "the missing image was not named: $(cat "$tmp/out")"
grep -qF "does not resolve: $b" "$tmp/out" && fail "a resolving image was named"
[[ "$(tries "$c")" == 1 ]] || fail "the references after a missing one were not asked"
[[ "$(tries "$a")" == 3 ]] || fail "a missing image was tried $(tries "$a") times, not 3"

reset
printf '%s\n' "$a" "$c" >"$FAKE_MISSING"
check "$tmp/refs" && fail "two missing images must fail"
for missing in "$a" "$c"; do
  grep -qF "does not resolve: $missing" "$tmp/out" || fail "both missing images must be named: $(cat "$tmp/out")"
done

reset
echo "$a 2" >"$FAKE_FLAKY"
check "$tmp/refs" || fail "a reference that resolves on the third try must pass: $(cat "$tmp/out")"
[[ "$(tries "$a")" == 3 ]] || fail "the flaky reference was tried $(tries "$a") times, not 3"

: >"$tmp/empty"
check "$tmp/empty" && fail "an empty list must fail"

# The default path: a copy of the script beside a stub check-deploy.sh.
mkdir -p "$tmp/repo/scripts"
cp "$src/scripts/check-pins-resolve.sh" "$tmp/repo/scripts/"
cat >"$tmp/repo/scripts/check-deploy.sh" <<EOF
#!/usr/bin/env bash
[[ -n "\${PINNED_REFS_OUT:-}" ]] || { echo "stub check-deploy: PINNED_REFS_OUT not set" >&2; exit 4; }
[[ "\${FAKE_DEPLOY_FAIL:-0}" == 1 ]] && { echo "check-deploy: broken" >&2; exit 1; }
printf '%s\n' "$a" "$c" >>"\$PINNED_REFS_OUT"
EOF
reset
bash "$tmp/repo/scripts/check-pins-resolve.sh" >"$tmp/out" 2>&1 ||
  fail "the default path must pass: $(cat "$tmp/out")"
grep -q "2 pinned images resolve" "$tmp/out" || fail "the default path missed check-deploy's references"
[[ "$(tries "$a")" == 1 && "$(tries "$c")" == 1 ]] || fail "the default path did not ask each reference"
reset
FAKE_DEPLOY_FAIL=1 bash "$tmp/repo/scripts/check-pins-resolve.sh" >"$tmp/out" 2>&1 &&
  fail "a failing check-deploy.sh must fail the check"
[[ ! -s "$FAKE_LOG" ]] || fail "references were asked after check-deploy.sh failed"
echo "test-check-pins-resolve: ok"
