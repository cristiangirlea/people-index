#!/usr/bin/env bash
# Tests scripts/check-pins-resolve.sh with a stub docker on PATH and, for the default path, a
# stub check-deploy.sh beside a copy of it: every reference resolving passes; missing ones fail
# and are all named, wherever they sort; a reference is tried three times and passes when a try
# succeeds; duplicates are asked once; an empty list fails; the default path takes the
# references check-deploy.sh writes to PINNED_REFS_OUT and fails when check-deploy.sh fails.
# Also tests the shared rendered Job and schedule image checks on copied deployment files.
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
# Exercise the same script check-deploy calls, against copied deployment files.
cp -R "$src/deploy/k8s" "$tmp/k8s"
export PINNED_REFS_OUT="$tmp/collected"
: >"$PINNED_REFS_OUT"
job_check() { bash "$src/scripts/check-job-images.sh" "$tmp/k8s" "$1" "$2" >"$tmp/out" 2>&1; }
set_image() { sed -i "s|^IMAGE=.*|IMAGE=$2|" "$tmp/k8s/overlays/$1/jobs.vars"; }
expected_image="$(sed -n 's/^IMAGE=//p' "$tmp/k8s/overlays/production-example/jobs.vars")"
for kind in run datagen verify schedule; do
  job_check production-example "$kind" || fail "pinned $kind rejected: $(cat "$tmp/out")"
done
grep -qxF "$expected_image" "$PINNED_REFS_OUT" || fail "release image was not collected"
set_image production-example "${expected_image%@*}"
for kind in run datagen verify schedule; do
  job_check production-example "$kind" && fail "bare tag accepted for $kind"
  grep -q 'not pinned' "$tmp/out" || fail "missing pin failure for $kind: $(cat "$tmp/out")"
done
for kind in run datagen verify schedule; do
  set_image production-example people-index:local
  job_check production-example "$kind" && fail "local image accepted in production $kind"
  set_image local people-index:local
  job_check local "$kind" || fail "local image rejected in $kind: $(cat "$tmp/out")"
done
set_image production-example 'invalid image'
job_check production-example schedule && fail "invalid image accepted"
grep -q 'jobs/run does not render for production-example' "$tmp/out" || fail "render failure not named"
grep -q 'IMAGE' "$tmp/out" || fail "render error not printed"
# Check the labels used by the overlay, Job and schedule callers.
# shellcheck source=/dev/null
source "$src/scripts/check-job-images.sh"
for label in deploy/k8s/overlays/local 'jobs/run overlays/local' 'schedule overlays/local'; do
  # shellcheck disable=SC2034 # check_ref and report_unpinned read the array.
  (unpinned=(); check_ref "$label" people-index:local; report_unpinned) || fail "local label rejected: $label"
done
echo "test-check-pins-resolve: ok"
