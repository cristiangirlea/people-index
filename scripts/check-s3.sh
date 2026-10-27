#!/usr/bin/env bash
# The store conformance and lease tests against a real S3 endpoint: the SeaweedFS service of
# deploy/compose (the image the deployment pins), its bucket made by s3-init, which also probes
# the conditional writes. Each invocation is a compose project of its own (S3_TEST_PROJECT,
# default people-index-s3-test-<pid>), so it never tears down a running stack or another
# invocation; the store publishes S3_TEST_PORT (default 18333), so two at once need different
# ports (the second fails to bind otherwise). The project and its volume are removed at the
# end, and a failure to remove them fails the check. Fails when a case fails or skips,
# or none runs. CI runs it on every pull request; locally `make test-s3` (needs Docker and
# cargo). scripts/test-check-s3.sh tests it against stub docker and cargo.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

# Exported values win over deploy/compose/.env, so a local .env cannot split the credentials
# the store starts with from the ones the tests use.
export S3_PORT="${S3_TEST_PORT:-18333}"
export S3_ACCESS_KEY_ID=people-index-test
export S3_SECRET_ACCESS_KEY=people-index-test-secret
project="${S3_TEST_PROJECT:-people-index-s3-test-$$}"
compose=(docker compose -p "$project" -f deploy/compose/compose.yaml)

work="$(mktemp -d)"
cleanup() {
  local rc=$?
  if ! "${compose[@]}" down --volumes --remove-orphans >"$work/down.log" 2>&1; then
    cat "$work/down.log" >&2
    echo "check-s3: could not remove compose project $project" >&2
    ((rc != 0)) || rc=1
  fi
  rm -rf "$work"
  ((rc != 0)) || echo "check-s3: ok"
  exit "$rc"
}
trap cleanup EXIT

fail() {
  echo "check-s3: $*" >&2
  exit 1
}

# `run` starts s3 first and waits for its health check (s3-init depends on it).
"${compose[@]}" run --rm s3-init || {
  "${compose[@]}" logs --no-log-prefix s3 >&2 || true
  fail "s3-init failed"
}

export PI_TEST_S3_ENDPOINT="http://127.0.0.1:$S3_PORT"
export PI_TEST_S3_BUCKET=people-index
export AWS_ACCESS_KEY_ID="$S3_ACCESS_KEY_ID"
export AWS_SECRET_ACCESS_KEY="$S3_SECRET_ACCESS_KEY"
export AWS_REGION=us-east-1

# run_cases <name> <cargo test args...> -- <test binary args...>: the selected cases must all
# pass, at least one must run, and none may print its skip note.
run_cases() {
  local name="$1" log="$work/$1.log"
  shift
  cargo test --locked "$@" --nocapture --color never 2>&1 | tee "$log" || fail "$name: cargo test failed"
  ! grep -q 'PI_TEST_S3_ENDPOINT is not set' "$log" || fail "$name: a case skipped"
  grep -qE '^test result: ok\. [1-9][0-9]* passed' "$log" || fail "$name: no case ran"
}

run_cases conformance -p pi-store --test conformance -- s3::
run_cases leases -p pi-coord --test leases -- leases_hold_on_a_real_s3_endpoint --exact
