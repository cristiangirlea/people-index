#!/usr/bin/env bash
# Tests scripts/check-s3.sh against stub docker and cargo commands on PATH: it passes only when
# every selected case ran and passed, and fails on a skipped case, no case, a failing cargo, a
# failing s3-init (without running cargo) and a teardown that does not remove the project; it
# removes its project every time, two invocations use different projects (S3_TEST_PROJECT names
# it), and the store and the tests get the same port (S3_TEST_PORT), credentials and bucket
# whatever S3_PORT and S3_ACCESS_KEY_ID were before.
set -euo pipefail
src="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
check="$src/scripts/check-s3.sh"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
fail() { echo "test-check-s3: $*" >&2; exit 1; }

# docker: logs each call to FAKE_CALLS; `compose ... run` records the values compose would
# interpolate in FAKE_DOCKER_ENV and exits FAKE_INIT_RC, `compose ... down` exits FAKE_DOWN_RC,
# `compose ... logs` 0. cargo: logs each call, records the test variables in FAKE_CARGO_ENV,
# prints FAKE_CARGO_OUT, exits FAKE_CARGO_RC.
mkdir "$tmp/bin"
cat >"$tmp/bin/docker" <<'EOF'
#!/usr/bin/env bash
echo "docker $*" >>"$FAKE_CALLS"
case " $* " in
  *" run --rm s3-init "*)
    echo "$S3_PORT $S3_ACCESS_KEY_ID $S3_SECRET_ACCESS_KEY" >"$FAKE_DOCKER_ENV"
    exit "$FAKE_INIT_RC" ;;
  *" down "*) exit "$FAKE_DOWN_RC" ;;
  *" logs "*) exit 0 ;;
esac
echo "stub docker: unexpected call: $*" >&2
exit 3
EOF
cat >"$tmp/bin/cargo" <<'EOF'
#!/usr/bin/env bash
echo "cargo $*" >>"$FAKE_CALLS"
echo "$PI_TEST_S3_ENDPOINT $PI_TEST_S3_BUCKET $AWS_ACCESS_KEY_ID $AWS_SECRET_ACCESS_KEY" >"$FAKE_CARGO_ENV"
printf '%s\n' "$FAKE_CARGO_OUT"
exit "$FAKE_CARGO_RC"
EOF
chmod +x "$tmp/bin/docker" "$tmp/bin/cargo"
export PATH="$tmp/bin:$PATH" FAKE_CALLS="$tmp/calls" FAKE_DOCKER_ENV="$tmp/docker.env" \
  FAKE_CARGO_ENV="$tmp/cargo.env"
unset S3_TEST_PROJECT S3_TEST_PORT
# What a shell or deploy/compose/.env may hold; the check must not use it.
export S3_PORT=1 S3_ACCESS_KEY_ID=elsewhere S3_SECRET_ACCESS_KEY=elsewhere-secret

ok_out='test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 9 filtered out'
good() { export FAKE_INIT_RC=0 FAKE_DOWN_RC=0 FAKE_CARGO_RC=0 FAKE_CARGO_OUT="$ok_out"; }

# expect <exit code> <description>: the check's exit code must match, and it must have tried to
# remove its project.
expect() {
  local want="$1" what="$2" got=0
  : >"$FAKE_CALLS"
  rm -f "$FAKE_DOCKER_ENV" "$FAKE_CARGO_ENV"
  bash "$check" >"$tmp/out" 2>&1 || got=$?
  [[ "$got" == "$want" ]] || fail "$what: exit $got, expected $want: $(cat "$tmp/out")"
  grep -q ' down --volumes ' "$FAKE_CALLS" || fail "$what: the project was not removed"
}
cargo_runs() { grep -c '^cargo test --locked ' "$FAKE_CALLS" || true; }

good
expect 0 "every case passed"
[[ "$(cargo_runs)" == 2 ]] || fail "every case passed: cargo ran $(cargo_runs) times, expected 2"
grep -q -- '-p pi-store --test conformance -- s3:: --nocapture --color never' "$FAKE_CALLS" ||
  fail "the conformance cases are not selected: $(cat "$FAKE_CALLS")"
grep -q -- '-p pi-coord --test leases -- leases_hold_on_a_real_s3_endpoint --exact --nocapture' \
  "$FAKE_CALLS" || fail "the lease case is not selected: $(cat "$FAKE_CALLS")"
grep -q '^check-s3: ok$' "$tmp/out" || fail "every case passed: no ok line"
first="$(sed -nE 's/.* -p ([^ ]+) -f .* down .*/\1/p' "$FAKE_CALLS")"
# same_store <port>: the store and the tests agree on port, credentials and bucket.
same_store() {
  local port key secret endpoint bucket akey asecret
  read -r port key secret <"$FAKE_DOCKER_ENV"
  read -r endpoint bucket akey asecret <"$FAKE_CARGO_ENV"
  [[ "$port" == "$1" && "$endpoint" == "http://127.0.0.1:$1" ]] ||
    fail "store on port '$port', tests on '$endpoint', expected $1"
  [[ "$key" != elsewhere && "$akey" == "$key" && "$asecret" == "$secret" ]] ||
    fail "store credentials '$key', tests '$akey'"
  [[ "$bucket" == people-index ]] || fail "tests use bucket '$bucket', s3-init makes people-index"
}
same_store 18333

good
expect 0 "a second invocation"
second="$(sed -nE 's/.* -p ([^ ]+) -f .* down .*/\1/p' "$FAKE_CALLS")"
[[ -n "$first" && "$first" != "$second" ]] ||
  fail "two invocations share compose project '$first'"

good
S3_TEST_PORT=24000 S3_TEST_PROJECT=pi-s3-mine expect 0 "S3_TEST_PORT and S3_TEST_PROJECT"
same_store 24000
[[ "$(grep -c '^docker compose -p pi-s3-mine -f ' "$FAKE_CALLS")" == "$(grep -c '^docker ' "$FAKE_CALLS")" ]] ||
  fail "S3_TEST_PROJECT is not used for every compose call: $(cat "$FAKE_CALLS")"

good; FAKE_CARGO_OUT="PI_TEST_S3_ENDPOINT is not set: S3 conformance case skipped"$'\n'"$ok_out"
expect 1 "a skipped case"

good; FAKE_CARGO_OUT='test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 31 filtered out'
expect 1 "no case ran"

good; FAKE_CARGO_RC=101
expect 1 "cargo test failed after an ok-looking summary"

good; FAKE_INIT_RC=1
expect 1 "s3-init failed"
[[ "$(cargo_runs)" == 0 ]] || fail "s3-init failed: cargo still ran"

good; FAKE_DOWN_RC=1
expect 1 "the project could not be removed"
grep -q 'could not remove compose project' "$tmp/out" || fail "teardown failure not reported"
! grep -q 'check-s3: ok' "$tmp/out" || fail "ok printed although the teardown failed"

good; FAKE_DOWN_RC=1; FAKE_CARGO_OUT='test result: ok. 0 passed; 0 failed'
expect 1 "a failure and a failed teardown"

echo "test-check-s3: ok"
