#!/usr/bin/env bash
# Creates the pipeline and snapshot buckets, probes their isolation, then proves the store
# supports the conditional writes the commit and lease
# protocol depends on (docs/ARCHITECTURE.md, Commits and Leases):
#   - PUT If-None-Match: * on an existing key  -> 412 (create-if-absent: commits, lease claims)
#   - PUT If-Match: <stale etag>               -> 412 (update-if-match: lease renewals, takeovers)
# A store that silently overwrites would let two workers both "win" a task, so the stack refuses
# to start rather than run on one.
set -euo pipefail

: "${S3_ENDPOINT:?}" "${S3_BUCKET:?}"
deadline=$((SECONDS + ${S3_WAIT_SECONDS:-120}))
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

PIPELINE_KEY="$AWS_ACCESS_KEY_ID" PIPELINE_SECRET="$AWS_SECRET_ACCESS_KEY"
: "${SNAPSHOT_BUCKET:?}" "${SNAPSHOT_KEY:?}" "${SNAPSHOT_SECRET:?}"
s3api() { aws --endpoint-url "$S3_ENDPOINT" s3api "$@"; }

echo "s3-init: waiting for ${S3_ENDPOINT}"
until s3api list-buckets >/dev/null 2>&1; do
  if ((SECONDS >= deadline)); then
    echo "s3-init: the store did not answer in time" >&2
    s3api list-buckets >&2 || true
    exit 1
  fi
  sleep 2
done

if s3api head-bucket --bucket "$S3_BUCKET" >/dev/null 2>&1; then
  echo "s3-init: bucket ${S3_BUCKET} exists"
else
  echo "s3-init: creating bucket ${S3_BUCKET}"
  s3api create-bucket --bucket "$S3_BUCKET" >/dev/null
fi

# Create the snapshot bucket with its own scoped identity.
export AWS_ACCESS_KEY_ID="$SNAPSHOT_KEY" AWS_SECRET_ACCESS_KEY="$SNAPSHOT_SECRET"
if ! s3api head-bucket --bucket "$SNAPSHOT_BUCKET" >/dev/null 2>&1; then
  s3api create-bucket --bucket "$SNAPSHOT_BUCKET" >/dev/null
fi
s3api head-bucket --bucket "$SNAPSHOT_BUCKET" >/dev/null
export AWS_ACCESS_KEY_ID="$PIPELINE_KEY" AWS_SECRET_ACCESS_KEY="$PIPELINE_SECRET"

# Each identity must be denied list, write and delete on the other bucket.
expect_403() {
  local what="$1"
  shift
  if "$@" >"$work/out" 2>"$work/err"; then
    echo "s3-init: $what unexpectedly succeeded" >&2
    exit 1
  fi
  if ! grep -Eq '403|AccessDenied' "$work/err"; then
    echo "s3-init: $what failed without access denied:" >&2
    cat "$work/err" >&2
    exit 1
  fi
}
printf 'probe\n' >"$work/isolation"
for bucket in "$SNAPSHOT_BUCKET" "$S3_BUCKET"; do
  if [[ "$bucket" == "$S3_BUCKET" ]]; then
    export AWS_ACCESS_KEY_ID="$SNAPSHOT_KEY" AWS_SECRET_ACCESS_KEY="$SNAPSHOT_SECRET"
  fi
  expect_403 "list $bucket" s3api list-objects-v2 --bucket "$bucket"
  expect_403 "put $bucket" s3api put-object --bucket "$bucket" --key _isolation-probe --body "$work/isolation"
  expect_403 "delete $bucket" s3api delete-object --bucket "$bucket" --key _isolation-probe
done
export AWS_ACCESS_KEY_ID="$PIPELINE_KEY" AWS_SECRET_ACCESS_KEY="$PIPELINE_SECRET"
echo "s3-init: bucket isolation verified"

# expect_412 <description> <command...>: the command must fail with PreconditionFailed.
expect_412() {
  local what="$1"
  shift
  if "$@" >"$work/out" 2>"$work/err"; then
    echo "s3-init: ${what} succeeded; the store ignores preconditions and cannot be used" >&2
    exit 1
  fi
  if ! grep -q 'PreconditionFailed' "$work/err"; then
    echo "s3-init: ${what} failed for another reason:" >&2
    cat "$work/err" >&2
    exit 1
  fi
}

key="_conformance/$(date +%s)-$$-${RANDOM}"
printf 'first\n' >"$work/first"
printf 'second\n' >"$work/second"

s3api put-object --bucket "$S3_BUCKET" --key "$key" --body "$work/first" --if-none-match '*' >/dev/null
expect_412 "create-if-absent on an existing key" \
  s3api put-object --bucket "$S3_BUCKET" --key "$key" --body "$work/second" --if-none-match '*'

etag="$(s3api head-object --bucket "$S3_BUCKET" --key "$key" --query ETag --output text)"
s3api put-object --bucket "$S3_BUCKET" --key "$key" --body "$work/second" --if-match "$etag" >/dev/null
expect_412 "update with a stale etag" \
  s3api put-object --bucket "$S3_BUCKET" --key "$key" --body "$work/first" --if-match "$etag"

s3api delete-object --bucket "$S3_BUCKET" --key "$key" >/dev/null
echo "s3-init: ready (conditional writes verified)"
