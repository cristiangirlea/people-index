#!/bin/sh
# Creates the pipeline's bucket in the in-cluster S3 store, then proves the credentials can
# reach it. Safe to run any number of times: 200 (created) and 409 (already there) both pass,
# and the HEAD afterwards is what decides success.
#
# The credentials go to curl on stdin (-K -), never on its command line, so they do not show up
# in the process list.
set -eu

endpoint="${S3_ENDPOINT:?S3_ENDPOINT is required}"
bucket="${S3_BUCKET:?S3_BUCKET is required}"
region="${AWS_REGION:-us-east-1}"
: "${AWS_ACCESS_KEY_ID:?AWS_ACCESS_KEY_ID is required}"
: "${AWS_SECRET_ACCESS_KEY:?AWS_SECRET_ACCESS_KEY is required}"
attempts="${ATTEMPTS:-60}"

signed() {
  printf 'user = "%s:%s"\n' "$AWS_ACCESS_KEY_ID" "$AWS_SECRET_ACCESS_KEY" |
    curl -K - -sS -m 10 --aws-sigv4 "aws:amz:${region}:s3" "$@"
}

# The store's health endpoint needs no credentials: wait for it before signing anything.
i=0
until curl -sS -m 5 -o /dev/null -f "${endpoint}/healthz"; do
  i=$((i + 1))
  if [ "$i" -ge "$attempts" ]; then
    echo "bucket-init: ${endpoint} not healthy after ${attempts} attempts" >&2
    exit 1
  fi
  sleep 2
done

status="$(signed -o /tmp/create.out -w '%{http_code}' -X PUT "${endpoint}/${bucket}")"
case "$status" in
  200 | 409) ;;
  *)
    echo "bucket-init: creating ${bucket} returned ${status}" >&2
    cat /tmp/create.out >&2 || true
    exit 1
    ;;
esac

status="$(signed -o /dev/null -w '%{http_code}' -I "${endpoint}/${bucket}")"
if [ "$status" != "200" ]; then
  echo "bucket-init: HEAD ${bucket} returned ${status}; the credentials cannot use it" >&2
  exit 1
fi
echo "bucket-init: ${bucket} ready at ${endpoint}"
