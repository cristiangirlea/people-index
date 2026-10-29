#!/bin/sh
# Generate the local gateway configuration without printing credentials.
set -eu
: "${PIPELINE_KEY:?}" "${PIPELINE_SECRET:?}" "${SNAPSHOT_KEY:?}" "${SNAPSHOT_SECRET:?}" "${PIPELINE_BUCKET:?}" "${SNAPSHOT_BUCKET:?}"
# Restrict credentials to portable characters so JSON cannot be injected.
for value in "$PIPELINE_KEY" "$PIPELINE_SECRET" "$SNAPSHOT_KEY" "$SNAPSHOT_SECRET"; do
  case "$value" in *[!A-Za-z0-9_-]*) echo "identity-config: credentials must use letters, digits, _ or -" >&2; exit 1;; esac
done
for value in "$PIPELINE_BUCKET" "$SNAPSHOT_BUCKET"; do
  case "$value" in *[!A-Za-z0-9_.-]*) echo "identity-config: invalid bucket name" >&2; exit 1;; esac
done
[ "$PIPELINE_BUCKET" != "$SNAPSHOT_BUCKET" ] || { echo "identity-config: buckets must differ" >&2; exit 1; }
[ "$PIPELINE_KEY" != "$SNAPSHOT_KEY" ] || { echo "identity-config: access keys must differ" >&2; exit 1; }
umask 077
printf '{"identities":[{"name":"pipeline","credentials":[{"accessKey":"%s","secretKey":"%s"}],"actions":["Admin:%s","Read:%s","Write:%s","List:%s"]},{"name":"snapshots","credentials":[{"accessKey":"%s","secretKey":"%s"}],"actions":["Admin:%s","Read:%s","Write:%s","List:%s"]}]}\n' "$PIPELINE_KEY" "$PIPELINE_SECRET" "$PIPELINE_BUCKET" "$PIPELINE_BUCKET" "$PIPELINE_BUCKET" "$PIPELINE_BUCKET" "$SNAPSHOT_KEY" "$SNAPSHOT_SECRET" "$SNAPSHOT_BUCKET" "$SNAPSHOT_BUCKET" "$SNAPSHOT_BUCKET" "$SNAPSHOT_BUCKET" > /config/s3.json
