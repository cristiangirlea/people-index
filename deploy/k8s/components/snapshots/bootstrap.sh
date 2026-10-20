#!/bin/sh
# Registers the object store as an Elasticsearch snapshot repository and installs the snapshot
# lifecycle policy. Both are PUTs of the full definition, so a re-run converges instead of
# failing. Registering the repository also verifies it: every master and data node writes and
# reads a test blob, which catches a wrong endpoint or missing keystore entry here rather than
# at the first nightly snapshot.
#
# The password goes to curl on stdin (-K -), never on its command line.
set -eu

es="${ES_URL:?ES_URL is required}"
ca="${ES_CA_CERT:?ES_CA_CERT is required}"
user="${ES_USERNAME:?ES_USERNAME is required}"
: "${ES_PASSWORD:?ES_PASSWORD is required}"
repo="${SNAPSHOT_REPOSITORY:-people-index}"
bucket="${SNAPSHOT_BUCKET:?SNAPSHOT_BUCKET is required}"
base_path="${SNAPSHOT_BASE_PATH:-snapshots}"
policy="${SLM_POLICY:-nightly}"
schedule="${SLM_SCHEDULE:-0 30 1 * * ?}"
indices="${SLM_INDICES:-persons*}"
expire_after="${SLM_EXPIRE_AFTER:-14d}"
attempts="${ATTEMPTS:-90}"

es_curl() {
  printf 'user = "%s:%s"\n' "$user" "$ES_PASSWORD" |
    curl -K - -sS -m 60 --cacert "$ca" -H 'Content-Type: application/json' "$@"
}

# PUT a JSON body and fail with the response when Elasticsearch does not answer 200.
put_json() {
  path="$1"
  body="$2"
  status="$(es_curl -o /tmp/put.out -w '%{http_code}' -X PUT "${es}${path}" --data-binary "$body")"
  if [ "$status" != "200" ]; then
    echo "snapshot-bootstrap: PUT ${path} returned ${status}" >&2
    cat /tmp/put.out >&2 || true
    echo >&2
    exit 1
  fi
  echo "snapshot-bootstrap: PUT ${path} ok"
}

i=0
until es_curl -o /dev/null -f "${es}/_cluster/health?wait_for_status=yellow&timeout=10s"; do
  i=$((i + 1))
  if [ "$i" -ge "$attempts" ]; then
    echo "snapshot-bootstrap: cluster at ${es} not yellow after ${attempts} attempts" >&2
    exit 1
  fi
  sleep 5
done

put_json "/_snapshot/${repo}" "$(printf '{"type":"s3","settings":{"bucket":"%s","base_path":"%s","client":"default"}}' \
  "$bucket" "$base_path")"

put_json "/_slm/policy/${policy}" "$(printf '{"schedule":"%s","name":"<%s-snap-{now/d}>","repository":"%s","config":{"indices":["%s"],"include_global_state":false,"ignore_unavailable":true},"retention":{"expire_after":"%s","min_count":3,"max_count":30}}' \
  "$schedule" "$policy" "$repo" "$indices" "$expire_after")"
