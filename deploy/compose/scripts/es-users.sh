#!/usr/bin/env bash
# Waits for the formed cluster, then creates the pipeline's role and user (idempotent: PUT).
#
# The pipeline never uses the elastic superuser. Role people-index-writer is the same one ECK
# creates on Kubernetes (deploy/k8s/base/elasticsearch/roles.yml lists every call it covers):
# cluster monitor (_cluster/health, _nodes for the shard default) and every privilege on exactly
# two names, the alias `<alias>` and the run indexes `<alias>-*` (`<alias>-<run_id>`). No index
# templates: the pipeline creates its index with the mapping in the request.
set -euo pipefail

: "${ES_URL:?}" "${ES_NODES:?}" "${ELASTIC_PASSWORD:?}" "${PIPELINE_ES_PASSWORD:?}"
alias_name="${PI_ALIAS:-persons}"
ca=/ca/ca.crt
deadline=$((SECONDS + ${ES_WAIT_SECONDS:-300}))

# JSON string escaping for the two characters that can break a quoted value.
json_escape() {
  local s="$1"
  s="${s//\\/\\\\}"
  s="${s//\"/\\\"}"
  printf '%s' "$s"
}

as_elastic() {
  curl -sS --fail-with-body --cacert "$ca" -u "elastic:${ELASTIC_PASSWORD}" \
    -H 'Content-Type: application/json' "$@"
}

echo "es-users: waiting for a cluster of ${ES_NODES} nodes at ${ES_URL}"
until as_elastic "${ES_URL}/_cluster/health?wait_for_nodes=${ES_NODES}&wait_for_status=yellow&timeout=10s" >/dev/null 2>&1; do
  if ((SECONDS >= deadline)); then
    echo "es-users: the cluster did not form in time" >&2
    as_elastic "${ES_URL}/_cluster/health?pretty" >&2 || true
    exit 1
  fi
  sleep 2
done

alias_json="$(json_escape "$alias_name")"
echo "es-users: role people-index-writer on ${alias_name} and ${alias_name}-*"
as_elastic -X PUT "${ES_URL}/_security/role/people-index-writer" -d @- >/dev/null <<EOF
{
  "cluster": ["monitor"],
  "indices": [{ "names": ["${alias_json}", "${alias_json}-*"], "privileges": ["all"] }]
}
EOF

echo "es-users: user pipeline"
as_elastic -X PUT "${ES_URL}/_security/user/pipeline" -d @- >/dev/null <<EOF
{
  "password": "$(json_escape "$PIPELINE_ES_PASSWORD")",
  "roles": ["people-index-writer"],
  "full_name": "people-index pipeline"
}
EOF

# Prove the credentials the pipeline will use actually authenticate.
curl -sS --fail-with-body --cacert "$ca" -u "pipeline:${PIPELINE_ES_PASSWORD}" \
  "${ES_URL}/_security/_authenticate" >/dev/null
echo "es-users: ready"
