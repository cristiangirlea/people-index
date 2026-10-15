#!/usr/bin/env bash
# Waits for the formed cluster, then creates the pipeline's role and user (idempotent: PUT).
#
# The pipeline never uses the elastic superuser. Role people-index-writer is the same one ECK
# creates on Kubernetes: cluster monitor (health, _nodes for the shard default) and
# manage_index_templates, and every privilege on the alias's indexes (`<alias>*`: the run's
# `<alias>-<run_id>` index and the alias itself).
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

echo "es-users: role people-index-writer on ${alias_name}*"
as_elastic -X PUT "${ES_URL}/_security/role/people-index-writer" -d @- >/dev/null <<EOF
{
  "cluster": ["monitor", "manage_index_templates"],
  "indices": [{ "names": ["$(json_escape "$alias_name")*"], "privileges": ["all"] }]
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
