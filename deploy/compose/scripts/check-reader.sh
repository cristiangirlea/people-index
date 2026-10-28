#!/usr/bin/env bash
# Check the reader role and the documented API key against the second published index.
set -euo pipefail
: "${ES_URL:?}" "${ELASTIC_PASSWORD:?}" "${READER_ES_PASSWORD:?}"
[[ $# == 1 ]] || { echo "usage: $0 <run_id>" >&2; exit 2; }
alias_name="${PI_ALIAS:-persons}"
index="${alias_name}-$1"
index="${index,,}"
body="$(mktemp)"
key_id=""
pit=""
cleanup() {
  if [[ -n "$pit" ]]; then
    curl -sS --cacert /ca/ca.crt -u "elastic:${ELASTIC_PASSWORD}" -H 'Content-Type: application/json' \
      -X DELETE "${ES_URL}/_pit" -d "{\"id\":\"${pit}\"}" >/dev/null || true
  fi
  if [[ -n "$key_id" ]]; then
    curl -sS --cacert /ca/ca.crt -u "elastic:${ELASTIC_PASSWORD}" -H 'Content-Type: application/json' \
      -X DELETE "${ES_URL}/_security/api_key" -d "{\"ids\":[\"${key_id}\"]}" >/dev/null || true
  fi
  rm -f "$body"
}
trap cleanup EXIT
request() {
  local expected="$1"; shift
  local status
  status="$(curl -sS --cacert /ca/ca.crt -o "$body" -w '%{http_code}' "$@")"
  if [[ "$status" != "$expected" ]]; then
    echo "check-reader: expected HTTP ${expected}, got ${status}" >&2
    cat "$body" >&2
    exit 1
  fi
}
admin=(-u "elastic:${ELASTIC_PASSWORD}")
reader=(-u "reader:${READER_ES_PASSWORD}")
json=(-H 'Content-Type: application/json')
request 200 "${admin[@]}" "${ES_URL}/${alias_name}/_count"
expected_count="$(sed -n 's/.*"count":\([0-9]*\).*/\1/p' "$body")"
[[ -n "$expected_count" ]] || { echo 'check-reader: missing count' >&2; exit 1; }
request 200 "${reader[@]}" "${json[@]}" -X POST "${ES_URL}/${alias_name}/_search" -d '{"size":0,"track_total_hits":true}'
count="$(sed -n 's/.*"total":{"value":\([0-9]*\),"relation":"eq"}.*/\1/p' "$body")"
[[ "$count" == "$expected_count" ]] || { echo "check-reader: count ${count}, expected ${expected_count}" >&2; exit 1; }
request 200 "${reader[@]}" "${ES_URL}/${alias_name}/_mapping"
request 403 "${reader[@]}" -X POST "${ES_URL}/${index}/_search"
request 403 "${reader[@]}" "${json[@]}" -X POST "${ES_URL}/${alias_name}/_doc" -d '{}'
request 403 "${reader[@]}" -X DELETE "${ES_URL}/${index}"
request 403 "${reader[@]}" "${ES_URL}/.security/_search"
request 403 "${reader[@]}" "${ES_URL}/_cluster/health"
request 200 "${reader[@]}" -X POST "${ES_URL}/${alias_name}/_pit?keep_alive=1m"
pit="$(sed -n 's/.*"id":"\([^"]*\)".*/\1/p' "$body")"
[[ -n "$pit" ]] || { echo 'check-reader: missing PIT id' >&2; exit 1; }
request 403 "${reader[@]}" "${json[@]}" -X POST "${ES_URL}/_search" -d "{\"pit\":{\"id\":\"${pit}\",\"keep_alive\":\"1m\"}}"
# Alias is an Elasticsearch index name and cannot contain JSON quoting characters.
request 200 "${admin[@]}" "${json[@]}" -X POST "${ES_URL}/_security/api_key" -d "{\"name\":\"people-index-reader\",\"expiration\":\"90d\",\"role_descriptors\":{\"people-index-reader\":{\"cluster\":[],\"indices\":[{\"names\":[\"${alias_name}\"],\"privileges\":[\"read\",\"view_index_metadata\"]}]}}}"
key_id="$(sed -n 's/.*"id":"\([^"]*\)".*/\1/p' "$body")"
key="$(sed -n 's/.*"encoded":"\([^"]*\)".*/\1/p' "$body")"
[[ -n "$key_id" && -n "$key" ]] || { echo 'check-reader: missing API key' >&2; exit 1; }
request 200 -H "Authorization: ApiKey ${key}" "${json[@]}" -X POST "${ES_URL}/${alias_name}/_search" -d '{"size":0}'
request 403 -H "Authorization: ApiKey ${key}" -X POST "${ES_URL}/${index}/_search"
request 200 "${admin[@]}" "${json[@]}" -X DELETE "${ES_URL}/_security/api_key" -d "{\"ids\":[\"${key_id}\"]}"
key_id=""
request 401 -H "Authorization: ApiKey ${key}" "${json[@]}" -X POST "${ES_URL}/${alias_name}/_search" -d '{"size":0}'
echo "check-reader: alias count ${count}; permissions, PIT and API key verified"
