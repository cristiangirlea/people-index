#!/usr/bin/env bash
# After two published runs (`make compose-e2e`), checks what the second publish did:
#
#   check-republish.sh es <first_run_id> <second_run_id>       (es-users image: curl)
#     the alias points at <alias>-<second> and nothing else, <alias>-<first> is gone, and no
#     other <alias>-* index is left
#   check-republish.sh summary <first_run_id> <second_run_id>  (s3-init image: aws, jq)
#     runs/<second>/summary.json names <alias>-<second>, publish.previous and publish.deleted
#     are exactly [<alias>-<first>], and publish.not_deleted is empty
#
# It reads as the elastic superuser so the check sees the cluster as it is. What the pipeline
# user may do is proven by the second run itself: a refused alias move fails the run, and a
# refused delete is listed in not_deleted.
set -euo pipefail

usage() {
  echo "usage: $0 es|summary <first_run_id> <second_run_id>" >&2
  exit 2
}
(($# == 3)) || usage
mode="$1"
first_id="$2"
second_id="$3"
[[ "${first_id,,}" != "${second_id,,}" ]] || { echo "check-republish: the two run ids are the same" >&2; exit 2; }
alias_name="${PI_ALIAS:-persons}"
# The pipeline's index name: <alias>-<run_id>, lowercased (pi-stages plan_file::index_name).
first="${alias_name}-${first_id}"
first="${first,,}"
second="${alias_name}-${second_id}"
second="${second,,}"

fail() {
  echo "check-republish: $*" >&2
  exit 1
}

# curl as the elastic superuser over the cluster CA (the es-users service mounts it at /ca).
es() { curl -sS --cacert /ca/ca.crt -u "elastic:${ELASTIC_PASSWORD}" "$@"; }

check_es() {
  : "${ES_URL:?}" "${ELASTIC_PASSWORD:?}"
  local targets indexes status

  # _cat answers one name per line; sort makes the comparison independent of order.
  targets="$(es --fail-with-body "${ES_URL}/_cat/aliases/${alias_name}?h=index" | sort)"
  [[ "$targets" == "$second" ]] ||
    fail "alias ${alias_name} points at [${targets//$'\n'/ }], expected [${second}]"

  # A pattern that matches nothing is an empty answer, not a 404.
  indexes="$(es --fail-with-body "${ES_URL}/_cat/indices/${alias_name}-*?h=index&expand_wildcards=all" | sort)"
  [[ "$indexes" == "$second" ]] ||
    fail "indexes ${alias_name}-*: [${indexes//$'\n'/ }], expected only [${second}]"

  status="$(es -o /dev/null -w '%{http_code}' -I "${ES_URL}/${first}")"
  [[ "$status" == 404 ]] || fail "the first run's index ${first} answers ${status}, expected 404"

  echo "check-republish: alias ${alias_name} -> ${second} only; ${first} deleted"
}

check_summary() {
  : "${S3_ENDPOINT:?}" "${S3_BUCKET:?}"
  local key="s3://${S3_BUCKET}/runs/${second_id}/summary.json" summary
  summary="$(aws --endpoint-url "$S3_ENDPOINT" s3 cp "$key" -)" || fail "cannot read ${key}"
  # The index names come from the summary's own alias (s3-init does not carry PI_ALIAS); the
  # es check holds the alias to the configured one.
  jq -e --arg first_id "$first_id" --arg second_id "$second_id" '
    (.alias + "-" + $first_id | ascii_downcase) as $first
    | (.alias + "-" + $second_id | ascii_downcase) as $second
    | .index == $second
      and .publish.previous == [$first]
      and .publish.deleted == [$first]
      and .publish.not_deleted == []
  ' >/dev/null <<<"$summary" || {
    echo "check-republish: ${key}:" >&2
    jq '{alias, index, publish}' <<<"$summary" >&2 || printf '%s\n' "$summary" >&2
    fail "expected index <alias>-${second_id}, previous and deleted [<alias>-${first_id}], not_deleted []"
  }
  echo "check-republish: ${key}: previous and deleted [<alias>-${first_id}], not_deleted []"
}

case "$mode" in
  es) check_es ;;
  summary) check_summary ;;
  *) usage ;;
esac
