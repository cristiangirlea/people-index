#!/usr/bin/env bash
# After a failed run and two published runs (`make compose-e2e`), checks what the publishes did:
#
#   check-republish.sh left <failed_run_id>                               (es-users image: curl)
#     the failed run left its index <alias>-<failed>, and the alias does not point at it: the
#     state the next publish must clean up
#   check-republish.sh es <first_run_id> <second_run_id> [<failed_run_id>]  (es-users image: curl)
#     the alias points at <alias>-<second> and nothing else, <alias>-<first> (and
#     <alias>-<failed>) is gone, and no other <alias>-* index is left
#   check-republish.sh summary <first_run_id> <second_run_id> [<failed_run_id>]  (s3-init image: aws, jq)
#     runs/<second>/summary.json names <alias>-<second>, publish.previous and publish.deleted
#     are exactly [<alias>-<first>], and publish.not_deleted is empty; with a failed run,
#     runs/<first>/summary.json lists <alias>-<failed> under publish.deleted (an older run's
#     index, never on the alias) and publish.not_deleted is empty
#
# It reads as the elastic superuser so the check sees the cluster as it is. What the pipeline
# user may do is proven by the runs themselves: a refused alias move fails the run, and a
# refused listing or delete is listed in not_deleted.
set -euo pipefail

usage() {
  echo "usage: $0 left <failed_run_id> | es|summary <first_run_id> <second_run_id> [<failed_run_id>]" >&2
  exit 2
}
(($# >= 2)) || usage
mode="$1"
alias_name="${PI_ALIAS:-persons}"
# The pipeline's index name: <alias>-<run_id>, lowercased (pi-stages plan_file::index_name).
index_of() {
  local name="${alias_name}-$1"
  printf '%s' "${name,,}"
}
if [[ "$mode" == left ]]; then
  (($# == 2)) || usage
  failed_id="$2"
else
  (($# == 3 || $# == 4)) || usage
  first_id="$2"
  second_id="$3"
  failed_id="${4:-}"
  [[ "${first_id,,}" != "${second_id,,}" ]] || { echo "check-republish: the two run ids are the same" >&2; exit 2; }
  if [[ -n "$failed_id" ]]; then
    [[ "${failed_id,,}" != "${first_id,,}" && "${failed_id,,}" != "${second_id,,}" ]] ||
      { echo "check-republish: the failed run id repeats a published one" >&2; exit 2; }
  fi
  first="$(index_of "$first_id")"
  second="$(index_of "$second_id")"
fi
failed="$( [[ -z "${failed_id:-}" ]] || index_of "$failed_id")"

fail() {
  echo "check-republish: $*" >&2
  exit 1
}

# curl as the elastic superuser over the cluster CA (the es-users service mounts it at /ca).
es() { curl -sS --cacert /ca/ca.crt -u "elastic:${ELASTIC_PASSWORD}" "$@"; }

# The HTTP status of a HEAD on one index.
head_status() { es -o /dev/null -w '%{http_code}' -I "${ES_URL}/$1"; }

check_left() {
  : "${ES_URL:?}" "${ELASTIC_PASSWORD:?}"
  local status targets
  status="$(head_status "$failed")"
  [[ "$status" == 200 ]] || fail "the failed run's index ${failed} answers ${status}, expected 200"
  # A pattern or alias that matches nothing is an empty answer, not a 404.
  targets="$(es --fail-with-body "${ES_URL}/_cat/aliases/${alias_name}?h=index" | sort)"
  [[ $'\n'"$targets"$'\n' != *$'\n'"$failed"$'\n'* ]] || fail "alias ${alias_name} points at the failed run's index ${failed}"
  echo "check-republish: the failed run left ${failed}, not on alias ${alias_name}"
}

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

  status="$(head_status "$first")"
  [[ "$status" == 404 ]] || fail "the first run's index ${first} answers ${status}, expected 404"
  if [[ -n "$failed" ]]; then
    status="$(head_status "$failed")"
    [[ "$status" == 404 ]] || fail "the failed run's index ${failed} answers ${status}, expected 404"
  fi

  echo "check-republish: alias ${alias_name} -> ${second} only; ${first}${failed:+ and ${failed}} deleted"
}

# summary_of <run_id>: that run's summary.json on stdout.
summary_of() {
  local key="s3://${S3_BUCKET}/runs/$1/summary.json"
  aws --endpoint-url "$S3_ENDPOINT" s3 cp "$key" - || fail "cannot read ${key}"
}

# show_and_fail <summary> <run_id> <message>
show_and_fail() {
  echo "check-republish: s3://${S3_BUCKET}/runs/$2/summary.json:" >&2
  jq '{alias, index, publish}' <<<"$1" >&2 || printf '%s\n' "$1" >&2
  fail "$3"
}

check_summary() {
  : "${S3_ENDPOINT:?}" "${S3_BUCKET:?}"
  local summary
  summary="$(summary_of "$second_id")"
  # The index names come from the summary's own alias (s3-init does not carry PI_ALIAS); the
  # es check holds the alias to the configured one.
  jq -e --arg first_id "$first_id" --arg second_id "$second_id" '
    (.alias + "-" + $first_id | ascii_downcase) as $first
    | (.alias + "-" + $second_id | ascii_downcase) as $second
    | .index == $second
      and .publish.previous == [$first]
      and .publish.deleted == [$first]
      and .publish.not_deleted == []
  ' >/dev/null <<<"$summary" ||
    show_and_fail "$summary" "$second_id" \
      "expected index <alias>-${second_id}, previous and deleted [<alias>-${first_id}], not_deleted []"
  echo "check-republish: runs/${second_id}: previous and deleted [<alias>-${first_id}], not_deleted []"

  [[ -n "$failed_id" ]] || return 0
  summary="$(summary_of "$first_id")"
  jq -e --arg first_id "$first_id" --arg failed_id "$failed_id" '
    (.alias + "-" + $first_id | ascii_downcase) as $first
    | (.alias + "-" + $failed_id | ascii_downcase) as $failed
    | .index == $first
      and all(.publish.previous[]; . != $failed)
      and any(.publish.deleted[]; . == $failed)
      and .publish.not_deleted == []
  ' >/dev/null <<<"$summary" ||
    show_and_fail "$summary" "$first_id" \
      "expected index <alias>-${first_id}, <alias>-${failed_id} deleted but not previous, not_deleted []"
  echo "check-republish: runs/${first_id}: <alias>-${failed_id} deleted as an older run's index"
}

case "$mode" in
  left) check_left ;;
  es) check_es ;;
  summary) check_summary ;;
  *) usage ;;
esac
