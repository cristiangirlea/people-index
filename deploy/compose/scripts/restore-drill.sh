#!/usr/bin/env bash
# The rollback and restore runbook (docs/OPERATIONS.md, "Rollback and restore"), exercised
# against the running compose stack: snapshot the published index, lose it, restore it under
# its own name, restore it again under a new name beside it, and re-point the alias each time.
#
#   restore-drill.sh [<run_id>]     on the host, beside a running stack (make compose-restore-drill)
#
# The alias must point at exactly one index, <alias>-<run_id> when a run id is given, and
# <index>-restored must not exist. The drill ends as it started: the alias on the same index
# name with the same documents. It removes what it added (its snapshot, the repository
# registration if it made it, the keystore entries it put in, the renamed copy); a repository
# or keys that were there before are left as they were. A failure keeps the snapshot and the
# repository so the index can still be restored by hand, and still removes the keys it added.
#
# Elasticsearch reads S3 keys only from its keystore (secure settings); the client settings
# (endpoint, path-style access, region) are in compose.yaml. On the host the drill pipes the
# snapshot identity's keys from the `s3` service's environment into every node's keystore
# (`elasticsearch-keystore add --stdin`), so they never appear on a command line or in output,
# and removes them again at the end. Everything else runs in the es-users image (curl, the
# CA, the elastic and reader passwords) as
#
#   restore-drill.sh in-container drill [<run_id>]
#   restore-drill.sh in-container reload      POST _nodes/reload_secure_settings
set -euo pipefail
shopt -s inherit_errexit

repo="${SNAPSHOT_REPOSITORY:-people-index}"
bucket="${SNAPSHOT_BUCKET:-people-index-snapshots}"
base_path="${SNAPSHOT_BASE_PATH:-snapshots}"
secure_keys=(s3.client.default.access_key s3.client.default.secret_key)

usage() {
  echo "usage: $0 [<run_id>]" >&2
  exit 2
}

fail() {
  echo "restore-drill: $*" >&2
  exit 1
}

# ---------------------------------------------------------------------------------------------
# On the host: the keystore, then the drill in the es-users image.

compose() { docker compose -f "$compose_file" "$@"; }

# in_container <mode> [args]: this script in the es-users image. MSYS_NO_PATHCONV keeps Git Bash
# on Windows from rewriting /scripts/... into a Windows path; elsewhere it means nothing.
in_container() {
  MSYS_NO_PATHCONV=1 compose run --rm --no-deps --entrypoint bash \
    es-users /scripts/restore-drill.sh in-container "$@"
}

# Runs on exit: take out the keys the drill put in, then reload so the nodes drop them.
host_cleanup() {
  local status=$? node
  if ((${#added[@]} > 0)); then
    for node in "${added[@]}"; do
      compose exec -T "$node" bin/elasticsearch-keystore remove "${secure_keys[@]}" || status=1
    done
    in_container reload || status=1
    echo "restore-drill: snapshot keys removed from ${added[*]}"
    if ((status != 0)); then
      echo "restore-drill: to restore a kept snapshot by hand, put the keys back first" \
        "(docs/OPERATIONS.md, Rollback and restore, Access: the keystore step)" >&2
    fi
  fi
  exit "$status"
}

host() {
  (($# <= 1)) || usage
  local node nodes listed have key
  # Git Bash: a Windows path (pwd -W), which MSYS_NO_PATHCONV below leaves as it is.
  compose_file="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && { pwd -W 2>/dev/null || pwd; })/compose.yaml"
  added=()

  mapfile -t nodes < <(compose ps --services --status running | grep -E '^es[0-9]+$' | sort)
  ((${#nodes[@]} > 0)) || fail "no running Elasticsearch node; start the stack first (make compose-up)"
  # Single quotes: the variables are the s3 container's, expanded there.
  # shellcheck disable=SC2016
  compose exec -T s3 sh -c 'test -n "$SNAPSHOT_KEY" && test -n "$SNAPSHOT_SECRET"' ||
    fail "the s3 service has no snapshot identity (SNAPSHOT_KEY, SNAPSHOT_SECRET)"

  trap host_cleanup EXIT
  for node in "${nodes[@]}"; do
    # Keys someone put there are theirs: use them and leave them. Half a pair is refused
    # rather than completed or overwritten.
    listed="$(compose exec -T "$node" bin/elasticsearch-keystore list)"
    have=0
    for key in "${secure_keys[@]}"; do
      if grep -Fx "$key" <<<"$listed" >/dev/null; then have=$((have + 1)); fi
    done
    if ((have == ${#secure_keys[@]})); then
      echo "restore-drill: ${node} already has snapshot keys; using them"
      continue
    fi
    ((have == 0)) || fail "${node} has only some of ${secure_keys[*]} in its keystore; fix it by hand"
    added+=("$node")
    compose exec -T s3 printenv SNAPSHOT_KEY |
      compose exec -T "$node" bin/elasticsearch-keystore add --stdin --force "${secure_keys[0]}"
    compose exec -T s3 printenv SNAPSHOT_SECRET |
      compose exec -T "$node" bin/elasticsearch-keystore add --stdin --force "${secure_keys[1]}"
  done
  echo "restore-drill: snapshot keys in the keystore of ${nodes[*]}"
  in_container reload
  in_container drill "$@"
}

# ---------------------------------------------------------------------------------------------
# In the es-users image.

es_setup() {
  : "${ES_URL:?}" "${ELASTIC_PASSWORD:?}"
  work="$(mktemp -d)"
  body="$work/body"
}

# call <user> <password> <expected status> <curl args...>: one request; the response is in
# $body. The expected status may list alternatives (200|404). The password goes to curl on
# stdin (-K -), never on its command line.
call() {
  local user="$1" password="$2" expected="$3" status
  shift 3
  status="$(printf 'user = "%s:%s"\n' "$user" "$password" |
    curl -K - -sS -m 900 --cacert /ca/ca.crt -H 'Content-Type: application/json' \
      -o "$body" -w '%{http_code}' "$@")" || status="curl failed"
  if ! [[ "$status" =~ ^(${expected})$ ]]; then
    echo "restore-drill: ${user}: expected HTTP ${expected}, got ${status} from ${*: -1}" >&2
    cat "$body" >&2 2>/dev/null || true
    echo >&2
    exit 1
  fi
}
es() { call elastic "$ELASTIC_PASSWORD" "$@"; }
as_reader() { call reader "$READER_ES_PASSWORD" "$@"; }

# status_of <url>: the HTTP status of a GET as the elastic superuser.
status_of() {
  printf 'user = "elastic:%s"\n' "$ELASTIC_PASSWORD" |
    curl -K - -sS -m 60 --cacert /ca/ca.crt -o /dev/null -w '%{http_code}' "$1"
}

now_ms() { date +%s%3N; }

reload() {
  es_setup
  trap 'rm -rf "$work"' EXIT
  es 200 -X POST "${ES_URL}/_nodes/reload_secure_settings"
  grep -q '"failed":0' "$body" || fail "reload_secure_settings failed on a node: $(cat "$body")"
  # A node that could not apply the settings reports it per node, not in the failed count.
  if grep -q 'reload_exception' "$body"; then
    fail "reload_secure_settings: a node could not reload: $(cat "$body")"
  fi
  echo "restore-drill: secure settings reloaded on every node"
}

# Runs on exit: on success remove the snapshot (and the repository if the drill registered
# it); on failure keep both, since the snapshot may be the only copy of the index.
drill_cleanup() {
  local status=$?
  if [[ "$done_ok" == yes ]]; then
    es 200 -X DELETE "${ES_URL}/_snapshot/${repo}/${snapshot}"
    if [[ "$created_repo" == yes ]]; then
      es 200 -X DELETE "${ES_URL}/_snapshot/${repo}"
    fi
  elif [[ -n "$snapshot" ]]; then
    echo "restore-drill: failed; kept snapshot ${repo}/${snapshot} for a restore by hand" >&2
  fi
  rm -rf "$work"
  exit "$status"
}

# read_alias: sets $aliased to the indexes the alias points at, sorted, space-separated
# (empty, after a 404, when it points at none).
read_alias() {
  es '200|404' "${ES_URL}/_alias/${alias_name}?ignore_unavailable=true"
  aliased="$(grep -o '"[^"]*":{"aliases"' "$body" | sed 's/^"\([^"]*\)".*/\1/' | sort | tr '\n' ' ')" ||
    aliased=""
  aliased="${aliased% }"
}

# count_of <index>: sets $count to its document count after a refresh.
count_of() {
  es 200 -X POST "${ES_URL}/$1/_refresh"
  es 200 "${ES_URL}/$1/_count"
  count="$(sed -n 's/.*"count":\([0-9]*\).*/\1/p' "$body")"
  [[ -n "$count" ]] || fail "no document count for $1: $(cat "$body")"
}

# reader_sees <n>: the reader user finds exactly n documents through the alias.
reader_sees() {
  local seen
  as_reader 200 -X POST "${ES_URL}/${alias_name}/_search" -d '{"size":0,"track_total_hits":true}'
  seen="$(sed -n 's/.*"total":{"value":\([0-9]*\),"relation":"eq"}.*/\1/p' "$body")"
  [[ "$seen" == "$1" ]] || fail "the reader sees ${seen:-no} documents through ${alias_name}, expected $1"
}

wait_green() {
  es 200 "${ES_URL}/_cluster/health/$1?wait_for_status=green&timeout=300s"
  grep -q '"timed_out":false' "$body" || fail "$1 did not reach green: $(cat "$body")"
}

# restore <request body>: restore from the drill's snapshot; returns once the primaries are in.
restore() {
  es 200 -X POST "${ES_URL}/_snapshot/${repo}/${snapshot}/_restore?wait_for_completion=true" -d "$1"
  grep -q '"failed":0' "$body" || fail "the restore had failed shards: $(cat "$body")"
}

# point_alias <index>: the runbook's single _aliases request: off whatever <alias>-* index it
# points at and onto <index>, atomically.
point_alias() {
  es 200 -X POST "${ES_URL}/_aliases" -d "{\"actions\":[
    {\"remove\":{\"index\":\"${alias_name}-*\",\"alias\":\"${alias_name}\",\"must_exist\":false}},
    {\"add\":{\"index\":\"$1\",\"alias\":\"${alias_name}\"}}]}"
  read_alias
  [[ "$aliased" == "$1" ]] || fail "the alias points at '${aliased}', not at $1 alone"
}

drill() {
  (($# <= 1)) || usage
  es_setup
  : "${READER_ES_PASSWORD:?}"
  alias_name="${PI_ALIAS:-persons}"
  snapshot=""
  created_repo=no
  done_ok=no
  local index renamed docs version want t0 t_snapshot t_restore t_rename
  trap drill_cleanup EXIT

  es 200 "${ES_URL}/"
  version="$(sed -n 's/.*"number" *: *"\([^"]*\)".*/\1/p' "$body" | head -1)"

  read_alias
  [[ -n "$aliased" && "$aliased" != *" "* ]] || fail "the alias ${alias_name} points at '${aliased}'; expected exactly one index"
  index="$aliased"
  if (($# == 1)); then
    want="${alias_name}-$1"
    [[ "$index" == "${want,,}" ]] || fail "the alias points at ${index}, not ${want,,}"
  fi
  renamed="${index}-restored"
  [[ "$(status_of "${ES_URL}/${renamed}")" == 404 ]] ||
    fail "${renamed} exists (or the check failed); the drill restores a copy under that name"
  count_of "$index"
  docs="$count"
  reader_sees "$docs"
  echo "restore-drill: Elasticsearch ${version}; ${alias_name} -> ${index}, ${docs} documents"

  # 1. The repository, as deploy/k8s/components/snapshots registers it. The PUT verifies it on
  #    every node: a node without the keys fails here, naming itself.
  if [[ "$(status_of "${ES_URL}/_snapshot/${repo}")" == 200 ]]; then
    echo "restore-drill: repository ${repo} exists; using it"
  else
    es 200 -X PUT "${ES_URL}/_snapshot/${repo}" \
      -d "{\"type\":\"s3\",\"settings\":{\"bucket\":\"${bucket}\",\"base_path\":\"${base_path}\",\"client\":\"default\"}}"
    created_repo=yes
    echo "restore-drill: registered repository ${repo} (bucket ${bucket}, ${base_path}/), verified on every node"
  fi

  # 2. Snapshot the published index, as the SLM policy does (no global state).
  snapshot="drill-${index}-$(date -u +%Y%m%d%H%M%S)"
  t0="$(now_ms)"
  es 200 -X PUT "${ES_URL}/_snapshot/${repo}/${snapshot}?wait_for_completion=true" \
    -d "{\"indices\":\"${index}\",\"include_global_state\":false}"
  t_snapshot=$(($(now_ms) - t0))
  grep -q '"state":"SUCCESS"' "$body" || fail "snapshot ${snapshot} did not succeed: $(cat "$body")"
  echo "restore-drill: snapshot ${repo}/${snapshot} in ${t_snapshot} ms"

  # 3. Lose the index. Its alias goes with it: the reader finds nothing to search.
  es 200 -X DELETE "${ES_URL}/${index}"
  as_reader 404 -X POST "${ES_URL}/${alias_name}/_search" -d '{"size":0}'
  echo "restore-drill: deleted ${index}; ${alias_name} is gone"

  # 4. Restore it under its own name without the aliases the snapshot recorded, wait for
  #    green, check the count, then point the alias at it.
  t0="$(now_ms)"
  restore "{\"indices\":\"${index}\",\"include_global_state\":false,\"include_aliases\":false}"
  wait_green "$index"
  t_restore=$(($(now_ms) - t0))
  read_alias
  [[ -z "$aliased" ]] || fail "the restore put ${alias_name} back on '${aliased}'; include_aliases was ignored"
  count_of "$index"
  [[ "$count" == "$docs" ]] || fail "${index} restored with ${count} documents, expected ${docs}"
  point_alias "$index"
  reader_sees "$docs"
  echo "restore-drill: restored ${index} (green, ${count} documents) in ${t_restore} ms; alias on it; reader sees ${docs}"

  # 5. Restore it again while the name exists: renamed, beside it; move the alias over in one
  #    request, and back.
  t0="$(now_ms)"
  restore "{\"indices\":\"${index}\",\"include_global_state\":false,\"include_aliases\":false,\"rename_pattern\":\"(.+)\",\"rename_replacement\":\"\$1-restored\"}"
  wait_green "$renamed"
  t_rename=$(($(now_ms) - t0))
  count_of "$renamed"
  [[ "$count" == "$docs" ]] || fail "${renamed} restored with ${count} documents, expected ${docs}"
  point_alias "$renamed"
  reader_sees "$docs"
  # The reader role names only the alias, so the index behind it stays out of reach.
  as_reader 403 -X POST "${ES_URL}/${renamed}/_search" -d '{"size":0}'
  echo "restore-drill: restored ${renamed} beside ${index} in ${t_rename} ms; alias moved to it; reader sees ${docs}"
  point_alias "$index"
  es 200 -X DELETE "${ES_URL}/${renamed}"
  reader_sees "$docs"

  done_ok=yes
  echo "restore-drill: ok: Elasticsearch ${version}, ${index}, ${docs} documents; snapshot ${t_snapshot} ms," \
    "restore under its own name ${t_restore} ms, renamed ${t_rename} ms (each to green)"
}

if [[ "${1:-}" == in-container ]]; then
  shift
  case "${1:-}" in
    reload) reload ;;
    drill) shift; drill "$@" ;;
    *) usage ;;
  esac
else
  host "$@"
fi
