#!/usr/bin/env bash
# Tests deploy/compose/scripts/check-republish.sh against stub curl and aws commands on PATH:
# it passes on the state a failed run and two good publishes leave and fails on every other one
# (alias on both indexes, the first or the failed run's index left, a summary with not_deleted
# or the wrong previous/deleted, unreadable or malformed input). The summary cases need jq, as
# the s3-init image has; without it they are skipped.
set -euo pipefail
src="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
check="$src/deploy/compose/scripts/check-republish.sh"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
fail() { echo "test-check-republish: $*" >&2; exit 1; }

# curl: _cat/aliases answers FAKE_ALIASES, _cat/indices answers FAKE_INDICES, a HEAD (a -w
# request) answers the status FAKE_HEADS gives its URL ("<url> <status>" per line);
# FAKE_CURL_FAIL=1 fails like a refused connection. Any other call is an error, so a check
# aimed at the wrong URL fails.
mkdir "$tmp/bin"
cat >"$tmp/bin/curl" <<'EOF'
#!/usr/bin/env bash
[[ "${FAKE_CURL_FAIL:-0}" == 1 ]] && { echo "curl: (7) Failed to connect" >&2; exit 7; }
url="${*: -1}"
case " $* " in
  *" -w "*)
    while read -r head status; do
      [[ "$head" == "$url" ]] && { printf '%s' "$status"; exit 0; }
    done <<<"$FAKE_HEADS" ;;
  *) case "$url" in
       "$ES_URL/_cat/aliases/persons?h=index") printf '%s\n' "$FAKE_ALIASES"; exit 0 ;;
       "$ES_URL/_cat/indices/persons-*?h=index&expand_wildcards=all") printf '%s\n' "$FAKE_INDICES"; exit 0 ;;
     esac ;;
esac
echo "stub curl: unexpected call: $*" >&2
exit 3
EOF
# aws: `s3 cp s3://b/runs/<id>/summary.json -` prints FAKE_SUMMARIES/<id>.json, or fails when it
# does not exist; any other call is an error.
cat >"$tmp/bin/aws" <<'EOF'
#!/usr/bin/env bash
[[ "$1 $2 $3 $4" == "--endpoint-url $S3_ENDPOINT s3 cp" && "$#" == 6 && "$6" == - ]] ||
  { echo "stub aws: unexpected call: $*" >&2; exit 3; }
key="$5"
id="${key#s3://b/runs/}"
id="${id%/summary.json}"
[[ "s3://b/runs/$id/summary.json" == "$key" ]] || { echo "stub aws: unexpected key: $key" >&2; exit 3; }
[[ -f "$FAKE_SUMMARIES/$id.json" ]] || { echo "fatal error: Not Found" >&2; exit 1; }
cat "$FAKE_SUMMARIES/$id.json"
EOF
chmod +x "$tmp/bin/curl" "$tmp/bin/aws"
export PATH="$tmp/bin:$PATH" ES_URL=https://es.invalid:9200 ELASTIC_PASSWORD=x \
  S3_ENDPOINT=http://s3.invalid S3_BUCKET=b PI_ALIAS=persons FAKE_SUMMARIES="$tmp/summaries"
f=20261020-095900
a=20261020-100000
b=20261020-100000-2

# expect <exit code> <description> <args...>: the check's exit code must match.
expect() {
  local want="$1" what="$2" got=0
  shift 2
  bash "$check" "$@" >"$tmp/out" 2>&1 || got=$?
  [[ "$got" == "$want" ]] || fail "$what: exit $got, expected $want: $(cat "$tmp/out")"
}

heads() { FAKE_HEADS="$ES_URL/persons-$a $1"$'\n'"$ES_URL/persons-$f $2"; }
good_es() {
  export FAKE_ALIASES="persons-$b" FAKE_INDICES="persons-$b" FAKE_CURL_FAIL=0 FAKE_HEADS
  heads 404 404
}

good_es
expect 0 "a clean second publish" es "$a" "$b"
expect 0 "a clean second publish after a failed run" es "$a" "$b" "$f"
good_es; FAKE_ALIASES="persons-$a"$'\n'"persons-$b"
expect 1 "the alias on both indexes" es "$a" "$b"
good_es; FAKE_ALIASES=""
expect 1 "no alias" es "$a" "$b"
good_es; FAKE_INDICES="persons-$a"$'\n'"persons-$b"
expect 1 "the first index left" es "$a" "$b"
good_es; FAKE_INDICES="persons-$b"$'\n'"persons-$f"
expect 1 "the failed run's index listed" es "$a" "$b" "$f"
good_es; heads 200 404
expect 1 "the first index still answers" es "$a" "$b"
good_es; heads 404 200
expect 1 "the failed run's index still answers" es "$a" "$b" "$f"
expect 0 "a failed run's index not checked without its id" es "$a" "$b"
good_es; FAKE_CURL_FAIL=1
expect 7 "Elasticsearch unreachable" es "$a" "$b"
good_es
expect 2 "the same run id twice" es "$a" "$a"
expect 2 "the failed id repeats a published one" es "$a" "$b" "$a"
expect 2 "an unknown mode" other "$a" "$b"
expect 2 "missing arguments" es "$a"
expect 2 "too many arguments" es "$a" "$b" "$f" x

# left: the failed run's index answers and the alias is elsewhere (or nowhere yet).
good_es; FAKE_ALIASES=""; heads 404 200
expect 0 "the failed run left its index" left "$f"
FAKE_ALIASES="persons-$a"
expect 0 "the failed run left its index beside a served one" left "$f"
FAKE_ALIASES="persons-$f"
expect 1 "the alias on the failed run's index" left "$f"
FAKE_ALIASES=""; heads 404 404
expect 1 "the failed run left no index" left "$f"
expect 2 "left without an id" left
expect 2 "left with two ids" left "$f" "$a"

if command -v jq >/dev/null; then
  mkdir "$FAKE_SUMMARIES"
  # summary <run_id> <index> <publish object>
  summary() {
    printf '{"run_id":"%s","alias":"persons","index":"%s","publish":%s}\n' "$1" "$2" "$3" \
      >"$FAKE_SUMMARIES/$1.json"
  }
  good_second() {
    summary "$b" "persons-$b" "{\"docs\":1,\"previous\":[\"persons-$a\"],\"deleted\":[\"persons-$a\"],\"not_deleted\":[]}"
  }
  good_first() {
    summary "$a" "persons-$a" "{\"docs\":1,\"previous\":[],\"deleted\":[\"persons-$f\"],\"not_deleted\":[]}"
  }
  good_second
  expect 0 "a clean summary" summary "$a" "$b"
  # Each of the next three differs from the clean summary in one field, so it fails on it alone.
  summary "$b" "persons-$b" "{\"docs\":1,\"previous\":[\"persons-$a\"],\"deleted\":[\"persons-$a\"],\"not_deleted\":[[\"persons-old\",\"403\"]]}"
  expect 1 "an index not deleted" summary "$a" "$b"
  summary "$b" "persons-$b" "{\"docs\":1,\"previous\":[\"persons-$a\"],\"deleted\":[],\"not_deleted\":[]}"
  expect 1 "the first index not deleted" summary "$a" "$b"
  summary "$b" "persons-$b" "{\"docs\":1,\"previous\":[],\"deleted\":[\"persons-$a\"],\"not_deleted\":[]}"
  expect 1 "no previous index" summary "$a" "$b"
  summary "$b" "persons-$a" "{\"docs\":1,\"previous\":[\"persons-$a\"],\"deleted\":[\"persons-$a\"],\"not_deleted\":[]}"
  expect 1 "the wrong index" summary "$a" "$b"
  summary "$b" "persons-$b" "{\"docs\":1,\"previous\":[\"persons-$a\"]}"
  expect 1 "missing deleted and not_deleted" summary "$a" "$b"
  printf 'not json\n' >"$FAKE_SUMMARIES/$b.json"
  expect 1 "a malformed summary" summary "$a" "$b"
  rm "$FAKE_SUMMARIES/$b.json"
  expect 1 "no summary" summary "$a" "$b"

  # The first run's summary, with a failed run before it.
  good_second; good_first
  expect 0 "the failed run's index deleted by the first publish" summary "$a" "$b" "$f"
  summary "$a" "persons-$a" "{\"docs\":1,\"previous\":[],\"deleted\":[],\"not_deleted\":[]}"
  expect 1 "the failed run's index not deleted" summary "$a" "$b" "$f"
  summary "$a" "persons-$a" "{\"docs\":1,\"previous\":[\"persons-$f\"],\"deleted\":[\"persons-$f\"],\"not_deleted\":[]}"
  expect 1 "the failed run's index named as previous" summary "$a" "$b" "$f"
  summary "$a" "persons-$a" "{\"docs\":1,\"previous\":[],\"deleted\":[\"persons-$f\"],\"not_deleted\":[[\"persons-*\",\"403\"]]}"
  expect 1 "a listing not done" summary "$a" "$b" "$f"
  summary "$a" "persons-$b" "{\"docs\":1,\"previous\":[],\"deleted\":[\"persons-$f\"],\"not_deleted\":[]}"
  expect 1 "the first summary of the wrong index" summary "$a" "$b" "$f"
  rm "$FAKE_SUMMARIES/$a.json"
  expect 1 "no first summary" summary "$a" "$b" "$f"
  expect 0 "the first summary not read without a failed run" summary "$a" "$b"
else
  echo "test-check-republish: jq not installed; summary cases skipped"
fi
echo "test-check-republish: ok"
