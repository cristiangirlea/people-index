#!/usr/bin/env bash
# Tests deploy/compose/scripts/check-republish.sh against stub curl and aws commands on PATH:
# it passes on the state a good second publish leaves and fails on every other one (alias on
# both indexes, the first index left, a summary with not_deleted or the wrong previous/deleted,
# unreadable or malformed input). The summary cases need jq, as the s3-init image has; without
# it they are skipped.
set -euo pipefail
src="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
check="$src/deploy/compose/scripts/check-republish.sh"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
fail() { echo "test-check-republish: $*" >&2; exit 1; }

# curl: _cat/aliases answers FAKE_ALIASES, _cat/indices answers FAKE_INDICES, the HEAD on
# FAKE_HEAD_URL (a -w request) answers FAKE_STATUS; FAKE_CURL_FAIL=1 fails like a refused
# connection. Any other call is an error, so a check aimed at the wrong URL fails.
mkdir "$tmp/bin"
cat >"$tmp/bin/curl" <<'EOF'
#!/usr/bin/env bash
[[ "${FAKE_CURL_FAIL:-0}" == 1 ]] && { echo "curl: (7) Failed to connect" >&2; exit 7; }
url="${*: -1}"
case " $* " in
  *" -w "*) [[ "$url" == "$FAKE_HEAD_URL" ]] && { printf '%s' "$FAKE_STATUS"; exit 0; } ;;
  *) case "$url" in
       "$ES_URL/_cat/aliases/persons?h=index") printf '%s\n' "$FAKE_ALIASES"; exit 0 ;;
       "$ES_URL/_cat/indices/persons-*?h=index&expand_wildcards=all") printf '%s\n' "$FAKE_INDICES"; exit 0 ;;
     esac ;;
esac
echo "stub curl: unexpected call: $*" >&2
exit 3
EOF
# aws: `s3 cp FAKE_KEY -` prints FAKE_SUMMARY's file, or fails when it does not exist; any other
# call is an error.
cat >"$tmp/bin/aws" <<'EOF'
#!/usr/bin/env bash
[[ "$*" == "--endpoint-url $S3_ENDPOINT s3 cp $FAKE_KEY -" ]] || { echo "stub aws: unexpected call: $*" >&2; exit 3; }
[[ -f "${FAKE_SUMMARY:-}" ]] || { echo "fatal error: Not Found" >&2; exit 1; }
cat "$FAKE_SUMMARY"
EOF
chmod +x "$tmp/bin/curl" "$tmp/bin/aws"
export PATH="$tmp/bin:$PATH" ES_URL=https://es.invalid:9200 ELASTIC_PASSWORD=x \
  S3_ENDPOINT=http://s3.invalid S3_BUCKET=b PI_ALIAS=persons
a=20261020-100000
b=20261020-100000-2
export FAKE_HEAD_URL="$ES_URL/persons-$a" FAKE_KEY="s3://b/runs/$b/summary.json"

# expect <exit code> <description> <args...>: the check's exit code must match.
expect() {
  local want="$1" what="$2" got=0
  shift 2
  bash "$check" "$@" >"$tmp/out" 2>&1 || got=$?
  [[ "$got" == "$want" ]] || fail "$what: exit $got, expected $want: $(cat "$tmp/out")"
}

good_es() { export FAKE_ALIASES="persons-$b" FAKE_INDICES="persons-$b" FAKE_STATUS=404 FAKE_CURL_FAIL=0; }

good_es
expect 0 "a clean second publish" es "$a" "$b"
good_es; FAKE_ALIASES="persons-$a"$'\n'"persons-$b"
expect 1 "the alias on both indexes" es "$a" "$b"
good_es; FAKE_ALIASES=""
expect 1 "no alias" es "$a" "$b"
good_es; FAKE_INDICES="persons-$a"$'\n'"persons-$b"
expect 1 "the first index left" es "$a" "$b"
good_es; FAKE_STATUS=200
expect 1 "the first index still answers" es "$a" "$b"
good_es; FAKE_CURL_FAIL=1
expect 7 "Elasticsearch unreachable" es "$a" "$b"
good_es
expect 2 "the same run id twice" es "$a" "$a"
expect 2 "an unknown mode" other "$a" "$b"
expect 2 "missing arguments" es "$a"

if command -v jq >/dev/null; then
  summary() {
    printf '{"run_id":"%s","alias":"persons","index":"%s","publish":%s}\n' "$b" "$1" "$2" \
      >"$tmp/summary.json"
  }
  export FAKE_SUMMARY="$tmp/summary.json"
  summary "persons-$b" "{\"docs\":1,\"previous\":[\"persons-$a\"],\"deleted\":[\"persons-$a\"],\"not_deleted\":[]}"
  expect 0 "a clean summary" summary "$a" "$b"
  # Each of the next three differs from the clean summary in one field, so it fails on it alone.
  summary "persons-$b" "{\"docs\":1,\"previous\":[\"persons-$a\"],\"deleted\":[\"persons-$a\"],\"not_deleted\":[[\"persons-old\",\"403\"]]}"
  expect 1 "an index not deleted" summary "$a" "$b"
  summary "persons-$b" "{\"docs\":1,\"previous\":[\"persons-$a\"],\"deleted\":[],\"not_deleted\":[]}"
  expect 1 "the first index not deleted" summary "$a" "$b"
  summary "persons-$b" "{\"docs\":1,\"previous\":[],\"deleted\":[\"persons-$a\"],\"not_deleted\":[]}"
  expect 1 "no previous index" summary "$a" "$b"
  summary "persons-$a" "{\"docs\":1,\"previous\":[\"persons-$a\"],\"deleted\":[\"persons-$a\"],\"not_deleted\":[]}"
  expect 1 "the wrong index" summary "$a" "$b"
  summary "persons-$b" "{\"docs\":1,\"previous\":[\"persons-$a\"]}"
  expect 1 "missing deleted and not_deleted" summary "$a" "$b"
  printf 'not json\n' >"$tmp/summary.json"
  expect 1 "a malformed summary" summary "$a" "$b"
  rm "$tmp/summary.json"
  expect 1 "no summary" summary "$a" "$b"
else
  echo "test-check-republish: jq not installed; summary cases skipped"
fi
echo "test-check-republish: ok"
