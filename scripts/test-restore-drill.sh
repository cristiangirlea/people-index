#!/usr/bin/env bash
# Tests deploy/compose/scripts/restore-drill.sh against a stub docker and a stub curl backed by
# a small fake Elasticsearch (Python, state in a JSON file): the drill passes on a served index
# and leaves the cluster and the keystores as it found them, and its refusals and failures leave
# no keys behind: the alias on two indexes or none, an existing <index>-restored, a failed
# restore after the delete (the snapshot is kept), a reload that fails or reports a
# reload_exception, half a key pair either way round. Keys and a repository that were there
# before are neither overwritten nor removed, and every curl call has a timeout (-m).
set -euo pipefail
src="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
drill="$src/deploy/compose/scripts/restore-drill.sh"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
fail() { echo "test-restore-drill: $*" >&2; exit 1; }

py=""
for candidate in python3 python; do
  if command -v "$candidate" >/dev/null 2>&1 && "$candidate" -c 'import sys' >/dev/null 2>&1; then
    py="$(command -v "$candidate")"
    break
  fi
done
[[ -n "$py" ]] || fail "needs python3 or python for the fake Elasticsearch"
# Paths handed to a native Windows Python (Git Bash) must be Windows paths.
native() { if command -v cygpath >/dev/null 2>&1; then cygpath -m "$1"; else printf '%s' "$1"; fi; }

mkdir "$tmp/bin" "$tmp/ks"
cat >"$tmp/fake_es.py" <<'EOF'
"""A fake Elasticsearch for curl's arguments: the calls restore-drill.sh makes, nothing more."""
import json
import os
import re
import sys
from fnmatch import fnmatch
from urllib.parse import urlsplit

args = sys.argv[1:]
out, wfmt, method, data, url = None, "", "GET", None, None
i = 0
while i < len(args):
    a = args[i]
    if a in ("-o", "-w", "-X", "-d", "-H", "--cacert", "-m", "-K"):
        value = args[i + 1]
        if a == "-o":
            out = value
        elif a == "-w":
            wfmt = value
        elif a == "-X":
            method = value
        elif a == "-d":
            data = value
        i += 2
        continue
    if a.startswith("-"):
        i += 1
        continue
    url = a
    i += 1
if data is not None and method == "GET":
    method = "POST"
match = re.search(r'user = "([^:"]*):', sys.stdin.read())
user = match.group(1) if match else None

state_path = os.environ["FAKE_STATE"]
with open(state_path, encoding="utf-8") as f:
    st = json.load(f)
path = urlsplit(url).path
with open(state_path + ".log", "a", encoding="utf-8") as log:
    log.write(f"{method} {path} {user}\n")
alias = st["alias_name"]


def reply(status, body):
    if out:
        with open(out, "w", encoding="utf-8", newline="") as f:
            f.write(body if isinstance(body, str) else json.dumps(body, separators=(",", ":")))
    with open(state_path, "w", encoding="utf-8") as f:
        json.dump(st, f)
    sys.stdout.write(wfmt.replace("%{http_code}", str(status)))
    sys.exit(0)


def missing(name):
    reply(404, {"error": {"type": "index_not_found_exception", "index": name}, "status": 404})


def total():
    return sum(st["indices"][name] for name in st["alias"])


parts = [p for p in path.split("/") if p]
if user not in ("elastic", "reader"):
    reply(401, {"error": "unauthenticated"})
if user == "reader":
    if method == "POST" and parts == [alias, "_search"]:
        if not st["alias"]:
            missing(alias)
        reply(200, {"hits": {"total": {"value": total(), "relation": "eq"}}})
    reply(403, {"error": "forbidden"})

if path == "/":
    reply(200, {"version": {"number": "9.5.5"}})
if path == "/_nodes/reload_secure_settings" and method == "POST":
    st["reloads"] += 1
    nodes = {"n1": {"name": "es01"}}
    if os.environ.get("FAKE_RELOAD_EXCEPTION") == "1":
        nodes["n2"] = {"name": "es02", "reload_exception": {"type": "exception"}}
    failed = 1 if os.environ.get("FAKE_RELOAD_FAILED") == "1" else 0
    reply(200, {"_nodes": {"total": 3, "successful": 3 - failed, "failed": failed}, "nodes": nodes})
if parts == ["_alias", alias] and method == "GET":
    if not st["alias"]:
        reply(404, {"error": f"alias [{alias}] missing", "status": 404})
    reply(200, {name: {"aliases": {alias: {}}} for name in st["alias"]})
if parts == ["_aliases"] and method == "POST":
    for action in json.loads(data)["actions"]:
        (kind, spec), = action.items()
        if kind == "remove":
            st["alias"] = [n for n in st["alias"] if not fnmatch(n, spec["index"])]
        elif kind == "add":
            if spec["index"] not in st["indices"]:
                missing(spec["index"])
            st["alias"].append(spec["index"])
    reply(200, {"acknowledged": True})
if parts[:2] == ["_cluster", "health"]:
    if parts[2] not in st["indices"]:
        reply(408, {"timed_out": True})
    reply(200, {"status": "green", "timed_out": False})
if parts[:2] == ["_snapshot", "people-index"]:
    if len(parts) == 2:
        if method == "GET":
            reply(200 if st["repo"] else 404, {})
        if method == "PUT":
            st["repo"] = True
            reply(200, {"acknowledged": True})
        if method == "DELETE":
            if not st["repo"]:
                reply(404, {})
            st["repo"] = False
            reply(200, {"acknowledged": True})
    if not st["repo"]:
        reply(404, {"error": "repository_missing_exception"})
    snap = parts[2]
    if len(parts) == 3 and method == "PUT":
        index = json.loads(data)["indices"]
        st["snapshots"][snap] = {index: {"docs": st["indices"][index], "aliased": index in st["alias"]}}
        reply(200, {"snapshot": {"snapshot": snap, "state": "SUCCESS", "shards": {"failed": 0}}})
    if len(parts) == 3 and method == "DELETE":
        if snap not in st["snapshots"]:
            reply(404, {})
        del st["snapshots"][snap]
        reply(200, {"acknowledged": True})
    if len(parts) == 4 and parts[3] == "_restore" and method == "POST":
        if os.environ.get("FAKE_FAIL") == "restore":
            reply(500, {"error": "injected restore failure"})
        req = json.loads(data)
        for index, saved in st["snapshots"][snap].items():
            if index != req["indices"]:
                continue
            name = index
            if "rename_pattern" in req:
                name = re.sub(req["rename_pattern"], req["rename_replacement"].replace("$1", r"\1"), index)
            if name in st["indices"]:
                reply(500, {"error": f"cannot restore index [{name}]: an open index with the same name exists"})
            st["indices"][name] = saved["docs"]
            if req.get("include_aliases", True) and saved["aliased"]:
                st["alias"].append(name)
        reply(200, {"snapshot": {"snapshot": snap, "shards": {"total": 2, "failed": 0, "successful": 2}}})
if len(parts) == 1 and not parts[0].startswith("_"):
    name = parts[0]
    if name not in st["indices"]:
        missing(name)
    if method == "DELETE":
        del st["indices"][name]
        st["alias"] = [n for n in st["alias"] if n != name]
        reply(200, {"acknowledged": True})
    reply(200, {name: {}})
if len(parts) == 2 and parts[1] in ("_refresh", "_count"):
    if parts[0] not in st["indices"]:
        missing(parts[0])
    reply(200, {"count": st["indices"][parts[0]]} if parts[1] == "_count" else {"_shards": {"failed": 0}})
sys.stderr.write(f"fake es: unexpected call: {method} {url}\n")
reply(400, {"error": "unexpected call"})
EOF

# The drill runs with MSYS_NO_PATHCONV=1 (as under docker compose run), so on Git Bash the stub
# converts the output path for a native Python itself.
cat >"$tmp/bin/curl" <<'EOF'
#!/usr/bin/env bash
# One line per call (printf %q quotes a multi-line body), for the timeout check at the end.
{ printf '%q ' "$@"; echo; } >>"$FAKE_CURL_LOG"
args=()
while (($#)); do
  if [[ "$1" == -o ]] && command -v cygpath >/dev/null 2>&1; then
    args+=(-o "$(cygpath -m "$2")")
    shift 2
    continue
  fi
  args+=("$1")
  shift
done
exec "$FAKE_PY" "$FAKE_ES" "${args[@]}"
EOF

# docker compose: `ps` lists the services, `exec` on s3 answers its environment, `exec` on a node
# runs a keystore kept as one file per key under $FAKE_KS/<node>/, and `run ... es-users` runs
# the drill's in-container half here. Every keystore change and run is logged.
cat >"$tmp/bin/docker" <<'EOF'
#!/usr/bin/env bash
[[ "$1 $2" == "compose -f" ]] || { echo "stub docker: unexpected call: $*" >&2; exit 3; }
shift 3
log() { echo "$*" >>"$FAKE_STATE.docker.log"; }
case "$1" in
  ps) printf '%s\n' es01 es02 es03 s3 ;;
  exec)
    [[ "$2" == -T ]] || { echo "stub docker: exec without -T" >&2; exit 3; }
    service="$3"
    shift 3
    if [[ "$service" == s3 ]]; then
      case "$1" in
        sh) exit 0 ;;
        printenv) printf '%s\n' "value-of-$2" ;;
        *) echo "stub docker: unexpected s3 call: $*" >&2; exit 3 ;;
      esac
      exit 0
    fi
    [[ "$1" == bin/elasticsearch-keystore ]] || { echo "stub docker: unexpected exec: $*" >&2; exit 3; }
    dir="$FAKE_KS/$service"
    mkdir -p "$dir"
    case "$2" in
      list) echo keystore.seed; ls "$dir" ;;
      add)
        [[ "$3 $4" == "--stdin --force" ]] || { echo "stub docker: add without --stdin --force" >&2; exit 3; }
        IFS= read -r value
        printf '%s' "$value" >"$dir/$5"
        log "add $service $5" ;;
      remove)
        shift 2
        for key in "$@"; do
          [[ -f "$dir/$key" ]] || { echo "ERROR: Setting [$key] does not exist in the keystore." >&2; exit 65; }
          rm "$dir/$key"
          log "remove $service $key"
        done ;;
      *) echo "stub docker: unexpected keystore call: $*" >&2; exit 3 ;;
    esac ;;
  run)
    [[ "$*" == "run --rm --no-deps --entrypoint bash es-users /scripts/restore-drill.sh in-container "* ]] ||
      { echo "stub docker: unexpected run: $*" >&2; exit 3; }
    shift 7
    log "run $*"
    bash "$FAKE_DRILL" "$@" ;;
  *) echo "stub docker: unexpected call: $*" >&2; exit 3 ;;
esac
EOF
chmod +x "$tmp/bin/curl" "$tmp/bin/docker"

export PATH="$tmp/bin:$PATH" FAKE_PY="$py" FAKE_ES FAKE_STATE FAKE_KS="$tmp/ks" FAKE_DRILL="$drill" \
  FAKE_CURL_LOG="$tmp/curl.log" \
  ES_URL=https://es.invalid:9200 ELASTIC_PASSWORD=x READER_ES_PASSWORD=y PI_ALIAS=persons
FAKE_ES="$(native "$tmp/fake_es.py")"
FAKE_STATE="$(native "$tmp/state.json")"
run=20261020-100000
index="persons-$run"
other="persons-20261019-100000"
access=s3.client.default.access_key
secret=s3.client.default.secret_key

# reset [<python dict update>]: one served index of 20 documents, no repository, empty keystores.
reset() {
  unset FAKE_FAIL FAKE_RELOAD_EXCEPTION FAKE_RELOAD_FAILED
  rm -rf "$tmp/ks" "$tmp/state.json"*
  mkdir -p "$tmp/ks"
  "$py" - "$FAKE_STATE" "${1:-}" <<'EOF'
import json, sys
st = {"alias_name": "persons", "indices": {"persons-20261020-100000": 20},
      "alias": ["persons-20261020-100000"], "repo": False, "snapshots": {}, "reloads": 0}
if sys.argv[2]:
    exec(sys.argv[2])
with open(sys.argv[1], "w", encoding="utf-8") as f:
    json.dump(st, f)
EOF
  : >"$tmp/state.json.docker.log"
}
# q <python expression over st>: prints its value.
q() {
  "$py" - "$FAKE_STATE" "$1" <<'EOF'
import json, sys
with open(sys.argv[1], encoding="utf-8") as f:
    st = json.load(f)
print(eval(sys.argv[2]))
EOF
}
# expect <exit code> <description> <args...>: the drill's exit code must match.
expect() {
  local want="$1" what="$2" got=0
  shift 2
  bash "$drill" "$@" >"$tmp/out" 2>&1 || got=$?
  [[ "$got" == "$want" ]] || fail "$what: exit $got, expected $want: $(cat "$tmp/out")"
}
# no_keys_added: no node keeps a key the drill added, and the last thing it did was a reload.
no_keys_added() {
  local what="$1" left
  left="$(cd "$tmp/ks" && find . -type f -name 's3.client.*' | sort | tr '\n' ' ')"
  [[ -z "$left" ]] || fail "$what: keys left behind: $left"
  [[ "$(tail -n 1 "$tmp/state.json.docker.log")" == "run in-container reload" ]] ||
    fail "$what: the drill did not reload after removing the keys: $(cat "$tmp/state.json.docker.log")"
}
served_as_before() {
  [[ "$(q 'st["alias"]')" == "['$index']" && "$(q 'st["indices"]')" == "{'$index': 20}" ]] ||
    fail "$1: the cluster changed: alias $(q 'st["alias"]'), indexes $(q 'st["indices"]')"
}

reset
expect 0 "a served index" "$run"
served_as_before "a served index"
[[ "$(q 'st["snapshots"]')" == "{}" && "$(q 'st["repo"]')" == False ]] ||
  fail "a served index: snapshot or repository left: $(q 'st')"
no_keys_added "a served index"
grep -q "add es03 $secret" "$tmp/state.json.docker.log" || fail "a served index: es03 got no keys"
grep -q '^restore-drill: ok: ' "$tmp/out" || fail "a served index: no ok line: $(cat "$tmp/out")"
if grep -q 'value-of-' "$tmp/out"; then fail "a served index: a key reached the output"; fi
expect 0 "a served index, no run id"
reset
expect 1 "another run's index" 20261020-100001
no_keys_added "another run's index"

reset "st['indices']['$other'] = 5; st['alias'].append('$other')"
expect 1 "the alias on two indexes"
grep -q 'expected exactly one index' "$tmp/out" || fail "the alias on two indexes: $(cat "$tmp/out")"
no_keys_added "the alias on two indexes"

reset "st['alias'] = []"
expect 1 "the alias on no index"
no_keys_added "the alias on no index"

reset "st['indices']['$index-restored'] = 20"
expect 1 "an existing $index-restored"
grep -q "$index-restored exists" "$tmp/out" || fail "an existing copy: $(cat "$tmp/out")"
[[ "$(q 'st["indices"]')" == "{'$index': 20, '$index-restored': 20}" ]] || fail "an existing copy: indexes changed"
no_keys_added "an existing $index-restored"

reset
export FAKE_FAIL=restore
expect 1 "a restore that fails after the delete"
[[ "$(q 'len(st["snapshots"])')" == 1 && "$(q 'st["repo"]')" == True ]] ||
  fail "a failed restore: the snapshot or the repository was removed: $(q 'st')"
grep -q 'kept snapshot people-index/drill-' "$tmp/out" || fail "a failed restore: no kept-snapshot message"
grep -q 'put the keys back first' "$tmp/out" || fail "a failed restore: no pointer to the keystore step"
no_keys_added "a restore that fails after the delete"

reset
export FAKE_RELOAD_EXCEPTION=1
expect 1 "a node that reports a reload_exception"
grep -q 'could not reload' "$tmp/out" || fail "a reload_exception: $(cat "$tmp/out")"
[[ "$(q 'st["repo"]')" == False ]] || fail "a reload_exception: the drill went on"
no_keys_added "a node that reports a reload_exception"

reset
export FAKE_RELOAD_FAILED=1
expect 1 "a reload that fails on a node"
grep -q 'reload_secure_settings failed on a node' "$tmp/out" || fail "a failed reload: $(cat "$tmp/out")"
[[ "$(q 'st["repo"]')" == False ]] || fail "a failed reload: the drill went on"
no_keys_added "a reload that fails on a node"

reset "st['repo'] = True"
expect 0 "an existing repository"
[[ "$(q 'st["repo"]')" == True && "$(q 'st["snapshots"]')" == "{}" ]] ||
  fail "an existing repository: removed, or the drill's snapshot left: $(q 'st')"
served_as_before "an existing repository"

reset
mkdir -p "$tmp/ks/es02"
printf theirs >"$tmp/ks/es02/$access"
printf theirs >"$tmp/ks/es02/$secret"
expect 0 "keys already on es02"
[[ "$(cat "$tmp/ks/es02/$access")$(cat "$tmp/ks/es02/$secret")" == theirstheirs ]] ||
  fail "keys already on es02: overwritten or removed"
if grep -q ' es02 ' "$tmp/state.json.docker.log"; then
  fail "keys already on es02: the drill touched them: $(cat "$tmp/state.json.docker.log")"
fi
[[ -z "$(find "$tmp/ks/es01" "$tmp/ks/es03" -type f)" ]] || fail "keys already on es02: keys left on es01 or es03"

reset
mkdir -p "$tmp/ks/es02"
printf theirs >"$tmp/ks/es02/$access"
expect 1 "half a key pair on es02"
[[ "$(cat "$tmp/ks/es02/$access")" == theirs && ! -f "$tmp/ks/es02/$secret" ]] ||
  fail "half a key pair: es02's keystore changed"
[[ -z "$(find "$tmp/ks/es01" -type f)" ]] || fail "half a key pair: keys left on es01"
served_as_before "half a key pair on es02"

reset
mkdir -p "$tmp/ks/es02"
printf theirs >"$tmp/ks/es02/$secret"
expect 1 "only the secret key on es02"
[[ "$(cat "$tmp/ks/es02/$secret")" == theirs && ! -f "$tmp/ks/es02/$access" ]] ||
  fail "only the secret key: es02's keystore changed"
[[ -z "$(find "$tmp/ks/es01" -type f)" ]] || fail "only the secret key: keys left on es01"
grep -q 'has only some of' "$tmp/out" || fail "only the secret key: $(cat "$tmp/out")"

expect 2 "two arguments" "$run" extra

# Every request the drill made, in every case above, had a timeout.
[[ -s "$tmp/curl.log" ]] || fail "no curl call was recorded"
if grep -v -E '(^| )-m [0-9]' "$tmp/curl.log" >"$tmp/no-timeout"; then
  fail "curl calls without -m: $(cat "$tmp/no-timeout")"
fi
echo "test-restore-drill: ok"
