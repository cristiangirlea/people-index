#!/usr/bin/env bash
# Exercise the generator without writing to the host's /config.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
python="$(command -v python3 || command -v python)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
sed "s|/config/s3.json|$work/s3.json|" "$root/deploy/k8s/base/s3/identity-config.sh" >"$work/config.sh"
export PIPELINE_KEY=pipe PIPELINE_SECRET=secret SNAPSHOT_KEY=snap SNAPSHOT_SECRET=secret
export PIPELINE_BUCKET=pipe-bucket SNAPSHOT_BUCKET=snap-bucket
sh "$work/config.sh"
cat >"$work/check.py" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as f:
    identities = json.load(f)["identities"]
expected = {"pipeline": "pipe-bucket", "snapshots": "snap-bucket"}
assert len(identities) == len(expected)
for identity in identities:
    bucket = expected.pop(identity["name"])
    assert set(identity["actions"]) == {f"{action}:{bucket}" for action in ("Admin", "Read", "Write", "List")}
assert not expected
PY
"$python" "$work/check.py" "$work/s3.json"
case "$(uname -s)" in
  MINGW* | MSYS* | CYGWIN*) ;;
  *) [[ "$(stat -c %a "$work/s3.json" 2>/dev/null || stat -f %Lp "$work/s3.json")" == 600 ]] || { echo "wrong output mode" >&2; exit 1; } ;;
esac
# The checker must reject permissions assigned to the other identity's bucket.
"$python" - "$work/s3.json" "$work/swapped.json" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as f:
    config = json.load(f)
a, b = config["identities"]
a["actions"], b["actions"] = b["actions"], a["actions"]
with open(sys.argv[2], "w", encoding="utf-8") as f:
    json.dump(config, f)
PY
reject() { if "$@" >/dev/null 2>&1; then echo "accepted invalid input" >&2; exit 1; fi; }
reject "$python" "$work/check.py" "$work/swapped.json"
reject env PIPELINE_BUCKET='bad"b' sh "$work/config.sh"
reject env SNAPSHOT_BUCKET=pipe-bucket sh "$work/config.sh"
reject env PIPELINE_SECRET='bad"key' sh "$work/config.sh"
reject env SNAPSHOT_KEY=pipe sh "$work/config.sh"
reject env -u SNAPSHOT_BUCKET sh "$work/config.sh"
echo "test-identity-config: ok"
