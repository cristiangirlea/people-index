#!/usr/bin/env bash
# Tests scripts/check-s3-endpoints.py (check-deploy's S3 endpoint rule) on rendered documents:
# endpoints with a scheme pass, in flat or nested config; a bare endpoint or a protocol key in
# any node set fails; other kinds are not read. Needs PyYAML, like check-deploy.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if command -v python3 >/dev/null 2>&1 && python3 -c 'import yaml' 2>/dev/null; then python=python3; else python=python; fi
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
"$python" - "$work" <<'PY'
import copy, pathlib, sys, yaml
p = pathlib.Path(sys.argv[1])
def es(*configs):
    return {"apiVersion": "elasticsearch.k8s.elastic.co/v1", "kind": "Elasticsearch",
            "metadata": {"name": "es"},
            "spec": {"nodeSets": [{"name": f"set{i}", "count": 1, "config": c} for i, c in enumerate(configs)]}}
good = {"s3.client.default.endpoint": "http://s3:8333", "s3.client.default.path_style_access": True}
other = {"kind": "ConfigMap", "metadata": {"name": "c"}, "data": {"s3.client.default.endpoint": "s3:8333"}}
def write(name, *docs):
    p.joinpath(name).write_text(yaml.safe_dump_all(list(docs)))
write("good-flat.yaml", es(good, {"s3.client.default.endpoint": "https://s3.eu-west-1.amazonaws.com"}), other)
write("good-nested.yaml", es({"s3": {"client": {"default": {"endpoint": "https://s3.example"}}}}))
write("good-none.yaml", es({"node.store.allow_mmap": False}))
bare = copy.deepcopy(good); bare["s3.client.default.endpoint"] = "s3:8333"
write("bad-bare.yaml", es(good, bare))
protocol = copy.deepcopy(good); protocol["s3.client.default.protocol"] = "http"
write("bad-protocol.yaml", es(protocol, good))
write("bad-nested.yaml", es({"s3": {"client": {"backup": {"endpoint": "s3.example", "protocol": "https"}}}}))
PY
for doc in "$work"/good-*.yaml; do
  "$python" "$root/scripts/check-s3-endpoints.py" "$doc" ||
    { echo "test-check-s3-endpoints: refused ${doc##*/}" >&2; exit 1; }
done
for doc in "$work"/bad-*.yaml; do
  if "$python" "$root/scripts/check-s3-endpoints.py" "$doc" 2>/dev/null; then
    echo "test-check-s3-endpoints: accepted ${doc##*/}" >&2
    exit 1
  fi
done
echo "test-check-s3-endpoints: ok"
