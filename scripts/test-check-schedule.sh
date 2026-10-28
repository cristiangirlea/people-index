#!/usr/bin/env bash
# Exercise the same comparison used by check-deploy with differing pod fields.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if command -v python3 >/dev/null 2>&1; then python=python3; else python=python; fi
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
"$python" - "$work" <<'PY'
import pathlib, sys, yaml
p = pathlib.Path(sys.argv[1])
pod = {"securityContext": {"runAsNonRoot": True}, "containers": [{"securityContext": {"readOnlyRootFilesystem": True}, "env": [{"name": "COMMON"}, {"name": "PI_RUN_ID"}], "volumeMounts": []}], "volumes": []}
run = {"kind": "Job", "metadata": {"name": "pi-run-test"}, "spec": {"template": {"spec": pod}}}
p.joinpath("run.yaml").write_text(yaml.safe_dump(run))
pod["containers"][0]["env"][-1]["name"] = "PI_RUN_ID_FROM_JOB_NAME"
scheduled = {"kind": "CronJob", "metadata": {"name": "pi-schedule"}, "spec": {"jobTemplate": {"spec": {"template": {"spec": pod}}}}}
p.joinpath("good.yaml").write_text(yaml.safe_dump(scheduled))
pod["securityContext"]["runAsNonRoot"] = False
p.joinpath("bad-security.yaml").write_text(yaml.safe_dump(scheduled))
pod["securityContext"]["runAsNonRoot"] = True
pod["containers"][0]["env"][0]["name"] = "DIFFERENT"
p.joinpath("bad-env.yaml").write_text(yaml.safe_dump(scheduled))
PY
"$python" "$root/scripts/check-schedule-pod.py" "$work/run.yaml" "$work/good.yaml"
for bad in "$work"/bad-*.yaml; do
  if "$python" "$root/scripts/check-schedule-pod.py" "$work/run.yaml" "$bad" 2>/dev/null; then
    echo "schedule comparison accepted $bad" >&2
    exit 1
  fi
done
echo "test-check-schedule: ok"
