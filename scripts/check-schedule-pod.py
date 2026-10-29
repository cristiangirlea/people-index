#!/usr/bin/env python3
import sys
import yaml

def docs(path):
    with open(path, encoding="utf-8") as f:
        return list(yaml.safe_load_all(f))

run = next(d for d in docs(sys.argv[1]) if d and d.get("kind") == "Job" and d["metadata"]["name"].startswith("pi-run-"))["spec"]["template"]["spec"]
scheduled = next(d for d in docs(sys.argv[2]) if d and d.get("kind") == "CronJob" and d["metadata"]["name"] == "pi-schedule")["spec"]["jobTemplate"]["spec"]["template"]["spec"]
a, b = run["containers"][0], scheduled["containers"][0]
assert run["securityContext"] == scheduled["securityContext"]
assert a["securityContext"] == b["securityContext"]
assert {e["name"]: e for e in a["env"] if e["name"] != "PI_RUN_ID"} == {e["name"]: e for e in b["env"] if e["name"] != "PI_RUN_ID_FROM_JOB_NAME"}
assert run["volumes"] == scheduled["volumes"]
assert a["volumeMounts"] == b["volumeMounts"]
