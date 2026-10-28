"""Fails when a rendered Elasticsearch resource sets an S3 client endpoint without its scheme
(http:// or https://) or an s3.client.*.protocol key, which Elasticsearch 9 deprecates.

Usage: check-s3-endpoints.py <rendered.yaml>   (scripts/check-deploy.sh, every overlay)
"""
import sys

import yaml


def flat(prefix, value):
    """Yields (dotted key, value) for a nested config map; ECK accepts both forms."""
    if isinstance(value, dict):
        for key, inner in value.items():
            yield from flat(f"{prefix}.{key}" if prefix else str(key), inner)
    else:
        yield prefix, value


def problems(docs):
    bad = []
    for doc in docs:
        if not doc or doc.get("kind") != "Elasticsearch":
            continue
        for node_set in doc.get("spec", {}).get("nodeSets", []):
            for key, value in flat("", node_set.get("config") or {}):
                parts = key.split(".")
                if len(parts) != 4 or parts[:2] != ["s3", "client"]:
                    continue
                if parts[3] == "protocol":
                    bad.append(f"{node_set.get('name')}: {key} is deprecated; put the scheme in the endpoint")
                elif parts[3] == "endpoint" and not str(value).startswith(("http://", "https://")):
                    bad.append(f"{node_set.get('name')}: {key} {value!r} has no http:// or https:// scheme")
    return bad


def main():
    with open(sys.argv[1], encoding="utf-8") as source:
        bad = problems(list(yaml.safe_load_all(source)))
    for line in bad:
        print(f"check-s3-endpoints: {line}", file=sys.stderr)
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
