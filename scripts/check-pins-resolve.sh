#!/usr/bin/env bash
# Fails when a pinned image no longer resolves in its registry. A pin (name:tag@sha256:<digest>)
# guards against a tag that changes, not against a registry deleting the pinned manifest, which
# some projects do when they re-push a tag; a deploy then fails to pull. check-deploy.sh lists
# every pinned reference (Dockerfile, compose, k3d, CI's docker run lines, ECK, every rendered
# kustomization and Job template); this asks the registry for each digest.
#
#   check-pins-resolve.sh            the references check-deploy.sh finds
#   check-pins-resolve.sh <file>     the references in <file>, one per line
#
# Needs docker with buildx (`docker buildx imagetools inspect`); runs in CI's weekly schedule.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

if (($# == 1)); then
  refs="$1"
else
  PINNED_REFS_OUT="$tmp/refs" bash "$root/scripts/check-deploy.sh" >/dev/null
  refs="$tmp/refs"
fi
mapfile -t pins < <(sort -u "$refs")
((${#pins[@]} > 0)) || { echo "check-pins-resolve: no pinned references found" >&2; exit 1; }

missing=()
for ref in "${pins[@]}"; do
  # Up to three tries, so a registry hiccup is not reported as a vanished image.
  for try in 1 2 3; do
    if docker buildx imagetools inspect "$ref" >/dev/null 2>"$tmp/err"; then
      continue 2
    fi
    ((try < 3)) && sleep 5
  done
  missing+=("$ref: $(tr '\n' ' ' <"$tmp/err")")
done
if ((${#missing[@]} > 0)); then
  printf 'check-pins-resolve: does not resolve: %s\n' "${missing[@]}" >&2
  echo "check-pins-resolve: bump the pin (docker buildx imagetools inspect <name>:<tag>)" >&2
  exit 1
fi
echo "check-pins-resolve: ${#pins[@]} pinned images resolve"
