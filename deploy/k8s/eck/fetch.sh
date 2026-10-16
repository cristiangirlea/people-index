#!/usr/bin/env bash
# Downloads the ECK operator release files (crds.yaml, operator.yaml) into a directory and
# checks them against SHA256SUMS beside this script. Only verified files ever end up in that
# directory, so whatever applies them (make eck) applies exactly what SHA256SUMS pins, and
# scripts/check-deploy.sh validates the same files.
#
#   deploy/k8s/eck/fetch.sh <dir>        # e.g. target/eck
#
# Files already in <dir> are reused when they still match; otherwise both are downloaded
# again into a staging directory, checked with `sha256sum -c`, and moved into <dir> only when
# every checksum matches. On a mismatch nothing is moved and the script exits 1.
#
# Upgrading: change `version` below, refresh SHA256SUMS (`sha256sum crds.yaml operator.yaml`
# on the new release files), and read the ECK release notes for the supported Elasticsearch
# versions.
set -euo pipefail

version="3.5.0"
base_url="https://download.elastic.co/downloads/eck/$version"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
sums="$here/SHA256SUMS"

[[ $# -eq 1 && -n "$1" ]] || {
  echo "usage: $0 <dir>" >&2
  exit 2
}
out="$1"
[[ -f "$sums" ]] || {
  echo "eck: no $sums" >&2
  exit 2
}

# sha256sum -c against SHA256SUMS, run inside the directory that holds the files.
check() {
  if command -v sha256sum >/dev/null 2>&1; then
    (cd "$1" && sha256sum --quiet -c "$sums")
  else
    (cd "$1" && shasum -a 256 --quiet -c "$sums")
  fi
}

mapfile -t files < <(awk 'NF { print $2 }' "$sums")
[[ ${#files[@]} -gt 0 ]] || {
  echo "eck: $sums lists no files" >&2
  exit 2
}
for name in "${files[@]}"; do
  [[ "$name" =~ ^[a-z0-9.-]+\.yaml$ ]] || {
    echo "eck: unexpected file name in $sums: $name" >&2
    exit 2
  }
done

mkdir -p "$out"
cached=yes
for name in "${files[@]}"; do [[ -f "$out/$name" ]] || cached=no; done
if [[ "$cached" == yes ]] && check "$out" >/dev/null 2>&1; then
  echo "eck: $version files in $out match SHA256SUMS" >&2
  exit 0
fi
# Whatever is there no longer matches (or is incomplete): drop it before downloading.
for name in "${files[@]}"; do rm -f "$out/$name"; done

staging="$(mktemp -d "$out/.fetch.XXXXXX")"
trap 'rm -rf "$staging"' EXIT
for name in "${files[@]}"; do
  echo "eck: downloading $base_url/$name" >&2
  curl -fsSL --retry 3 -m 120 -o "$staging/$name" "$base_url/$name"
done
if ! check "$staging"; then
  echo "eck: the downloaded ECK $version files do not match $sums; nothing was kept" >&2
  exit 1
fi
for name in "${files[@]}"; do mv -f "$staging/$name" "$out/$name"; done
echo "eck: $version files in $out match SHA256SUMS" >&2
