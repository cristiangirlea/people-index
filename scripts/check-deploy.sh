#!/usr/bin/env bash
# Validates the deployment without a cluster:
#   - the ECK release files, fetched by deploy/k8s/eck/fetch.sh (what `make eck` applies),
#     still match deploy/k8s/eck/SHA256SUMS, and fetch.sh refuses files that do not;
#   - those ECK files, base and every overlay (rendered with kustomize) pass kubeconform
#     -strict against the Kubernetes schemas and, for custom resources (ECK,
#     prometheus-operator), the datreeio CRDs-catalog schemas;
#   - the run, datagen and verify Job templates render for every overlay and pass the same
#     check;
#   - deploy/k3d/cluster.yaml passes k3d's own config schema;
#   - the shell scripts parse (and pass shellcheck when it is installed);
#   - deploy/compose/compose.yaml, when present, passes `docker compose config -q`.
#
# Overlays are rendered from a temporary copy in which missing credential env files are filled
# from their *.env.example, so the check never needs (or writes) real secrets.
#
# kubeconform v0.8.0 is used from PATH, or downloaded once to
# ${XDG_CACHE_HOME:-$HOME/.cache}/people-index/ and checked against its pinned SHA-256.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cache="${XDG_CACHE_HOME:-$HOME/.cache}/people-index"
kubeconform_version="v0.8.0"
# Kubernetes version of the schemas: the k3s release deploy/k3d/cluster.yaml pins.
kubernetes_version="1.35.0"
k3d_schema_url="https://raw.githubusercontent.com/k3d-io/k3d/v5.9.0/pkg/config/v1alpha5/schema.json"
crd_catalog='https://raw.githubusercontent.com/datreeio/CRDs-catalog/main/{{.Group}}/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json'
# Kinds rendered here that have no schema anywhere, passed to -skip (by kind, not
# -ignore-missing-schemas, so a new kind without a schema still fails). Only
# CustomResourceDefinition: kubernetes-json-schema does not publish it (its OpenAPI schema is
# recursive) and the catalog only holds the custom resources themselves. The ECK CRDs are
# pinned by SHA256SUMS instead. Every custom resource used (Elasticsearch, ServiceMonitor,
# PodMonitor, PrometheusRule) is validated against its catalog schema.
skip_kinds="CustomResourceDefinition"

fail() {
  echo "check-deploy: $*" >&2
  exit 1
}
step() { echo "check-deploy: $*"; }

sha256() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | cut -d' ' -f1
  else
    shasum -a 256 "$1" | cut -d' ' -f1
  fi
}

kustomize_build() {
  if command -v kubectl >/dev/null 2>&1; then
    kubectl kustomize "$1"
  elif command -v kustomize >/dev/null 2>&1; then
    kustomize build "$1"
  else
    fail "needs kubectl (or kustomize) on PATH"
  fi
}

# kubeconform from PATH, or the pinned release, verified, from the cache.
find_kubeconform() {
  if command -v kubeconform >/dev/null 2>&1; then
    command -v kubeconform
    return
  fi
  local os arch ext asset want bin dir
  case "$(uname -s)" in
    Linux) os=linux ;;
    Darwin) os=darwin ;;
    MINGW* | MSYS* | CYGWIN*) os=windows ;;
    *) fail "no kubeconform build for $(uname -s); install kubeconform $kubeconform_version" ;;
  esac
  case "$(uname -m)" in
    x86_64 | amd64) arch=amd64 ;;
    aarch64 | arm64) arch=arm64 ;;
    *) fail "no kubeconform build for $(uname -m); install kubeconform $kubeconform_version" ;;
  esac
  ext=tar.gz
  [[ "$os" == windows ]] && ext=zip
  asset="kubeconform-$os-$arch.$ext"
  # From the release's CHECKSUMS file.
  case "$asset" in
    kubeconform-linux-amd64.tar.gz) want=9bc2bffbf71f261128533edaf912153948b7ff238f9a531ae6d34466ec287883 ;;
    kubeconform-linux-arm64.tar.gz) want=1f53fc8e81258197a35e8603054162a5af1de8c5af13746c71ab680d9534ed87 ;;
    kubeconform-darwin-amd64.tar.gz) want=71dbc87ac9f24099a62b93570e65aa06312ba6ac8aea63b7f86e9d999edf5a92 ;;
    kubeconform-darwin-arm64.tar.gz) want=f84f4dfbebf4a6b0b230385fa065a39ea35e02608c2b50d025dcf64775a69d67 ;;
    kubeconform-windows-amd64.zip) want=e3f56102bcf4f50b034a567e2482a1c5330799983ddd655952310211aef73d93 ;;
    kubeconform-windows-arm64.zip) want=4f3c9889f5f3a1e4aba84f9212f599ad3164d1fb32175fba3a53b505b0fffd0f ;;
    *) fail "no pinned checksum for $asset; install kubeconform $kubeconform_version" ;;
  esac
  dir="$cache/kubeconform-$kubeconform_version-$os-$arch"
  bin="$dir/kubeconform"
  [[ "$os" == windows ]] && bin="$bin.exe"
  if [[ ! -x "$bin" ]]; then
    mkdir -p "$dir"
    step "downloading kubeconform $kubeconform_version ($asset)" >&2
    curl -fsSL --retry 3 -m 120 -o "$dir/$asset" \
      "https://github.com/yannh/kubeconform/releases/download/$kubeconform_version/$asset"
    [[ "$(sha256 "$dir/$asset")" == "$want" ]] || {
      rm -f "$dir/$asset"
      fail "checksum mismatch for $asset"
    }
    if [[ "$ext" == zip ]]; then
      unzip -oq "$dir/$asset" -d "$dir"
    else
      tar -xzf "$dir/$asset" -C "$dir" kubeconform
    fi
    rm -f "$dir/$asset"
  fi
  echo "$bin"
}

kubeconform="$(find_kubeconform)"
validate() {
  local args=(-strict -summary -kubernetes-version "$kubernetes_version"
    -cache "$cache/schemas" -schema-location default -schema-location "$crd_catalog")
  [[ -n "$skip_kinds" ]] && args+=(-skip "$skip_kinds")
  mkdir -p "$cache/schemas"
  "$kubeconform" "${args[@]}" -
}

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

# 1. The ECK release files are what SHA256SUMS says, fetched the way `make eck` fetches them;
# and fetch.sh keeps nothing when a checksum does not match: a copy of it with one checksum
# changed, run on a directory that already holds the files (the reuse path `make eck` takes
# when target/eck exists), must exit 1 (a mismatch, not a download error) and leave no file.
eck_dir="$cache/eck"
bash "$root/deploy/k8s/eck/fetch.sh" "$eck_dir" || fail "ECK release files do not match deploy/k8s/eck/SHA256SUMS"
step "ECK release files match SHA256SUMS"
mkdir -p "$work/eck-bad"
cp "$root/deploy/k8s/eck/fetch.sh" "$work/eck-bad/"
awk 'NR == 1 { c = substr($0, 1, 1); $0 = (c == "0" ? "1" : "0") substr($0, 2) } { print }' \
  "$root/deploy/k8s/eck/SHA256SUMS" >"$work/eck-bad/SHA256SUMS"
mkdir -p "$work/eck-bad/out"
cp "$eck_dir"/*.yaml "$work/eck-bad/out/"
rc=0
bash "$work/eck-bad/fetch.sh" "$work/eck-bad/out" >/dev/null 2>&1 || rc=$?
[[ $rc -eq 1 ]] ||
  fail "deploy/k8s/eck/fetch.sh exited $rc on a file that does not match SHA256SUMS (want 1)"
[[ -z "$(find "$work/eck-bad/out" -name '*.yaml' 2>/dev/null)" ]] ||
  fail "deploy/k8s/eck/fetch.sh left unverified files behind"
step "fetch.sh refuses a checksum mismatch"

# 2. A copy of deploy/k8s with example credentials in place of the git-ignored env files.
cp -R "$root/deploy/k8s" "$work/k8s"
while IFS= read -r -d '' example; do
  [[ -e "${example%.example}" ]] || cp "$example" "${example%.example}"
done < <(find "$work/k8s" -name '*.env.example' -print0)

# 3. The verified ECK files, and every kustomization, render and validate.
step "eck (crds.yaml, operator.yaml)"
{
  cat "$eck_dir/crds.yaml"
  printf '
---
'
  cat "$eck_dir/operator.yaml"
} | validate || fail "the ECK files do not validate"
overlays=()
for dir in "$work"/k8s/overlays/*/; do
  dir="${dir%/}"
  overlays+=("overlays/${dir##*/}")
done
[[ -d "${work}/k8s/${overlays[0]:-none}" ]] || fail "no overlays under deploy/k8s/overlays"
for dir in base "${overlays[@]}"; do
  [[ -f "$work/k8s/$dir/kustomization.yaml" ]] || fail "deploy/k8s/$dir has no kustomization.yaml"
  step "kustomize $dir"
  kustomize_build "$work/k8s/$dir" >"$work/rendered.yaml" || fail "kustomize failed for deploy/k8s/$dir"
  validate <"$work/rendered.yaml" || fail "deploy/k8s/$dir does not validate"
done

# 4. The Job templates render for every overlay and validate.
for vars in "$work"/k8s/overlays/*/jobs.vars; do
  overlay="$(basename "$(dirname "$vars")")"
  for kind in run datagen verify; do
    step "jobs/$kind for $overlay"
    RUN_ID=check JOB_ID=check bash "$work/k8s/jobs/render.sh" "$kind" --overlay "$overlay" 2>/dev/null |
      validate || fail "jobs/$kind does not render or validate for $overlay"
  done
done

# 5. The k3d cluster config against k3d's schema (kind Simple, apiVersion k3d.io/v1alpha5).
mkdir -p "$cache/k3d"
if [[ ! -s "$cache/k3d/simple_v1alpha5.json" ]]; then
  curl -fsSL --retry 3 -m 60 -o "$cache/k3d/simple_v1alpha5.json" "$k3d_schema_url"
fi
step "deploy/k3d/cluster.yaml"
# A relative schema path, run from the schema directory: kubeconform on Windows reads an
# absolute `C:/...` location as a URL and finds nothing.
(cd "$cache/k3d" && "$kubeconform" -strict -summary \
  -schema-location '{{.ResourceKind}}_{{.ResourceAPIVersion}}.json' -) \
  <"$root/deploy/k3d/cluster.yaml" || fail "deploy/k3d/cluster.yaml does not validate"

# 6. Shell scripts parse; shellcheck when available.
scripts=(
  "$root/scripts/check-deploy.sh"
  "$root/deploy/k8s/gen-secrets.sh"
  "$root/deploy/k8s/eck/fetch.sh"
  "$root/deploy/k8s/jobs/render.sh"
  "$root/deploy/k8s/base/s3/bucket-init.sh"
  "$root/deploy/k8s/components/snapshots/bootstrap.sh"
)
for script in "${scripts[@]}"; do
  case "$script" in
    */bucket-init.sh | */bootstrap.sh) sh -n "$script" ;;
    *) bash -n "$script" ;;
  esac || fail "$script does not parse"
done
if command -v shellcheck >/dev/null 2>&1; then
  shellcheck "${scripts[@]}" || fail "shellcheck found problems"
  step "shell scripts parse and pass shellcheck"
else
  step "shell scripts parse (shellcheck not installed; skipped)"
fi

# 7. Compose, when it exists.
compose="$root/deploy/compose/compose.yaml"
if [[ -f "$compose" ]]; then
  command -v docker >/dev/null 2>&1 || fail "$compose exists but docker is not installed"
  step "docker compose config"
  docker compose -f "$compose" config -q || fail "$compose does not validate"
fi

step "ok"
