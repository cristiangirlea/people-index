#!/usr/bin/env bash
# Validates the deployment without a cluster:
#   - every image in the Dockerfile (its `# syntax=` frontend included), deploy/compose,
#     deploy/k3d, the CI workflow's `docker run` lines, the ECK operator manifest (as pinned
#     by deploy/k8s/eck/pin-operator.sh from deploy/k8s/eck/operator-image), deploy/k8s
#     (rendered) and the Job templates is pinned as name:tag@sha256:<digest>
#     (people-index:local and the Job templates' ${IMAGE} excepted), and the Dockerfile's rust
#     tag is CI's RUST_TOOLCHAIN;
#   - an image in both compose and deploy/k8s/base is the same reference in both, and the
#     Elasticsearch resource's image tag is its spec.version;
#   - the ECK release files, fetched by deploy/k8s/eck/fetch.sh (what `make eck` applies),
#     still match deploy/k8s/eck/SHA256SUMS, and fetch.sh refuses files that do not;
#   - those ECK files (operator.yaml pinned), base and every overlay (rendered with kustomize)
#     pass kubeconform -strict against the Kubernetes schemas and, for custom resources (ECK,
#     prometheus-operator), the datreeio CRDs-catalog schemas at a pinned commit;
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
# The CRDs-catalog at a fixed commit, so the schemas cannot change under an unchanged tree. To
# move it: gh api repos/datreeio/CRDs-catalog/commits/main --jq .sha
crd_catalog_commit="63669a570e231d4f1f8396d229a1de512bcf0a34"
crd_catalog="https://raw.githubusercontent.com/datreeio/CRDs-catalog/$crd_catalog_commit/"'{{.Group}}/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json'
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

# Image pinning. A reference passes as name:tag@sha256:<64 hex>: the tag for people, the digest
# for the bytes. The tag part cannot hold a `/`, so registry:port/name without a tag fails.
pinned_ref='^[^[:space:]@]+:[A-Za-z0-9._-]+@sha256:[0-9a-f]{64}$'
unpinned=()
check_ref() {
  case "$2" in
    # Built from this repository, never pulled; the Job templates' placeholder (jobs.vars).
    people-index:local | "\${IMAGE}") return ;;
  esac
  [[ "$2" =~ $pinned_ref ]] || { unpinned+=("$1: $2"); return; }
  # scripts/check-pins-resolve.sh collects every pinned reference this way.
  [[ -z "${PINNED_REFS_OUT:-}" ]] || printf '%s\n' "$2" >>"$PINNED_REFS_OUT"
}
# The value of every `image:` key in YAML read from stdin, quotes dropped.
yaml_images() {
  sed -nE "s/^[[:space:]-]*image:[[:space:]]*[\"']?([^\"'[:space:]#]+).*/\1/p"
}
report_unpinned() {
  ((${#unpinned[@]} == 0)) && return
  printf 'check-deploy: not pinned as name:tag@sha256:<digest>: %s\n' "${unpinned[@]}" >&2
  exit 1
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

# 0. Images are pinned by digest outside deploy/k8s (Dockerfile, compose, k3d, CI) (checked after rendering, in step 3), and
#    the image builds with the toolchain CI tests.
stages=()
while read -r keyword ref as stage _; do
  [[ "$keyword" == FROM ]] || continue
  in_stage=false
  for name in "${stages[@]}"; do [[ "$ref" == "$name" ]] && in_stage=true; done
  "$in_stage" || check_ref Dockerfile "$ref"
  [[ "${as,,}" == as && -n "$stage" ]] && stages+=("$stage")
done <"$root/Dockerfile"
# The BuildKit frontend named by the `# syntax=` parser directive (leading comment lines only).
syntax_ref="$(sed -nE '/^#[[:space:]]*[A-Za-z]+[[:space:]]*=/!q
  s/^#[[:space:]]*syntax[[:space:]]*=[[:space:]]*([^[:space:]]+).*/\1/p' "$root/Dockerfile")"
[[ -z "$syntax_ref" ]] || check_ref "Dockerfile (# syntax=)" "$syntax_ref"
# Compose and Kubernetes run the same images: name -> ref, compared after rendering base.
declare -A compose_refs=()
while read -r ref; do
  check_ref deploy/compose/compose.yaml "$ref"
  compose_refs["${ref%%[:@]*}"]="$ref"
done < <(yaml_images <"$root/deploy/compose/compose.yaml")
# The k3d node image (top-level `image:` of the cluster config).
while read -r ref; do
  check_ref deploy/k3d/cluster.yaml "$ref"
done < <(sed -nE 's/^image:[[:space:]]*([^[:space:]#]+).*/\1/p' "$root/deploy/k3d/cluster.yaml")
# In `docker run` lines, every word that looks like name:tag or name@digest.
while read -r ref; do
  check_ref .github/workflows/ci.yml "$ref"
done < <(sed -nE 's/^[[:space:]]*(run:[[:space:]]*)?docker run( .*)/\2/p' "$root/.github/workflows/ci.yml" |
  grep -oE '[[:space:]][a-z0-9][a-z0-9._/-]*[:@][^[:space:]]*' | sed -E 's/^[[:space:]]+//')
report_unpinned
docker_rust="$(sed -nE 's/^FROM rust:([0-9][0-9.]*)-.*/\1/p' "$root/Dockerfile")"
ci_rust="$(sed -nE 's/^[[:space:]]*RUST_TOOLCHAIN:[[:space:]]*"?([0-9][0-9.]*)"?.*/\1/p' \
  "$root/.github/workflows/ci.yml")"
[[ -n "$docker_rust" && "$docker_rust" == "$ci_rust" ]] ||
  fail "Dockerfile builds with rust '${docker_rust}', CI with RUST_TOOLCHAIN '${ci_rust}': make them equal"
step "images pinned by digest; Dockerfile and CI build with rust $ci_rust"

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

# 3. The verified ECK files, with the operator image pinned the way `make eck` pins it, and
#    every kustomization render, validate and name only pinned images.
step "eck (crds.yaml, operator.yaml pinned by operator-image)"
bash "$root/deploy/k8s/eck/pin-operator.sh" "$eck_dir/operator.yaml" >"$work/eck-operator.yaml" ||
  fail "deploy/k8s/eck/pin-operator.sh could not pin the operator image"
# make eck applies the pinned operator, never the raw verified file.
eck_recipe="$(awk '/^eck:/ { on = 1; next } on && !/^\t/ { exit } on' "$root/Makefile")"
[[ "$eck_recipe" == *pin-operator.sh* && "$eck_recipe" == *"-f \$(ECK_DIR)/operator-pinned.yaml"* &&
  "$eck_recipe" != *"-f \$(ECK_DIR)/operator.yaml"* ]] ||
  fail "the Makefile's eck target must apply \$(ECK_DIR)/operator-pinned.yaml made by pin-operator.sh"
# pin-operator.sh refuses, printing nothing, anything but one pinned reference that operator.yaml
# names exactly once: a copy of it next to a crafted operator-image, run on crafted inputs.
pin_bad="$work/pin-bad"
mkdir -p "$pin_bad"
cp "$root/deploy/k8s/eck/pin-operator.sh" "$pin_bad/"
operator_ref="$(awk '!/^[[:space:]]*(#|$)/ { print $1 }' "$root/deploy/k8s/eck/operator-image")"
operator_tag="${operator_ref%@*}"
refuses() { # <case> <operator-image content> <operator.yaml>
  local out rc=0
  printf '%s\n' "$2" >"$pin_bad/operator-image"
  out="$(bash "$pin_bad/pin-operator.sh" "$3" 2>/dev/null)" || rc=$?
  [[ "$rc" -eq 1 && -z "$out" ]] || fail "pin-operator.sh accepted $1 (exit $rc)"
}
refuses "an unpinned reference" "$operator_tag" "$eck_dir/operator.yaml"
refuses "two references" "$operator_ref"$'\n'"$operator_ref" "$eck_dir/operator.yaml"
refuses "another tag" "${operator_tag%:*}:0.0.0@${operator_ref#*@}" "$eck_dir/operator.yaml"
grep -v -F "$operator_tag" "$eck_dir/operator.yaml" >"$pin_bad/none.yaml"
refuses "an operator.yaml without the image" "$operator_ref" "$pin_bad/none.yaml"
{
  cat "$eck_dir/operator.yaml"
  grep -F "$operator_tag" "$eck_dir/operator.yaml"
} >"$pin_bad/twice.yaml"
refuses "an operator.yaml naming the image twice" "$operator_ref" "$pin_bad/twice.yaml"
step "make eck applies the pinned operator; pin-operator.sh refuses bad input"
{
  cat "$eck_dir/crds.yaml"
  printf '\n---\n'
  cat "$work/eck-operator.yaml"
} >"$work/eck.yaml"
validate <"$work/eck.yaml" || fail "the ECK files do not validate"
while read -r ref; do
  check_ref deploy/k8s/eck "$ref"
done < <(yaml_images <"$work/eck.yaml")
report_unpinned
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
  while read -r ref; do
    check_ref "deploy/k8s/$dir" "$ref"
    # Dependabot bumps compose and the manifests in separate pull requests; keep them equal.
    name="${ref%%[:@]*}"
    if [[ "$dir" == base && -n "${compose_refs[$name]:-}" && "${compose_refs[$name]}" != "$ref" ]]; then
      fail "deploy/k8s/base runs $ref, deploy/compose/compose.yaml ${compose_refs[$name]}: make them equal"
    fi
  done < <(yaml_images <"$work/rendered.yaml")
  report_unpinned
  # Local overlays render the identity generator, both bucket initializers and bootstrap.
  # Compare their actual rendered env values, so an overlay cannot silently split them.
  if [[ "$dir" == overlays/local ]]; then
    bucket_values="$(awk '
      /^[[:space:]]*- name: (PIPELINE_BUCKET|SNAPSHOT_BUCKET|S3_BUCKET)$/ { name=$3; next }
      name && /^[[:space:]]*value: / { print name, $2; name=""; next }
      name && /^[[:space:]]*- name:/ { name="" }
    ' "$work/rendered.yaml")"
    pipeline="$(awk '$1 == "PIPELINE_BUCKET" {print $2}' <<<"$bucket_values")"
    snapshots="$(awk '$1 == "SNAPSHOT_BUCKET" {print $2}' <<<"$bucket_values")"
    initializers="$(awk '$1 == "S3_BUCKET" {print $2}' <<<"$bucket_values" | sort)"
    [[ -n "$pipeline" && $(wc -l <<<"$snapshots") -eq 2 ]] || fail "local bucket environment is incomplete"
    store_url="$(awk '$1 == "PI_STORE_URL:" {print $2}' "$work/rendered.yaml")"
    [[ "$store_url" == "s3://$pipeline/"* ]] || fail "local pipeline store URL and bucket name disagree"
    snapshot="$(head -n 1 <<<"$snapshots")"
    [[ -n "$snapshot" && "$snapshots" == "$snapshot"$'\n'"$snapshot" && "$pipeline" != "$snapshot" &&
       "$initializers" == "$(printf '%s\n%s\n' "$pipeline" "$snapshot" | sort)" ]] ||
      fail "local identity, bucket-init and bootstrap bucket names disagree"
  fi
  # ECK acts on spec.version, the pods run spec.image: the image tag must be that version.
  es_spec="$(awk '/^---/ { es = 0 } /^kind: Elasticsearch$/ { es = 1 }
    es && /^  (version|image):/ { print $1, $2 }' "$work/rendered.yaml")"
  es_version="$(sed -nE 's/^version: *//p' <<<"$es_spec")"
  es_image="$(sed -nE 's/^image: *//p' <<<"$es_spec")"
  if [[ -n "$es_version$es_image" ]]; then
    es_tag="${es_image%@*}"
    es_tag="${es_tag##*:}"
    [[ "$es_tag" == "$es_version" ]] ||
      fail "deploy/k8s/$dir: Elasticsearch spec.version '$es_version' but spec.image '$es_image': make them equal"
  fi
done

# 4. The Job templates name only pinned images (their ${IMAGE} comes from jobs.vars), and render
#    for every overlay and validate.
job_kinds=(run datagen verify)
for kind in "${job_kinds[@]}"; do
  while read -r ref; do
    check_ref "deploy/k8s/jobs/$kind.yaml" "$ref"
  done < <(yaml_images <"$work/k8s/jobs/$kind.yaml")
done
report_unpinned
for vars in "$work"/k8s/overlays/*/jobs.vars; do
  overlay="$(basename "$(dirname "$vars")")"
  for kind in "${job_kinds[@]}"; do
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
  "$root/deploy/k8s/eck/pin-operator.sh"
  "$root/deploy/k8s/jobs/render.sh"
  "$root/deploy/k8s/base/s3/bucket-init.sh"
  "$root/deploy/k8s/base/s3/identity-config.sh"
  "$root/deploy/k8s/components/snapshots/bootstrap.sh"
)
for script in "${scripts[@]}"; do
  case "$script" in
    */bucket-init.sh | */identity-config.sh | */bootstrap.sh) sh -n "$script" ;;
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
  python="$(command -v python3 || command -v python)" || fail "needs python3 or python for compose bucket checks"
  docker compose -f "$compose" config --format json >"$work/compose.json" || fail "$compose does not validate"
  "$python" - "$work/compose.json" <<'PY' || fail "compose bucket names disagree"
import copy, json, sys
with open(sys.argv[1], encoding="utf-8") as f:
    services = json.load(f)["services"]
def agrees(services):
    s3 = services["s3"]["environment"]
    init = services["s3-init"]["environment"]
    pipeline = s3["PIPELINE_BUCKET"]
    snapshot = s3["SNAPSHOT_BUCKET"]
    return (pipeline != snapshot and init["S3_BUCKET"] == pipeline
            and init["SNAPSHOT_BUCKET"] == snapshot
            and all(services[name]["environment"]["PI_STORE_URL"].startswith("s3://" + pipeline + "/")
                    for name in ("coordinator", "worker")))
if not agrees(services):
    sys.exit("rendered compose buckets disagree")
bad = copy.deepcopy(services)
bad["s3-init"]["environment"]["S3_BUCKET"] = "different-bucket"
if agrees(bad):
    sys.exit("bucket check accepted a mismatch")
PY
fi

step "ok"
