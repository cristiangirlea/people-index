#!/usr/bin/env bash
# Shared image rules and rendered Job checks for deployment validation.
set -euo pipefail
# Image pinning. A reference passes as name:tag@sha256:<64 hex>: the tag for people, the digest
# for the bytes. The tag part cannot hold a `/`, so registry:port/name without a tag fails.
pinned_ref='^[^[:space:]@]+:[A-Za-z0-9._-]+@sha256:[0-9a-f]{64}$'
unpinned=()
check_ref() {
  case "$2" in
    # Built from this repository, never pulled.
    people-index:local) [[ "$1" == deploy/k8s/overlays/local || "$1" == overlays/local || "$1" == "schedule overlays/local" || "$1" == "jobs/"*" overlays/local" || "$1" == deploy/compose/compose.yaml ]] && return ;;
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


if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  k8s="$1"; overlay="$2"; kind="$3"
  work="$(mktemp -d)"
  trap 'rm -rf "$work"' EXIT
  fail() { echo "check-deploy: $*" >&2; exit 1; }
  render_kind="$kind"
  [[ "$kind" == schedule ]] && render_kind=run
  env -u IMAGE RUN_ID=check JOB_ID=check bash "$k8s/jobs/render.sh" "$render_kind" --overlay "$overlay" >"$work/job.yaml" 2>"$work/render.err" || {
    cat "$work/render.err" >&2
    fail "jobs/$render_kind does not render for $overlay"
  }
  if [[ "$kind" == schedule ]]; then
    schedule_image="$(yaml_images <"$work/job.yaml" | head -n 1)"
    [[ -n "$schedule_image" ]] || fail "no image rendered for $overlay"
    check_ref "schedule overlays/$overlay" "$schedule_image"
    report_unpinned
    printf '%s\n' "$schedule_image"
  else
    while read -r ref; do check_ref "jobs/$kind overlays/$overlay" "$ref"; done < <(yaml_images <"$work/job.yaml")
    report_unpinned
    cat "$work/job.yaml"
  fi
fi
