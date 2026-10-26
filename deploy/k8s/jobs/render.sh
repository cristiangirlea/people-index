#!/usr/bin/env bash
# Renders a run (coordinator + worker Jobs), a datagen Job or a verify Job from the templates
# beside this script, for `kubectl apply -f -`.
#
#   deploy/k8s/jobs/render.sh run     [--overlay local]   > run.yaml
#   deploy/k8s/jobs/render.sh datagen [--overlay local]   > datagen.yaml
#   deploy/k8s/jobs/render.sh verify  [--overlay local]   > verify.yaml
#
# Values come from deploy/k8s/overlays/<overlay>/jobs.vars (KEY=value lines; read as data,
# never executed), and an environment variable of the same name wins:
#   RUN_ID=nightly-01 WORKERS=6 deploy/k8s/jobs/render.sh run | kubectl --context k3d-people-index apply -f -
# RUN_ID (run) and JOB_ID (datagen, verify) default to the UTC time plus a random suffix.
#
# Every value is checked against a strict pattern before it is substituted, so nothing a caller
# passes can add YAML structure, and the output is refused if any ${...} is left.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
usage() {
  echo "usage: $0 run|datagen|verify [--overlay <name>]" >&2
  exit 2
}

kind="${1:-}"
[[ "$kind" == run || "$kind" == datagen || "$kind" == verify ]] || usage
shift
overlay="${OVERLAY:-local}"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --overlay)
      [[ $# -ge 2 ]] || usage
      overlay="$2"
      shift 2
      ;;
    *) usage ;;
  esac
done
[[ "$overlay" =~ ^[a-z0-9-]+$ ]] || { echo "render: bad overlay name: $overlay" >&2; exit 2; }
vars_file="$here/../overlays/$overlay/jobs.vars"
[[ -f "$vars_file" ]] || { echo "render: no $vars_file" >&2; exit 2; }

if [[ "$kind" == run ]]; then
  names=(RUN_ID IMAGE IMAGE_PULL_POLICY WORKERS WORKER_BACKOFF_LIMIT WORKER_CONCURRENCY
    WORKER_CPU WORKER_MEMORY COORDINATOR_CONCURRENCY COORDINATOR_CPU COORDINATOR_MEMORY
    RUN_DEADLINE_SECONDS TMP_SIZE)
elif [[ "$kind" == datagen ]]; then
  names=(JOB_ID IMAGE IMAGE_PULL_POLICY PERSONS ORGANIZATIONS FILES SEED DATAGEN_CPU
    DATAGEN_MEMORY DATAGEN_DEADLINE_SECONDS TMP_SIZE)
else
  names=(JOB_ID IMAGE IMAGE_PULL_POLICY VERIFY_SOURCE VERIFY_CPU VERIFY_MEMORY
    VERIFY_DEADLINE_SECONDS)
fi

declare -A value
# Defaults from the overlay: KEY=value per line, '#' comments, surrounding spaces ignored.
while IFS= read -r line || [[ -n "$line" ]]; do
  line="${line%%#*}"
  line="${line#"${line%%[![:space:]]*}"}"
  line="${line%"${line##*[![:space:]]}"}"
  [[ -z "$line" ]] && continue
  [[ "$line" =~ ^([A-Z_][A-Z0-9_]*)=(.*)$ ]] || { echo "render: bad line in $vars_file: $line" >&2; exit 2; }
  value["${BASH_REMATCH[1]}"]="${BASH_REMATCH[2]}"
done <"$vars_file"
# The environment wins over the overlay.
for name in "${names[@]}"; do
  if [[ -n "${!name:-}" ]]; then value[$name]="${!name}"; fi
done

generated_id() {
  printf '%s-%s' "$(date -u +%Y%m%d-%H%M%S)" "$(od -An -N2 -tx1 /dev/urandom | tr -d ' \n')"
}
if [[ "$kind" == run && -z "${value[RUN_ID]:-}" ]]; then value[RUN_ID]="$(generated_id)"; fi
if [[ "$kind" != run && -z "${value[JOB_ID]:-}" ]]; then value[JOB_ID]="$(generated_id)"; fi

# Patterns per value. Ids become part of Job names (at most 63 characters with the `pi-worker-`
# prefix) and label values, so they are DNS labels of at most 40 characters.
id='^[a-z0-9]([a-z0-9-]{0,38}[a-z0-9])?$'
int='^[0-9]+$'
quantity='^[0-9]+(\.[0-9]+)?(m|k|M|G|T|Ki|Mi|Gi|Ti)?$'
pattern_for() {
  case "$1" in
    RUN_ID | JOB_ID) echo "$id" ;;
    IMAGE) echo '^[A-Za-z0-9][A-Za-z0-9./:_@-]*$' ;;
    IMAGE_PULL_POLICY) echo '^(Always|IfNotPresent|Never)$' ;;
    VERIFY_SOURCE) echo '^(expected|summary)$' ;;
    *_CPU | *_MEMORY | TMP_SIZE) echo "$quantity" ;;
    *) echo "$int" ;;
  esac
}
for name in "${names[@]}"; do
  v="${value[$name]:-}"
  if [[ -z "$v" ]]; then
    echo "render: $name is not set (in $vars_file or the environment)" >&2
    exit 2
  fi
  if ! [[ "$v" =~ $(pattern_for "$name") ]]; then
    echo "render: $name=$v does not match $(pattern_for "$name")" >&2
    exit 2
  fi
done
if [[ "$kind" == run && "${value[WORKERS]}" -lt 1 ]]; then
  echo "render: WORKERS must be at least 1" >&2
  exit 2
fi

out="$(<"$here/$kind.yaml")"
for name in "${names[@]}"; do
  out="${out//\$\{$name\}/${value[$name]}}"
done
if [[ "$out" =~ \$\{[A-Za-z_]+\} ]]; then
  echo "render: placeholder left in $kind.yaml: ${BASH_REMATCH[0]}" >&2
  exit 1
fi
printf '%s\n' "$out"
if [[ "$kind" == run ]]; then
  echo "render: run id ${value[RUN_ID]}" >&2
else
  echo "render: $kind job ${value[JOB_ID]}" >&2
fi
