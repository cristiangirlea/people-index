#!/usr/bin/env bash
# Creates the git-ignored credential env files an overlay's secretGenerator reads, from the
# committed *.env.example files beside them, with fresh random values:
#
#   deploy/k8s/gen-secrets.sh local
#
# Existing env files are left alone (rotating a password is a deliberate edit, not a side
# effect of setup). Values are never printed.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
overlay="${1:-}"
if [[ ! "$overlay" =~ ^[a-z0-9-]+$ || ! -d "$here/overlays/$overlay/secrets" ]]; then
  echo "usage: $0 <overlay>   (an overlay directory with a secrets/ folder)" >&2
  exit 2
fi

# Random string of $1 characters from the set $2.
random() {
  local out=""
  while [[ ${#out} -lt $1 ]]; do
    out+="$(LC_ALL=C tr -dc "$2" </dev/urandom | head -c "$1" || true)"
  done
  printf '%s' "${out:0:$1}"
}

umask 077
shopt -s nullglob
for example in "$here/overlays/$overlay/secrets/"*.env.example; do
  target="${example%.example}"
  if [[ -e "$target" ]]; then
    echo "gen-secrets: keeping ${target#"$here/"}"
    continue
  fi
  tmp="$(mktemp "$target.XXXXXX")"
  while IFS= read -r line || [[ -n "$line" ]]; do
    if [[ "$line" =~ ^([A-Za-z_][A-Za-z0-9_]*)= ]]; then
      key="${BASH_REMATCH[1]}"
      case "$key" in
        # S3 access keys are conventionally upper-case alphanumerics of 20 characters.
        AWS_ACCESS_KEY_ID) printf '%s=%s\n' "$key" "$(random 20 'A-Z0-9')" ;;
        *) printf '%s=%s\n' "$key" "$(random 40 'A-Za-z0-9')" ;;
      esac
    else
      printf '%s\n' "$line"
    fi
  done <"$example" >"$tmp"
  mv "$tmp" "$target"
  echo "gen-secrets: wrote ${target#"$here/"}"
done
