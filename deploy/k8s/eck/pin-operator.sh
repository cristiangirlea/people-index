#!/usr/bin/env bash
# Prints an ECK operator.yaml (one fetch.sh verified) with its operator image pinned by digest:
# the one `image: <name>:<tag>` line naming the reference in operator-image beside this script,
# without its digest, becomes that full name:tag@sha256:<digest> reference. make eck applies
# this output and scripts/check-deploy.sh validates it, so both use the same replacement.
#
#   deploy/k8s/eck/pin-operator.sh target/eck/operator.yaml >target/eck/operator-pinned.yaml
#
# Exits 1, printing nothing, unless operator-image holds exactly one name:tag@sha256:<digest>
# reference and operator.yaml names that name:tag exactly once (a version bump in fetch.sh
# without one here, or the reverse, fails).
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
pin_file="$here/operator-image"

die() {
  echo "pin-operator: $*" >&2
  exit 1
}

[[ $# -eq 1 && -f "$1" ]] || {
  echo "usage: $0 <operator.yaml>" >&2
  exit 2
}
[[ -f "$pin_file" ]] || die "no $pin_file"

# Lines other than comments and blanks.
mapfile -t refs < <(awk '!/^[[:space:]]*(#|$)/ { print $1 }' "$pin_file")
((${#refs[@]} == 1)) || die "$pin_file must hold exactly one image reference (has ${#refs[@]})"
ref="${refs[0]}"
[[ "$ref" =~ ^[^[:space:]@]+:[A-Za-z0-9._-]+@sha256:[0-9a-f]{64}$ ]] ||
  die "$pin_file: not pinned as name:tag@sha256:<digest>: $ref"
tagged="${ref%@*}"

# Compared as a string, not a regex: the value of each `image:` key, quotes dropped.
pinned="$(awk -v tagged="$tagged" -v ref="$ref" '
  match($0, /^[[:space:]-]*image:[[:space:]]*/) {
    value = substr($0, RLENGTH + 1)
    gsub(/["\047[:space:]]/, "", value)
    if (value == tagged) {
      print substr($0, 1, RLENGTH) "\"" ref "\""
      n++
      next
    }
  }
  { print }
  END { exit n == 1 ? 0 : 3 }
' "$1")" || die "$1 does not name $tagged exactly once"
printf '%s\n' "$pinned"
