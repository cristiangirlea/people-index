#!/usr/bin/env bash
# Fails when a forbidden word appears in a tracked file, a staged file or a commit message.
# The word list lives outside the repository (a public repository cannot hold the words it
# guards against): SCRUB_WORDS_FILE, default ../people-index.scrub-words beside the checkout.
set -euo pipefail
root="$(git rev-parse --show-toplevel)"
words="${SCRUB_WORDS_FILE:-$root/../people-index.scrub-words}"
if [[ ! -f "$words" ]]; then
  echo "scrub-check: no word list at $words (set SCRUB_WORDS_FILE); skipped" >&2
  exit 0
fi
pattern="$(grep -v '^[[:space:]]*$' "$words" | paste -sd '|' -)"
[[ -z "$pattern" ]] && exit 0
# Matches are judged by output, not exit codes: xargs exits 0 when it has no files to give grep.
hits="$(
  cd "$root"
  git grep -I -n -i -E "$pattern" -- . ':!Cargo.lock' || true
  git ls-files -z --others --exclude-standard | xargs -0 -r grep -H -I -n -i -E "$pattern" || true
  if git rev-parse -q --verify HEAD >/dev/null; then
    git log --format='%h %B' | grep -i -E "$pattern" | sed 's/^/commit message: /' || true
  fi
)"
status=0
if [[ -n "$hits" ]]; then printf '%s\n' "$hits"; status=1; fi
if [[ "$status" -ne 0 ]]; then echo "scrub-check: forbidden words found (list: $words)" >&2; fi
exit "$status"
