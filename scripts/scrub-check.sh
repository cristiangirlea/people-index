#!/usr/bin/env bash
# Fails when a forbidden word appears in a tracked file, a staged file or a commit message.
# The word list lives outside the repository (a public repository cannot hold the words it
# guards against): SCRUB_WORDS_FILE, or by default people-index.scrub-words beside the main
# checkout. The default is found through the common git directory, so every worktree of the
# repository uses the same list wherever the worktree lives.
#
#   scrub-check.sh                    the tree, untracked files and every commit message
#   scrub-check.sh --message <file>   one commit message (the commit-msg hook)
set -euo pipefail
root="$(git rev-parse --show-toplevel)"
common="$(git rev-parse --path-format=absolute --git-common-dir)"
words="${SCRUB_WORDS_FILE:-$(dirname "$(dirname "$common")")/people-index.scrub-words}"
if [[ ! -f "$words" ]]; then
  # CI never has the list (the repository is public); locally, say where it was looked for.
  echo "scrub-check: no word list at $words (set SCRUB_WORDS_FILE); skipped" >&2
  exit 0
fi
pattern="$(grep -v '^[[:space:]]*$' "$words" | paste -sd '|' -)"
[[ -z "$pattern" ]] && exit 0

if [[ "${1:-}" == --message ]]; then
  message="${2:?usage: scrub-check.sh --message <file>}"
  # grep exits 1 for no match and 2 when it cannot check (unreadable file, invalid pattern in
  # the list): only 1 lets the commit through.
  rc=0
  grep -n -i -E "$pattern" "$message" || rc=$?
  case "$rc" in
    0) echo "scrub-check: forbidden word in the commit message (list: $words)" >&2; exit 1 ;;
    1) exit 0 ;;
    *) echo "scrub-check: cannot check $message against $words" >&2; exit 2 ;;
  esac
fi

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
