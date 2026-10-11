#!/bin/sh
# Push a branch, open its PR, watch CI until done.
# Usage: scripts/ship-pr.sh BRANCH TITLE [BODY-FILE]
# Body defaults to "Closes #N" (the issue number in feature/N branches;
# other branches get a one-commit-task note).
set -eu

branch=${1:?branch required}
title=${2:?title required}
body_file=${3:-}
temp_body=""

if [ -z "$body_file" ]; then
  # Only feature/<number> branches carry an issue (AUD-020): a number found
  # anywhere else in the branch name is not an issue reference.
  n=$(printf '%s' "$branch" | sed -n 's/^feature\/\([0-9][0-9]*\)$/\1/p')
  body_file=$(mktemp)
  temp_body=$body_file
  if [ -n "$n" ]; then
    printf 'Closes #%s.\n' "$n" > "$body_file"
  else
    printf 'One-commit task (no issue).\n' > "$body_file"
  fi
fi
trap '[ -n "$temp_body" ] && rm -f "$temp_body"' EXIT

git push -u origin "$branch"

gh pr create --base main --head "$branch" --title "$title" --body-file "$body_file"

# Wait for CI to register before watching — a fixed sleep raced slow
# workflow starts (AUD-020).
i=0
until gh pr checks "$branch" >/dev/null 2>&1; do
  i=$((i + 1))
  if [ "$i" -gt 24 ]; then
    break
  fi
  sleep 5
done
gh pr checks "$branch" --watch --interval 10
