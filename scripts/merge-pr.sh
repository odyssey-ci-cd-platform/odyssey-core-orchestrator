#!/bin/sh
# Merge a PR, delete its branch both ends, sync main, show the result.
# Usage: scripts/merge-pr.sh BRANCH
set -eu

branch=${1:?branch required}

# The required status check must be green on the head before GitHub will
# accept the merge; merging straight after a push races it.
gh pr checks "$branch" --watch --interval 10
gh pr merge "$branch" --merge --delete-branch
git checkout main
git pull
git log --oneline -3
git ls-remote --heads origin
