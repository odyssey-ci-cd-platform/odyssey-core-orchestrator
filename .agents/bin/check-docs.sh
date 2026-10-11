#!/bin/sh
# check-docs.sh — flag mid-sentence hard wraps in Markdown files changed since <ref>.
#
# Usage: .agents/bin/check-docs.sh [ref]
#   ref defaults to origin/main when it exists, otherwise HEAD~1.
#
# Heuristic: inside a Markdown file, a prose line that does not end with
# terminal punctuation (. ! ? : ;) followed by a line that starts with a
# lowercase letter is almost certainly a sentence broken at the column limit.
# Skips code fences, table rows, headings, blockquotes, indented code, and
# list-continuation indents.
#
# Also warns if CHANGELOG.md has an empty [Unreleased] section while other
# Markdown files changed. It cannot prove a changelog entry exists for every
# change; that part stays a human/agent discipline.

set -u

REF="${1:-}"
if [ -z "$REF" ]; then
    if git rev-parse --verify -q origin/main >/dev/null 2>&1; then
        REF=origin/main
    else
        REF=HEAD~1
    fi
fi

changed="$(git diff --name-only --diff-filter=ACM "$REF" -- '*.md' 2>/dev/null)"
if [ -z "$changed" ]; then
    echo "check-docs: no Markdown files changed since $REF"
    exit 0
fi

violations=0

for f in $changed; do
    [ -f "$f" ] || continue
    # awk runs in a subprocess, so violations are counted by capturing its
    # output here, not by incrementing the shell's counter from inside awk.
    found=$(awk -v file="$f" '
        # Skip fenced code blocks (``` or ~~~).
        /^```/ || /^~~~/ { infence = !infence; next }
        infence { next }
        # Skip YAML frontmatter: metadata between --- fences at the top.
        NR == 1 && /^---$/ { infm = 1; next }
        infm { if (/^---$/) infm = 0; next }
        # Skip tables, headings, blockquotes, indented code, empty lines.
        /^\|/ || /^#/ || /^>/ || /^[ \t]/ || /^[ \t]*$/ { prev = ""; next }
        {
            if (prev != "" && prev ~ /[A-Za-z0-9,;]$/ && $0 ~ /^[a-z]/) {
                printf "%s:%d: possible mid-sentence break after: %s\n", file, NR - 1, prev
                found = 1
            }
            prev = $0
        }
        END { exit found ? 1 : 0 }
    ' "$f")
    if [ -n "$found" ]; then
        printf '%s\n' "$found"
        violations=$((violations + 1))
    fi
done

# Soft check: changelog should have at least one entry under [Unreleased]
# whenever anything changed.
others="$(printf '%s\n' "$changed" | grep -v '^CHANGELOG.md$' | head -1)"
if [ -n "$others" ]; then
    if ! awk '/^## \[Unreleased\]/{found=1; next} found && /^- /{good=1} END{exit !good}' CHANGELOG.md 2>/dev/null; then
        echo "check-docs: WARNING: CHANGELOG.md has no entry under [Unreleased] but other files changed"
    fi
fi

if [ "$violations" -gt 0 ]; then
    echo "check-docs: $violations violation(s). Fix by joining the sentence into one line."
    exit 1
fi
echo "check-docs: OK"
