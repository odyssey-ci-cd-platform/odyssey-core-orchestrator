# Audit process

An audit is an independent, read-only evaluation of the repository's committed state.
It judges the project against what the project says it is trying to be, records findings with evidence, and tracks whether earlier findings were fixed.

This process is tool-agnostic.
Any auditor — a person, or any agent, model, or harness — follows the same steps and produces the same file shape.
Nothing here assumes a particular editor, CLI, model, or automation.

## Scope

Every audit covers the whole repository at a single commit:

- project architecture,
- code structure,
- test-driven development practice,
- agent and contributor workflows (issue flow, branch and commit conventions, CI, scripts),
- the code itself,
- whether each unit of code does what its name, doc comment, and the design docs say it does,
- whether decisions and implementations around architecture, coding style, formatting, tooling, documentation, cleanliness, and best practices hold up.

## Rules

- Audit committed state only. Record the commit SHA; ignore uncommitted or untracked work unless it is gitignored on purpose and the repository depends on it (say so explicitly when you include it).
- Audits are read-only. Do not fix anything during an audit; fixes are separate work items that follow the normal workflow in `AGENTS.md` and `RUNBOOK.md`.
- Inspect a clean checkout of the audited commit (for example a detached worktree), never the working tree someone is editing.
- Every finding needs evidence: a `path:line` reference, a command and its output, or a reproduction. Claims without evidence are not findings.
- Mark each finding **Confirmed** (reproduced or verified by running something) or **Plausible** (established by reading code, not executed).
- Anything a reproduction creates (containers, files, worktrees, processes) is cleaned up before the audit ends. Pre-existing state is reported, not removed.
- An audit file is never edited after it is committed. Corrections go in the next audit.

## Procedure

Work through the steps in order; later steps depend on understanding from earlier ones.

### 1. Prepare

1. Record the current UTC date and time, and the commit SHA being audited.
2. Create a clean checkout of that commit.
3. Find the previous audit: the lexicographically last `*-audit.md` file in this folder. If none exists, this is the first audit.

### 2. Reconcile the previous audit

Skip this step for the first audit.

For every finding in the previous audit whose status was not `Fixed` or `Obsolete` — that is, its own findings plus its backlog — re-check it at the new commit and assign one status:

- **Fixed** — the defect no longer reproduces. Cite the commit or PR that fixed it, and the evidence.
- **Partially fixed** — some of it is resolved. State what remains; it carries over.
- **Open** — unchanged. It carries over.
- **Obsolete** — the code or decision it concerned no longer exists, or the project deliberately decided against the fix (cite where the decision is recorded).

Every finding from the previous audit appears in exactly one of the new audit's sections: *Resolved since last audit* (Fixed, Obsolete) or *Backlog* (Partially fixed, Open).
Nothing is dropped silently.

### 3. Read the docs

Read `README.md`, `AGENTS.md`, `RUNBOOK.md`, `CHANGELOG.md`, and everything under `docs/`.
Establish what the project claims: its goals, principles, conventions, and recorded decisions.
These claims are the yardstick for every later step.

### 4. Evaluate the decisions

Check each recorded decision (README, ADRs) for internal consistency, for staleness against later decisions, and for whether the code actually follows it.

### 5. Evaluate the setup

Check the build, module, and dependency hygiene, the CI workflows, scripts, templates, and gate commands.
Run the gate (`make check`) and any formatting and tidiness checks the toolchain offers, and record the results.

### 6. Evaluate the tests

Run the full suite (with the race detector where the language has one) and record pass/fail and coverage.
Judge whether tests assert the intended behavior or lock in defects, what is untested, whether test conventions are followed, and whether the history shows tests written before the change.

### 7. Evaluate the implementation

Read every non-generated source file.
For each unit, ask whether it does what it claims, and how it behaves on error paths, cancellation, concurrency, and edge cases.
Reproduce suspected defects where it is cheap and safe to do so.

### 8. Write the audit file

Write the file described below, then clean up anything the audit created.

## File naming

`YYYY-MM-DD-HHMM-audit.md`, using the UTC time at which the audit started, for example `2026-10-08-1901-audit.md`.
The format sorts chronologically, so the latest audit is always the last file in a listing.

## Finding IDs and severity

Each finding has a stable ID of the form `AUD-NNN`.
IDs are never reused or renumbered: a carried-over finding keeps its original ID forever, and a new finding takes the next number after the highest ID in any previous audit.

Severity:

- **Critical** — data loss or corruption, resource leaks, security exposure, or a core design guarantee that does not hold.
- **High** — incorrect behavior users will hit, or a gap that blocks a stated goal.
- **Medium** — incorrect behavior in edge cases, or a design flaw with a clear cost.
- **Low** — hygiene, consistency, documentation drift.

A carried-over finding may be re-graded; say why in its entry.

## Audit file template

```markdown
# Audit YYYY-MM-DD HH:MM UTC

- **Commit:** <sha> (<branch or PR context>)
- **Auditor:** <who or what performed the audit>
- **Previous audit:** <filename, or "none — first audit">
- **Gate:** <make check result>; <race/coverage summary>; <format/tidy results>

## Summary

<Two to five sentences: overall verdict and the most important themes.>

## Resolved since last audit

| ID | Title | Status | Evidence |
|----|-------|--------|----------|
| AUD-NNN | ... | Fixed / Obsolete | <commit, PR, or reason> |

<Or "N/A — first audit." / "None.">

## Backlog

Findings carried over from earlier audits that are still open or only partially fixed.

| ID | Severity | Title | First seen | Audits open | Status | Notes |
|----|----------|-------|------------|-------------|--------|-------|
| AUD-NNN | High | ... | <audit filename> | <count> | Open / Partially fixed | <what remains> |

<Or "N/A — first audit." / "None.">

## New findings

### AUD-NNN — <title>

- **Severity:** Critical / High / Medium / Low
- **Category:** architecture / correctness / security / testing / workflow / tooling / docs / style
- **Verification:** Confirmed / Plausible
- **Location:** `path:line`
- **Problem:** <what is wrong, measured against which stated intent>
- **Failure scenario:** <concrete input or state leading to the wrong outcome>
- **Suggested fix:** <direction, not a patch>

## Strengths

<Brief: what is done well and should be kept.>

## Recommended order

<The few things to fix first, by ID.>
```
