---
name: verify
description: Run the same checks as CI for map-event-pipeline — scalafmt, compile with -Werror, unit tests, Testcontainers integration tests and the mill build — and report what failed. Use before committing, before opening a PR, or when asked to verify, check or test the project.
allowed-tools: Bash(scripts/verify.sh*)
---

# Verify

Run `scripts/verify.sh` from the repository root and report the result.

- `/verify` runs everything: `sbt check`, `sbt itTest` (skipped with a note when Docker is not running) and the mill build with unit tests.
- `/verify quick` passes `--quick`: only `sbt check` (formatting, compile, unit tests). Use it for fast iteration.

The full run takes several minutes; run it in the background and wait for it to finish instead of polling.

## Reporting

1. Show the summary block the script prints last.
2. For each failed step, find the first real error in its output — a `[error]` line with a file and line number, a failed test name and its assertion, or a scalafmt "isn't formatted properly" line — and quote it with the file as a clickable `path:line` link.
3. Formatting failures: run `sbt fmt` and say so; do not hand-format.
4. Do not claim success for a step the summary marks `FAIL` or `skip`.
