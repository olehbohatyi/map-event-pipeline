#!/usr/bin/env bash
# PostToolUse hook (Edit|Write): formats the .scala/.sbt file Claude just changed with the repo's
# .scalafmt.conf, so edits never leave formatting for CI to catch. Never blocks the edit.
set -uo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
input="$(cat)"

if command -v jq >/dev/null; then
  file="$(printf '%s' "$input" | jq -r '.tool_input.file_path // empty')"
elif command -v python3 >/dev/null; then
  file="$(printf '%s' "$input" | python3 -c 'import json, sys; print(json.load(sys.stdin).get("tool_input", {}).get("file_path", ""))')"
else
  exit 0
fi

case "$file" in
  *.scala | *.sbt) ;;
  *) exit 0 ;;
esac
[[ -f "$file" && "$file" == "$root"/* ]] || exit 0

# A native scalafmt is fastest; otherwise coursier launches the JVM CLI, which fetches the
# version pinned in .scalafmt.conf
if command -v scalafmt >/dev/null; then
  fmt=(scalafmt)
elif command -v cs >/dev/null; then
  fmt=(cs launch --java-opt --sun-misc-unsafe-memory-access=allow scalafmt --)
else
  exit 0
fi

if ! out="$("${fmt[@]}" --config "$root/.scalafmt.conf" "$file" 2>&1 | sed $'s/\x1b\\[[0-9;]*m//g'; exit "${PIPESTATUS[0]}")"; then
  # Usually a parse error mid-refactor; report it to Claude without blocking
  printf '{"hookSpecificOutput":{"hookEventName":"PostToolUse","additionalContext":%s}}\n' \
    "$(printf 'scalafmt could not format %s:\n%s' "$file" "$out" | python3 -c 'import json, sys; print(json.dumps(sys.stdin.read()))' 2>/dev/null || echo '"scalafmt failed"')"
fi
exit 0
