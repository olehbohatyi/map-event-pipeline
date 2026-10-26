#!/usr/bin/env bash
# Runs the same checks as CI. Used by the /verify Claude Code skill and by hand.
#
#   scripts/verify.sh           formatting, compile, unit tests (sbt), integration tests, mill
#   scripts/verify.sh --quick   formatting, compile, unit tests (sbt) only
set -uo pipefail

cd "$(dirname "$0")/.."

quick=false
[[ "${1:-}" == "--quick" ]] && quick=true

# Spark 4.x does not run on JDK 24+; stat's tests fork this Java 21
if [[ -z "${SPARK_JAVA_HOME:-}" ]] && command -v cs >/dev/null; then
  SPARK_JAVA_HOME="$(cs java-home --jvm temurin:21 2>/dev/null)" && export SPARK_JAVA_HOME
fi

declare -a results=()
failed=0

step() {
  local name="$1"
  shift
  echo "── $name: $*"
  if "$@"; then
    results+=("ok    $name")
  else
    results+=("FAIL  $name")
    failed=1
  fi
}

step "sbt check" sbt -batch check

if ! $quick; then
  if docker info >/dev/null 2>&1; then
    step "integration tests" sbt -batch itTest
  else
    results+=("skip  integration tests (Docker is not running)")
  fi
  step "mill" ./mill --no-server --ticker false __.compile + core.test + gate.test + pipe.test + stat.test
fi

echo
echo "── summary"
printf '%s\n' "${results[@]}"
exit $failed
