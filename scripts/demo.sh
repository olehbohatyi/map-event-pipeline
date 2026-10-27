#!/usr/bin/env bash
# Posts random road events around Ukrainian cities to the gate, to watch them appear on the live map (/map).
#   scripts/demo.sh             50 events, one every 0.5 s
#   scripts/demo.sh 200 0.1     200 events, one every 0.1 s
# GATE_URL overrides the gate address (default http://localhost:${GATE_PORT:-8080}).
set -euo pipefail

count="${1:-50}"
delay="${2:-0.5}"
gate="${GATE_URL:-http://localhost:${GATE_PORT:-8080}}"

cities=(
  "50.45 30.52" "49.84 24.03" "49.99 36.23" "46.48 30.72" "48.46 35.05" "47.84 35.14"
  "49.23 28.47" "49.59 34.55" "51.50 31.29" "48.62 22.29" "48.29 25.94" "46.98 31.99"
  "50.91 34.80" "50.25 28.66" "50.62 26.25" "48.92 24.71"
)
types=(Hazard Hazard Hazard Sign Traffic Traffic)
severities=(Low Low Low Medium Medium High Critical)
hazards=("pothole" "ice on the road" "flooding" "fallen tree")
signs=("missing sign" "damaged sign" "traffic light out")
traffic=("traffic jam" "accident" "road works")

# One random argument (no namerefs: macOS still ships bash 3.2)
pick() { local items=("$@"); echo "${items[RANDOM % $#]}"; }

# Up to ±0.15° around the city centre
jitter() { awk -v v="$1" -v r="$RANDOM" 'BEGIN { printf "%.4f", v + (r / 32767 - 0.5) * 0.3 }'; }

for ((i = 1; i <= count; i++)); do
  read -r lat lon <<<"$(pick "${cities[@]}")"
  type="$(pick "${types[@]}")"
  case "$type" in
    Hazard) text="$(pick "${hazards[@]}")" ;;
    Sign) text="$(pick "${signs[@]}")" ;;
    *) text="$(pick "${traffic[@]}")" ;;
  esac
  body=$(printf '{"eventType":"%s","severity":"%s","location":{"lat":%s,"lon":%s},"description":"%s"}' \
    "$type" "$(pick "${severities[@]}")" "$(jitter "$lat")" "$(jitter "$lon")" "$text")
  # 000 when the gate is unreachable; keep going so a restarting gate does not end the demo
  status=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$gate/api/v1/events" \
    -H 'Content-Type: application/json' -d "$body" || true)
  printf '%3d/%d %s %s\n' "$i" "$count" "$status" "$body"
  sleep "$delay"
done
