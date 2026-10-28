#!/usr/bin/env bash
# Smoke test for a Helm install: posts an event through the gate and waits until the pipe has stored it.
#   scripts/k8s-smoke.sh [release]      (needs kubectl pointed at the cluster)
set -euo pipefail

release="${1:-mep}"
port=18080

kubectl port-forward "svc/$release-gate" "$port:8080" >/dev/null 2>&1 &
forward=$!
trap 'kill $forward 2>/dev/null || true' EXIT

for _ in $(seq 1 30); do curl -sf "localhost:$port/api/v1/health" >/dev/null && break; sleep 1; done

id=$(curl -sf -X POST "localhost:$port/api/v1/events" -H 'Content-Type: application/json' \
  -d '{"eventType":"Hazard","severity":"High","location":{"lat":50.45,"lon":30.52},"description":"k8s smoke"}' |
  sed -E 's/.*"id":"([^"]+)".*/\1/')
echo "posted $id"

for _ in $(seq 1 60); do
  if body=$(curl -sf "localhost:$port/api/v1/events/$id"); then
    echo "stored: $body"
    exit 0
  fi
  sleep 1
done
echo "event $id was not stored within a minute" >&2
kubectl logs "deploy/$release-pipe" --tail=50 >&2
exit 1
