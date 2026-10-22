#!/usr/bin/env bash
# Start the given ezu binary in a network namespace with nothing but
# loopback, and check that it serves its health and metrics endpoints.
# Needs root for the namespace (CI runs it with sudo).
set -euo pipefail
binary=$(realpath "$1")
work=$(mktemp -d)
# The server runs as root inside the namespace, so its files need sudo too.
trap 'sudo rm -rf "$work"' EXIT
printf 'offline-check-token' > "$work/token"
sudo unshare --net bash -euo pipefail -c '
  ip link set lo up
  if ip route get 1.1.1.1 >/dev/null 2>&1; then
    echo "the namespace still has a route out" >&2
    exit 1
  fi
  EZU_DATA_DIR="$1/data" EZU_STATIC_TOKEN_FILE="$1/token" EZU_STATIC_NAMESPACE=check \
    "$2" &
  server=$!
  trap "kill $server" EXIT
  for _ in $(seq 50); do
    curl -sf http://127.0.0.1:7600/healthz >/dev/null && break
    sleep 0.2
  done
  curl -sf http://127.0.0.1:7600/healthz
  echo
  curl -sf http://127.0.0.1:7600/metrics | grep -q "^ezu_up 1"
  curl -sf -H "authorization: Bearer offline-check-token" http://127.0.0.1:7600/v1/status
  echo
' offline-check "$work" "$binary"
