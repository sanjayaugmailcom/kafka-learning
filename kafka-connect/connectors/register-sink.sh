#!/usr/bin/env bash
# POST the sink connector config to the Connect REST API.
# Run this after `docker compose up` and Connect is healthy (port 8083).
: "${DB_PASSWORD:=secret}"
export DB_PASSWORD
curl -s -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d "$(envsubst < "$(dirname "$0")/jdbc-sink.json")" | jq .
