#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

ACCESS_JWT="${ACCESS_JWT:?ACCESS_JWT is required}" \
  "${ROOT_DIR}/scripts/create-source.sh" \
  -s "Flights" \
  -c "IndiGo" \
  -i "New Delhi" \
  -t "Delhi (National Capital Territory)" \
  -n "India" \
  -w "goindigo.in" \
  -j "${SCRIPT_DIR}/table_schema.json"
