#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

ACCESS_JWT="${ACCESS_JWT:?ACCESS_JWT is required}" \
  "${ROOT_DIR}/scripts/create-source.sh" \
  -s "Events" \
  -c "Eventim" \
  -i "Bremen" \
  -t "Bremen" \
  -n "Germany" \
  -w "eventim.de" \
  -j "${SCRIPT_DIR}/table_schema.json"
