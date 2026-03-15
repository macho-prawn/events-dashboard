#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
DOCKER_ENV_FILE="${ROOT_DIR}/.env"
EVENTS_FILE="${SCRIPT_DIR}/sistic_events.json"

if [[ -f "${DOCKER_ENV_FILE}" ]]; then
  set -a
  # shellcheck disable=SC1090
  . "${DOCKER_ENV_FILE}"
  set +a
fi

BACKEND_PORT="${BACKEND_PORT:-3000}"
API_BASE_URL="${API_BASE_URL:-http://127.0.0.1:${BACKEND_PORT}}"
INGESTION_JWT="${INGESTION_JWT:?INGESTION_JWT is required}"

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

require_command curl
require_command jq

response_file="$(mktemp)"
trap 'rm -f "${response_file}"' EXIT

created_count=0
duplicate_count=0
processed_count=0
while IFS= read -r event_json; do
  if ! http_code="$(
    curl -sS \
      -o "${response_file}" \
      -w '%{http_code}' \
      -X POST \
      -H "Authorization: Bearer ${INGESTION_JWT}" \
      -H "Content-Type: application/json" \
      -d "${event_json}" \
      "${API_BASE_URL}/events"
  )"; then
    echo "sistic: request failed before receiving an HTTP response" >&2
    exit 1
  fi

  processed_count=$((processed_count + 1))
  case "${http_code}" in
    200|201)
      created_count=$((created_count + 1))
      ;;
    409)
      duplicate_count=$((duplicate_count + 1))
      ;;
    *)
      printf 'sistic: request %d failed with HTTP %s\n' "${processed_count}" "${http_code}" >&2
      cat "${response_file}" >&2
      exit 1
      ;;
  esac

  if (( processed_count % 100 == 0 )); then
    printf 'sistic: processed %d events (%d created, %d duplicates)\n' "${processed_count}" "${created_count}" "${duplicate_count}"
  fi
done < <(jq -c '.[]' "${EVENTS_FILE}")

printf 'sistic: processed %d events (%d created, %d duplicates)\n' "${processed_count}" "${created_count}" "${duplicate_count}"

if (( duplicate_count > 0 )); then
  printf 'sistic: duplicate events detected in %d documents\n' "${duplicate_count}" >&2
  exit 1
fi
