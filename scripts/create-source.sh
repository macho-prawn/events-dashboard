#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/create-source.sh -s <source> -c <company> -i <city> -t <state> -n <country> -j <tableSchema-json-or-file>

Description:
  Creates a source record by calling POST /source with an access JWT.
  All source fields are required and must be passed as options.
  The table schema can be provided either as an inline JSON array or as a path
  to a file containing that JSON.

Arguments:
  -s source       Source name, for example "Stripe"
  -c company      Company name, for example "Acme"
  -i city         City name, for example "Boston"
  -t state        State or region name, for example "Massachusetts"
  -n country      Country name, for example "United States"
  -j tableSchema  Inline JSON array or a file path to JSON

Environment:
  ACCESS_JWT    Required. Access JWT used to authenticate POST /source.
  API_BASE_URL  Optional. Defaults to http://127.0.0.1:${APP_PORT:-3000}

Example:
  scripts/create-source.sh \
    -s "Stripe" \
    -c "Acme" \
    -i "Boston" \
    -t "Massachusetts" \
    -n "United States" \
    -j '[{"name":"invoice_number","type":"text","required":true},{"name":"amount","type":"numeric","required":false}]'

Example using a file:
  scripts/create-source.sh \
    -s "Stripe" \
    -c "Acme" \
    -i "Boston" \
    -t "Massachusetts" \
    -n "United States" \
    -j /path/to/table-schema.json
EOF
}

SOURCE_NAME=""
COMPANY_NAME=""
CITY_NAME=""
STATE_NAME=""
COUNTRY_NAME=""
TABLE_SCHEMA_INPUT=""

while getopts ":s:c:i:t:n:j:h" opt; do
  case "${opt}" in
    s)
      SOURCE_NAME="${OPTARG}"
      ;;
    c)
      COMPANY_NAME="${OPTARG}"
      ;;
    i)
      CITY_NAME="${OPTARG}"
      ;;
    t)
      STATE_NAME="${OPTARG}"
      ;;
    n)
      COUNTRY_NAME="${OPTARG}"
      ;;
    j)
      TABLE_SCHEMA_INPUT="${OPTARG}"
      ;;
    h)
      usage
      exit 0
      ;;
    :)
      echo "option -${OPTARG} requires a value" >&2
      usage >&2
      exit 1
      ;;
    \?)
      echo "invalid option: -${OPTARG}" >&2
      usage >&2
      exit 1
      ;;
  esac
done

shift $((OPTIND - 1))

if [[ $# -ne 0 ]]; then
  usage >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
DOCKER_ENV_FILE="${ROOT_DIR}/.env"

if [[ -f "${DOCKER_ENV_FILE}" ]]; then
  set -a
  # shellcheck disable=SC1090
  . "${DOCKER_ENV_FILE}"
  set +a
fi

APP_PORT="${APP_PORT:-3000}"
API_BASE_URL="${API_BASE_URL:-http://127.0.0.1:${APP_PORT}}"

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

require_command curl
require_command jq

if [[ -z "${ACCESS_JWT:-}" ]]; then
  echo "ACCESS_JWT is required" >&2
  exit 1
fi

if [[ -z "$(printf '%s' "${SOURCE_NAME}" | tr -d '[:space:]')" ]]; then
  echo "source is required" >&2
  exit 1
fi

if [[ -z "$(printf '%s' "${COMPANY_NAME}" | tr -d '[:space:]')" ]]; then
  echo "company is required" >&2
  exit 1
fi

if [[ -z "$(printf '%s' "${CITY_NAME}" | tr -d '[:space:]')" ]]; then
  echo "city is required" >&2
  exit 1
fi

if [[ -z "$(printf '%s' "${STATE_NAME}" | tr -d '[:space:]')" ]]; then
  echo "state is required" >&2
  exit 1
fi

if [[ -z "$(printf '%s' "${COUNTRY_NAME}" | tr -d '[:space:]')" ]]; then
  echo "country is required" >&2
  exit 1
fi

if [[ -z "$(printf '%s' "${TABLE_SCHEMA_INPUT}" | tr -d '[:space:]')" ]]; then
  echo "tableSchema is required" >&2
  exit 1
fi

if [[ -f "${TABLE_SCHEMA_INPUT}" ]]; then
  TABLE_SCHEMA_JSON="$(cat "${TABLE_SCHEMA_INPUT}")"
else
  TABLE_SCHEMA_JSON="${TABLE_SCHEMA_INPUT}"
fi

if ! printf '%s' "${TABLE_SCHEMA_JSON}" | jq -e 'type == "array" and length > 0' >/dev/null; then
  echo "tableSchema must be a non-empty JSON array" >&2
  exit 1
fi

REQUEST_BODY="$(
  jq -cn \
    --arg source "${SOURCE_NAME}" \
    --arg company "${COMPANY_NAME}" \
    --arg city "${CITY_NAME}" \
    --arg state "${STATE_NAME}" \
    --arg country "${COUNTRY_NAME}" \
    --argjson tableSchema "${TABLE_SCHEMA_JSON}" \
    '{
      source: $source,
      company: $company,
      city: $city,
      state: $state,
      country: $country,
      tableSchema: $tableSchema
    }'
)"

curl -fsS \
  -X POST \
  -H "Authorization: Bearer ${ACCESS_JWT}" \
  -H "Content-Type: application/json" \
  -d "${REQUEST_BODY}" \
  "${API_BASE_URL}/source"
