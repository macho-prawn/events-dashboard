#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/generate-jwt.sh -t access
  scripts/generate-jwt.sh -t ingestion

Description:
  Reads JWT configuration from the api_key_access table and prints the requested JWT.
EOF
}

TOKEN_KIND=""
while getopts ":t:h" opt; do
  case "${opt}" in
    t)
      TOKEN_KIND="${OPTARG}"
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

if [[ $# -ne 0 || -z "${TOKEN_KIND}" ]]; then
  usage >&2
  exit 1
fi

case "${TOKEN_KIND}" in
  access|ingestion)
    ;;
  *)
    echo "invalid token type: ${TOKEN_KIND}" >&2
    usage >&2
    exit 1
    ;;
esac

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_FILE="${ROOT_DIR}/docker-compose.yml"
ENV_FILE="${ROOT_DIR}/.env"
COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-$(basename "${ROOT_DIR}")}"

if [[ -f "${ENV_FILE}" ]]; then
  set -a
  # shellcheck disable=SC1091
  . "${ENV_FILE}"
  set +a
fi

DB_USER="${DB_USER:-events}"
DB_NAME="${DB_NAME:-events}"
DB_UID="${DB_UID:-70}"
DB_GID="${DB_GID:-70}"
BACKEND_PORT="${BACKEND_PORT:-3000}"
API_BASE_URL="${API_BASE_URL:-http://127.0.0.1:${BACKEND_PORT}}"

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

require_command docker
require_command curl
require_command jq
require_command openssl

query_config() {
  docker compose -f "${COMPOSE_FILE}" --env-file "${ENV_FILE}" -p "${COMPOSE_PROJECT_NAME}" exec -T -u "${DB_UID}:${DB_GID}" events-dashboard-db \
    sh -lc "psql -U '${DB_USER}' -d '${DB_NAME}' -At -F '|' -c \"select signing_secret, issuer, subject, ingestion_signing_secret, ingestion_issuer, ingestion_subject, ingestion_ttl_seconds from api_key_access where id = 1;\""
}

b64url() {
  openssl base64 -A | tr '+/' '-_' | tr -d '='
}

sign_hs256() {
  local unsigned="$1"
  local secret="$2"
  printf '%s' "$unsigned" | openssl dgst -binary -sha256 -hmac "$secret" | b64url
}

build_jwt() {
  local payload_json="$1"
  local secret="$2"
  local header_json='{"alg":"HS256","typ":"JWT"}'
  local unsigned
  unsigned="$(printf '%s' "$header_json" | b64url).$(printf '%s' "$payload_json" | b64url)"
  printf '%s.%s\n' "$unsigned" "$(sign_hs256 "$unsigned" "$secret")"
}

build_access_jwt() {
  if [[ -z "${ACCESS_SECRET}" || -z "${ACCESS_ISSUER}" || -z "${ACCESS_SUBJECT}" ]]; then
    echo "access JWT config is incomplete in api_key_access" >&2
    exit 1
  fi

  local access_payload
  access_payload="$(jq -cn \
    --arg iss "$ACCESS_ISSUER" \
    --arg sub "$ACCESS_SUBJECT" \
    '{iss: $iss, sub: $sub}')"
  build_jwt "$access_payload" "$ACCESS_SECRET"
}

CONFIG_ROW="$(query_config)"
if [[ -z "${CONFIG_ROW}" ]]; then
  echo "api_key_access row id=1 not found" >&2
  exit 1
fi

IFS='|' read -r ACCESS_SECRET ACCESS_ISSUER ACCESS_SUBJECT INGESTION_SECRET INGESTION_ISSUER INGESTION_SUBJECT INGESTION_TTL <<<"${CONFIG_ROW}"

case "$TOKEN_KIND" in
  access)
    build_access_jwt
    ;;
  ingestion)
    ACCESS_JWT="$(build_access_jwt)"
    curl -fsS \
      -H "Authorization: Bearer ${ACCESS_JWT}" \
      "${API_BASE_URL}/api-key" | jq -re '.apiKey'
    ;;
esac
