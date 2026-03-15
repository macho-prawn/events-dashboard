#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/list-records.sh -s <source> -c <company> [-i <city>] [-t <state>] [-n <country>] [-q <query>] [-f <format>] [-p <page>] [-d <documents>] [--no-header]
  scripts/list-records.sh -s <source> [-f <format>] [--no-header]

Description:
  Lists all records for a source/company pair by paging through GET /search,
  or probes GET /search to list known companies and record counts for a source when company is omitted.

Arguments:
  source   Source name, for example "News"
  company  Company name, for example "BBC". If omitted, lists companies and record counts for the source
  city     Optional city filter, for example "London"
  state    Optional state filter, for example "England"
  country  Optional country filter, for example "United Kingdom"
  query    Optional text search term
  format   Optional output format: json, csv, or tsv. Defaults to json
  page     Optional 1-based page number. When set, fetches only that page
  documents Optional client-side limit for documents emitted from each fetched page
  --no-header  Optional. Omit the header row for csv/tsv output

Environment:
  INGESTION_JWT  Required for both record search mode and source-only company probes.
  API_BASE_URL   Optional. Defaults to http://127.0.0.1:${APP_PORT:-3000}

Example:
  INGESTION_JWT="$(scripts/generate-jwt.sh -t ingestion)" \
    scripts/list-records.sh -s "News" -c "BBC" -i "London" -t "England" -n "United Kingdom" -q "economy" -f csv -p 2 -d 10 --no-header

Example source-only:
  INGESTION_JWT="$(scripts/generate-jwt.sh -t ingestion)" \
    scripts/list-records.sh -s "News" -f tsv --no-header
EOF
}

SOURCE_NAME=""
COMPANY_NAME=""
CITY_NAME=""
STATE_NAME=""
COUNTRY_NAME=""
QUERY_TEXT=""
OUTPUT_FORMAT="json"
REQUESTED_PAGE=""
DOCUMENT_LIMIT=""
OMIT_HEADER=false

normalized_args=()
for arg in "$@"; do
  case "${arg}" in
    --no-header)
      normalized_args+=("-H")
      ;;
    --help)
      normalized_args+=("-h")
      ;;
    *)
      normalized_args+=("${arg}")
      ;;
  esac
done

set -- "${normalized_args[@]}"

while getopts ":s:c:i:t:n:q:f:p:d:Hh" opt; do
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
    q)
      QUERY_TEXT="${OPTARG}"
      ;;
    f)
      OUTPUT_FORMAT="${OPTARG}"
      ;;
    p)
      REQUESTED_PAGE="${OPTARG}"
      ;;
    d)
      DOCUMENT_LIMIT="${OPTARG}"
      ;;
    H)
      OMIT_HEADER=true
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

extract_known_companies() {
  local source_dir=""
  local source_file=""
  local source_key=""

  source_key="$(printf '%s' "${SOURCE_NAME}" | tr '[:upper:]' '[:lower:]')"
  case "${source_key}" in
    news|events|flights)
      source_dir="${SCRIPT_DIR}/${source_key}/source"
      ;;
    ecommerce)
      source_dir="${SCRIPT_DIR}/ecommerce/source"
      ;;
    *)
      return 0
      ;;
  esac

  if [[ ! -d "${source_dir}" ]]; then
    return 0
  fi

  {
    while IFS= read -r -d '' source_file; do
      awk -F'"' '/-[[:space:]]*c[[:space:]]+"/ { print $2; next }' "${source_file}"
    done < <(find "${source_dir}" -maxdepth 1 -type f -name '*.sh' -print0)
  } | awk 'NF > 0' | sort -u
}

require_positive_integer() {
  local value="$1"
  local name="$2"

  if [[ ! "${value}" =~ ^[1-9][0-9]*$ ]]; then
    echo "${name} must be a positive integer" >&2
    exit 1
  fi
}

if [[ -z "$(printf '%s' "${SOURCE_NAME}" | tr -d '[:space:]')" ]]; then
  echo "source is required" >&2
  exit 1
fi

case "${OUTPUT_FORMAT}" in
  json|csv|tsv)
    ;;
  *)
    echo "format must be one of: json, csv, tsv" >&2
    exit 1
    ;;
esac

if [[ -n "${REQUESTED_PAGE}" ]]; then
  require_positive_integer "${REQUESTED_PAGE}" "page"
fi

if [[ -n "${DOCUMENT_LIMIT}" ]]; then
  require_positive_integer "${DOCUMENT_LIMIT}" "documents"
fi

source_only_mode=false

if [[ -z "$(printf '%s' "${COMPANY_NAME}" | tr -d '[:space:]')" ]]; then
  source_only_mode=true
fi

if [[ -z "${INGESTION_JWT:-}" ]]; then
  echo "INGESTION_JWT is required" >&2
  exit 1
fi

if [[ "${source_only_mode}" == true ]]; then
  if [[ -n "${CITY_NAME}" || -n "${STATE_NAME}" || -n "${COUNTRY_NAME}" || -n "${QUERY_TEXT}" || -n "${REQUESTED_PAGE}" || -n "${DOCUMENT_LIMIT}" ]]; then
    echo "city, state, country, query, page, and documents options require company" >&2
    exit 1
  fi
fi

response_file="$(mktemp)"
all_results_file="$(mktemp)"
companies_file="$(mktemp)"
trap 'rm -f "${response_file}" "${all_results_file}" "${companies_file}"' EXIT

printf '[]' >"${all_results_file}"

if [[ "${source_only_mode}" == true ]]; then
  while IFS= read -r company_candidate; do
    if [[ -z "${company_candidate}" ]]; then
      continue
    fi

    request_url="$(
      jq -rn \
        --arg api_base_url "${API_BASE_URL}" \
        --arg source "${SOURCE_NAME}" \
        --arg company "${company_candidate}" \
        '$api_base_url + "/search?source=" + ($source | @uri) + "&company=" + ($company | @uri) + "&page=1"'
    )"

    if ! http_code="$(
      curl -sS \
        -o "${response_file}" \
        -w '%{http_code}' \
        -H "Authorization: Bearer ${INGESTION_JWT}" \
        "${request_url}"
    )"; then
      echo "request failed before receiving an HTTP response" >&2
      exit 1
    fi

    case "${http_code}" in
      200)
        jq -cn \
          --arg company "${company_candidate}" \
          --argjson recordCount "$(jq -r '.total' "${response_file}")" \
          '{company: $company, recordCount: $recordCount}' >>"${companies_file}"
        ;;
      404)
        ;;
      *)
        printf 'request failed with HTTP %s\n' "${http_code}" >&2
        cat "${response_file}" >&2
        exit 1
        ;;
    esac
  done < <(extract_known_companies)

  jq -s '
    unique_by(.company)
    | sort_by(.company)
  ' "${companies_file}" >"${all_results_file}"

  if [[ "${OUTPUT_FORMAT}" == "json" ]]; then
    cat "${all_results_file}"
    exit 0
  fi

  jq -r --arg format "${OUTPUT_FORMAT}" --argjson omit_header "${OMIT_HEADER}" '
    (if $omit_header then empty else (["company", "recordCount"] | if $format == "csv" then @csv else @tsv end) end),
    (.[] | [.company, .recordCount] | if $format == "csv" then @csv else @tsv end)
  ' "${all_results_file}"
  exit 0
fi

page="${REQUESTED_PAGE:-1}"
page_size=0
total=0
fetched=0
single_page_mode=false

if [[ -n "${REQUESTED_PAGE}" ]]; then
  single_page_mode=true
fi

while :; do
  request_url="$(
    jq -rn \
      --arg api_base_url "${API_BASE_URL}" \
      --arg source "${SOURCE_NAME}" \
      --arg company "${COMPANY_NAME}" \
      --arg city "${CITY_NAME}" \
      --arg state "${STATE_NAME}" \
      --arg country "${COUNTRY_NAME}" \
      --arg query "${QUERY_TEXT}" \
      --arg page "${page}" \
      '
        [
          "source=" + ($source | @uri),
          "company=" + ($company | @uri),
          (if $city == "" then empty else "city=" + ($city | @uri) end),
          (if $state == "" then empty else "state=" + ($state | @uri) end),
          (if $country == "" then empty else "country=" + ($country | @uri) end),
          (if $query == "" then empty else "q=" + ($query | @uri) end),
          "page=" + $page
        ]
        | $api_base_url + "/search?" + join("&")
      '
  )"

  if ! http_code="$(
    curl -sS \
      -o "${response_file}" \
      -w '%{http_code}' \
      -H "Authorization: Bearer ${INGESTION_JWT}" \
      "${request_url}"
  )"; then
    echo "request failed before receiving an HTTP response" >&2
    exit 1
  fi

  if [[ "${http_code}" != "200" ]]; then
    printf 'request failed with HTTP %s\n' "${http_code}" >&2
    cat "${response_file}" >&2
    exit 1
  fi

  if ! jq -e '.results and (.results | type == "array") and (.page | type == "number") and (.pageSize | type == "number") and (.total | type == "number")' "${response_file}" >/dev/null; then
    echo "unexpected response shape from /search" >&2
    cat "${response_file}" >&2
    exit 1
  fi

  page_size="$(jq -r '.pageSize' "${response_file}")"
  total="$(jq -r '.total' "${response_file}")"
  result_count="$(jq -r '.results | length' "${response_file}")"

  if [[ -n "${DOCUMENT_LIMIT}" ]]; then
    jq --argjson limit "${DOCUMENT_LIMIT}" '.results |= .[:$limit]' "${response_file}" >"${response_file}.tmp"
    mv "${response_file}.tmp" "${response_file}"
  fi

  jq -s '.[0] + .[1].results' "${all_results_file}" "${response_file}" >"${all_results_file}.tmp"
  mv "${all_results_file}.tmp" "${all_results_file}"

  fetched=$((fetched + result_count))

  if [[ "${single_page_mode}" == true ]] || (( result_count == 0 || fetched >= total || result_count < page_size )); then
    break
  fi

  page=$((page + 1))
done

if [[ "${OUTPUT_FORMAT}" == "json" ]]; then
  cat "${all_results_file}"
  exit 0
fi

jq -r --arg format "${OUTPUT_FORMAT}" --argjson omit_header "${OMIT_HEADER}" '
  def base_columns:
    ["id", "sourceParentId", "source", "company", "city", "state", "country", "createdAt"];
  def payload_columns:
    (map((.payload // {}) | keys_unsorted) | add // [] | unique | sort);
  def flat_record:
    {
      id: .id,
      sourceParentId: .sourceParentId,
      source: .source,
      company: .company,
      city: .city,
      state: .state,
      country: .country,
      createdAt: .createdAt
    } + (.payload // {});
  def normalize_cell($value):
    if $value == null then
      ""
    elif ($value | type) == "array" or ($value | type) == "object" then
      ($value | tojson)
    else
      $value
    end;
  (base_columns + payload_columns) as $columns
  | (if $omit_header then empty else ($columns | if $format == "csv" then @csv else @tsv end) end),
    (.[] | flat_record | [$columns[] as $column | normalize_cell(.[ $column ])] | if $format == "csv" then @csv else @tsv end)
' "${all_results_file}"
