#!/usr/bin/env python3

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path


ROOT = Path(__file__).resolve().parent
SOURCE_DIR = ROOT / "source"
EVENT_DIR = ROOT / "events"
SOURCE_NAME = "Flights"

SCHEMA = [
    {"name": "flight_id", "type": "text", "required": True},
    {"name": "flight_number", "type": "text", "required": True},
    {"name": "carrier_name", "type": "text", "required": True},
    {"name": "carrier_iata", "type": "text", "required": True},
    {"name": "origin_iata", "type": "text", "required": True},
    {"name": "origin_city", "type": "text", "required": True},
    {"name": "origin_country", "type": "text", "required": True},
    {"name": "destination_iata", "type": "text", "required": True},
    {"name": "destination_city", "type": "text", "required": True},
    {"name": "destination_country", "type": "text", "required": True},
    {"name": "scheduled_departure_at", "type": "timestamptz", "required": True},
    {"name": "scheduled_arrival_at", "type": "timestamptz", "required": True},
    {"name": "actual_departure_at", "type": "timestamptz", "required": False},
    {"name": "actual_arrival_at", "type": "timestamptz", "required": False},
    {"name": "status", "type": "text", "required": True},
    {"name": "aircraft_type", "type": "text", "required": False},
    {"name": "terminal", "type": "text", "required": False},
    {"name": "gate", "type": "text", "required": False},
    {"name": "baggage_claim", "type": "text", "required": False},
    {"name": "is_codeshare", "type": "boolean", "required": False},
    {"name": "route_metadata", "type": "jsonb", "required": False},
]


@dataclass(frozen=True)
class Destination:
    iata: str
    city: str
    country: str
    distance_km: int
    service_type: str


@dataclass(frozen=True)
class Company:
    company: str
    source_slug: str
    event_slug: str
    city: str
    state: str
    country: str
    carrier_iata: str
    origin_iata: str
    aircraft_types: tuple[str, ...]
    destinations: tuple[Destination, ...]


COMPANIES = [
    Company("Delta Air Lines", "delta-air-lines", "delta_air_lines", "Atlanta", "Georgia", "United States", "DL", "ATL", ("Airbus A321", "Boeing 757-200", "Boeing 737-900"), (Destination("LHR", "London", "United Kingdom", 6764, "international"), Destination("JFK", "New York", "United States", 1222, "domestic"), Destination("MIA", "Miami", "United States", 964, "domestic"))),
    Company("Emirates", "emirates", "emirates", "Dubai", "Dubai Emirate", "United Arab Emirates", "EK", "DXB", ("Airbus A380", "Boeing 777-300ER", "Boeing 777-200LR"), (Destination("SIN", "Singapore", "Singapore", 5840, "international"), Destination("LHR", "London", "United Kingdom", 5500, "international"), Destination("SYD", "Sydney", "Australia", 12039, "international"))),
    Company("Qantas", "qantas", "qantas", "Sydney", "New South Wales", "Australia", "QF", "SYD", ("Boeing 737-800", "Airbus A330-200", "Boeing 787-9"), (Destination("SIN", "Singapore", "Singapore", 6308, "international"), Destination("MEL", "Melbourne", "Australia", 714, "domestic"), Destination("AKL", "Auckland", "New Zealand", 2160, "international"))),
    Company("Singapore Airlines", "singapore-airlines", "singapore_airlines", "Singapore", "South East", "Singapore", "SQ", "SIN", ("Airbus A350-900", "Boeing 787-10", "Airbus A380"), (Destination("NRT", "Tokyo", "Japan", 5340, "international"), Destination("SYD", "Sydney", "Australia", 6308, "international"), Destination("BKK", "Bangkok", "Thailand", 1434, "international"))),
    Company("Southwest Airlines", "southwest-airlines", "southwest_airlines", "Dallas", "Texas", "United States", "WN", "DAL", ("Boeing 737-700", "Boeing 737 MAX 8", "Boeing 737-800"), (Destination("HOU", "Houston", "United States", 385, "domestic"), Destination("DEN", "Denver", "United States", 1066, "domestic"), Destination("PHX", "Phoenix", "United States", 1415, "domestic"))),
    Company("United Airlines", "united-airlines", "united_airlines", "Chicago", "Illinois", "United States", "UA", "ORD", ("Boeing 737 MAX 9", "Airbus A320", "Boeing 787-8"), (Destination("SFO", "San Francisco", "United States", 2974, "domestic"), Destination("EWR", "Newark", "United States", 1160, "domestic"), Destination("FRA", "Frankfurt", "Germany", 6972, "international"))),
]

STATUSES = ("scheduled", "boarding", "departed", "arrived", "delayed")
TERMINALS = ("1", "2", "3", "T1", "T2")
GATES = ("A1", "B2", "C3", "D4", "E5", "T1B2")


def main() -> None:
    SOURCE_DIR.mkdir(parents=True, exist_ok=True)
    EVENT_DIR.mkdir(parents=True, exist_ok=True)

    write_json(SOURCE_DIR / "table_schema.json", SCHEMA)

    for company in COMPANIES:
        write_text(SOURCE_DIR / f"{company.source_slug}.sh", render_source_script(company))
        write_text(EVENT_DIR / f"{company.event_slug}.sh", render_event_script(company))
        write_json(EVENT_DIR / f"{company.event_slug}_events.json", generate_events(company))


def render_source_script(company: Company) -> str:
    return f"""#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${{BASH_SOURCE[0]}}")" && pwd)"
ROOT_DIR="$(cd "${{SCRIPT_DIR}}/../../.." && pwd)"

ACCESS_JWT="${{ACCESS_JWT:?ACCESS_JWT is required}}" \\
  "${{ROOT_DIR}}/scripts/create-source.sh" \\
  -s "{SOURCE_NAME}" \\
  -c "{company.company}" \\
  -i "{company.city}" \\
  -t "{company.state}" \\
  -n "{company.country}" \\
  -j "${{SCRIPT_DIR}}/table_schema.json"
"""


def render_event_script(company: Company) -> str:
    label = company.event_slug
    events_file = f"{company.event_slug}_events.json"
    return f"""#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${{BASH_SOURCE[0]}}")" && pwd)"
ROOT_DIR="$(cd "${{SCRIPT_DIR}}/../../.." && pwd)"
DOCKER_ENV_FILE="${{ROOT_DIR}}/.env"
EVENTS_FILE="${{SCRIPT_DIR}}/{events_file}"

if [[ -f "${{DOCKER_ENV_FILE}}" ]]; then
  set -a
  # shellcheck disable=SC1090
  . "${{DOCKER_ENV_FILE}}"
  set +a
fi

APP_PORT="${{APP_PORT:-3000}}"
API_BASE_URL="${{API_BASE_URL:-http://127.0.0.1:${{APP_PORT}}}}"
INGESTION_JWT="${{INGESTION_JWT:?INGESTION_JWT is required}}"

require_command() {{
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}}

require_command curl
require_command jq

response_file="$(mktemp)"
trap 'rm -f "${{response_file}}"' EXIT

created_count=0
duplicate_count=0
processed_count=0
while IFS= read -r event_json; do
  if ! http_code="$(
    curl -sS \\
      -o "${{response_file}}" \\
      -w '%{{http_code}}' \\
      -X POST \\
      -H "Authorization: Bearer ${{INGESTION_JWT}}" \\
      -H "Content-Type: application/json" \\
      -d "${{event_json}}" \\
      "${{API_BASE_URL}}/events"
  )"; then
    echo "{label}: request failed before receiving an HTTP response" >&2
    exit 1
  fi

  processed_count=$((processed_count + 1))
  case "${{http_code}}" in
    200|201)
      created_count=$((created_count + 1))
      ;;
    409)
      duplicate_count=$((duplicate_count + 1))
      ;;
    *)
      printf '{label}: request %d failed with HTTP %s\\n' "${{processed_count}}" "${{http_code}}" >&2
      cat "${{response_file}}" >&2
      exit 1
      ;;
  esac

  if (( processed_count % 100 == 0 )); then
    printf '{label}: processed %d events (%d created, %d duplicates)\\n' "${{processed_count}}" "${{created_count}}" "${{duplicate_count}}"
  fi
done < <(jq -c '.[]' "${{EVENTS_FILE}}")

printf '{label}: processed %d events (%d created, %d duplicates)\\n' "${{processed_count}}" "${{created_count}}" "${{duplicate_count}}"

if (( duplicate_count > 0 )); then
  printf '{label}: duplicate events detected in %d documents\\n' "${{duplicate_count}}" >&2
  exit 1
fi
"""


def generate_events(company: Company) -> list[dict[str, object]]:
    base_time = datetime(2025, 8, 13, 2, 59, tzinfo=UTC)
    records = []
    prefix = company.event_slug

    for index in range(1, 1001):
        destination = company.destinations[index % len(company.destinations)]
        scheduled_departure = base_time + timedelta(hours=9 * index)
        duration_minutes = 85 + (index % 6) * 28 + destination.distance_km // 120
        scheduled_arrival = scheduled_departure + timedelta(minutes=duration_minutes)
        status = STATUSES[index % len(STATUSES)]
        delay_minutes = 0 if status in {"scheduled", "arrived"} and index % 4 == 0 else (index % 5) * 10
        actual_departure = scheduled_departure + timedelta(minutes=delay_minutes if status != "scheduled" else 0)
        actual_arrival = scheduled_arrival + timedelta(minutes=max(delay_minutes - 5, 0) if status in {"departed", "arrived", "delayed"} else 0)

        records.append(
            {
                "source": SOURCE_NAME,
                "company": company.company,
                "city": company.city,
                "state": company.state,
                "country": company.country,
                "payload": {
                    "flight_id": f"{prefix}-{index:06d}",
                    "flight_number": f"{company.carrier_iata}{1000 + index}",
                    "carrier_name": company.company,
                    "carrier_iata": company.carrier_iata,
                    "origin_iata": company.origin_iata,
                    "origin_city": company.city,
                    "origin_country": company.country,
                    "destination_iata": destination.iata,
                    "destination_city": destination.city,
                    "destination_country": destination.country,
                    "scheduled_departure_at": format_timestamp(scheduled_departure),
                    "scheduled_arrival_at": format_timestamp(scheduled_arrival),
                    "actual_departure_at": format_timestamp(actual_departure),
                    "actual_arrival_at": format_timestamp(actual_arrival),
                    "status": status,
                    "aircraft_type": company.aircraft_types[index % len(company.aircraft_types)],
                    "terminal": TERMINALS[index % len(TERMINALS)],
                    "gate": GATES[index % len(GATES)],
                    "baggage_claim": f"B{(index % 8) + 1}",
                    "is_codeshare": index % 6 == 0,
                    "route_metadata": {
                        "distance_km": destination.distance_km,
                        "delay_minutes": delay_minutes,
                        "service_type": destination.service_type,
                        "synthetic_seed": f"{prefix}-{index:06d}",
                    },
                },
            }
        )

    return records


def format_timestamp(value: datetime) -> str:
    return value.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def write_json(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def write_text(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


if __name__ == "__main__":
    main()
