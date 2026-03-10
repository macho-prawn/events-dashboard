#!/usr/bin/env python3

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path


ROOT = Path(__file__).resolve().parent
SOURCE_DIR = ROOT / "source"
EVENT_DIR = ROOT / "events"
SOURCE_NAME = "Events"


@dataclass(frozen=True)
class Company:
    company: str
    event_slug: str
    city: str
    state: str
    country: str
    currency: str
    domain: str
    venues: tuple[str, ...]
    promoter_prefix: str


COMPANIES = [
    Company(
        company="Eventbrite",
        event_slug="eventbrite",
        city="San Francisco",
        state="California",
        country="United States",
        currency="USD",
        domain="eventbrite.com",
        venues=("The Midway", "Warfield Theatre", "Pier 48 Pavilion"),
        promoter_prefix="Bay Area Live",
    ),
    Company(
        company="Dice",
        event_slug="dice",
        city="London",
        state="England",
        country="United Kingdom",
        currency="GBP",
        domain="dice.fm",
        venues=("Roundhouse", "Brixton Electric", "Village Underground"),
        promoter_prefix="City Nights",
    ),
    Company(
        company="Bookmyshow",
        event_slug="bookmyshow",
        city="Mumbai",
        state="Maharashtra",
        country="India",
        currency="INR",
        domain="bookmyshow.com",
        venues=("Jio World Garden", "NCPA Mumbai", "Phoenix Arena"),
        promoter_prefix="Mumbai Stage",
    ),
    Company(
        company="Sistic",
        event_slug="sistic",
        city="Singapore",
        state="South East",
        country="Singapore",
        currency="SGD",
        domain="sistic.com.sg",
        venues=("Esplanade Theatre", "Singapore Indoor Stadium", "The Star Theatre"),
        promoter_prefix="Lion City Events",
    ),
    Company(
        company="Moshtix",
        event_slug="moshtix",
        city="Sydney",
        state="New South Wales",
        country="Australia",
        currency="AUD",
        domain="moshtix.com.au",
        venues=("Hordern Pavilion", "Enmore Theatre", "The Domain"),
        promoter_prefix="Harbour Sessions",
    ),
    Company(
        company="Eventim",
        event_slug="eventim",
        city="Bremen",
        state="Bremen",
        country="Germany",
        currency="EUR",
        domain="eventim.de",
        venues=("ÖVB Arena", "Pier 2", "Metropol Theater"),
        promoter_prefix="Nordic Pulse",
    ),
]

CATEGORIES = ("concert", "sports", "theater", "comedy", "festival", "family")
EVENT_THEMES = (
    "Summer Lights",
    "City Sessions",
    "Open Air Nights",
    "Weekend Showcase",
    "Arena Spotlight",
    "Late Night Club",
)
PERFORMER_FIRST_NAMES = (
    "Maya",
    "Noah",
    "Aria",
    "Leo",
    "Sana",
    "Kai",
    "Ivy",
    "Rohan",
)
PERFORMER_LAST_NAMES = (
    "Patel",
    "Ng",
    "Schmidt",
    "Turner",
    "Ito",
    "Khan",
    "Miller",
    "Costa",
)
AVAILABILITY_STATUSES = ("onsale", "low_inventory", "waitlist", "sold_out")


def main() -> None:
    EVENT_DIR.mkdir(parents=True, exist_ok=True)

    for company in COMPANIES:
        write_text(EVENT_DIR / f"{company.event_slug}.sh", render_event_script(company))
        write_json(EVENT_DIR / f"{company.event_slug}_events.json", generate_events(company))


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
    base_time = datetime(2025, 10, 3, 18, 0, tzinfo=UTC)
    events: list[dict[str, object]] = []

    for index in range(1, 1001):
        category = CATEGORIES[index % len(CATEGORIES)]
        theme = EVENT_THEMES[(index // 2) % len(EVENT_THEMES)]
        venue_name = company.venues[index % len(company.venues)]
        starts_at = base_time + timedelta(hours=18 * index)
        duration_hours = 2 + (index % 4)
        ends_at = starts_at + timedelta(hours=duration_hours)
        inventory_count = max(0, 2400 - (index * 2) - ((index % 7) * 9))
        availability_status = pick_availability(index, inventory_count)
        is_sold_out = availability_status == "sold_out"
        if is_sold_out:
            inventory_count = 0

        base_price = price_for(company.currency, category, index)
        performers = build_performers(company, index, category)
        event_id = f"{company.event_slug}-{index:06d}"

        events.append(
            {
                "source": SOURCE_NAME,
                "company": company.company,
                "city": company.city,
                "state": company.state,
                "country": company.country,
                "payload": {
                    "event_id": event_id,
                    "event_name": f"{theme} {title_case(category)} {index:04d}",
                    "event_category": category,
                    "venue_name": venue_name,
                    "venue_city": company.city,
                    "venue_country": company.country,
                    "starts_at": format_timestamp(starts_at),
                    "ends_at": format_timestamp(ends_at),
                    "ticket_currency": company.currency,
                    "base_ticket_price": base_price,
                    "inventory_count": inventory_count,
                    "availability_status": availability_status,
                    "is_sold_out": is_sold_out,
                    "performers": performers,
                    "event_metadata": {
                        "ticket_url": f"https://{company.domain}/events/{event_id}",
                        "promoter": f"{company.promoter_prefix} {title_case(category)}",
                        "age_restriction": "18+" if category in {"concert", "comedy"} and index % 5 == 0 else "all_ages",
                        "seat_map": "reserved" if category in {"sports", "theater"} else "general_admission",
                        "synthetic_seed": event_id,
                    },
                },
            }
        )

    return events


def pick_availability(index: int, inventory_count: int) -> str:
    if inventory_count == 0 or index % 11 == 0:
        return "sold_out"
    if inventory_count < 150 or index % 5 == 0:
        return "low_inventory"
    if index % 9 == 0:
        return "waitlist"
    return "onsale"


def build_performers(company: Company, index: int, category: str) -> list[dict[str, str]]:
    count = 1 if category in {"sports", "comedy"} else 2 + (index % 2)
    performers = []
    for offset in range(count):
        first_name = PERFORMER_FIRST_NAMES[(index + offset) % len(PERFORMER_FIRST_NAMES)]
        last_name = PERFORMER_LAST_NAMES[(index + offset * 2) % len(PERFORMER_LAST_NAMES)]
        performers.append(
            {
                "name": f"{first_name} {last_name}",
                "role": performer_role(category, offset),
                "billing_order": str(offset + 1),
                "home_market": company.city,
            }
        )
    return performers


def performer_role(category: str, offset: int) -> str:
    if category == "sports":
        return "home_team" if offset == 0 else "away_team"
    if category == "theater":
        return "cast"
    if category == "comedy":
        return "headliner" if offset == 0 else "support"
    if category == "festival":
        return "featured_artist"
    return "artist"


def price_for(currency: str, category: str, index: int) -> float:
    base_prices = {
        "USD": {"concert": 59.0, "sports": 72.0, "theater": 81.0, "comedy": 44.0, "festival": 95.0, "family": 35.0},
        "GBP": {"concert": 42.0, "sports": 56.0, "theater": 61.0, "comedy": 29.0, "festival": 74.0, "family": 24.0},
        "INR": {"concert": 1800.0, "sports": 2400.0, "theater": 2100.0, "comedy": 1200.0, "festival": 3200.0, "family": 900.0},
        "SGD": {"concert": 78.0, "sports": 92.0, "theater": 88.0, "comedy": 54.0, "festival": 128.0, "family": 40.0},
        "AUD": {"concert": 69.0, "sports": 85.0, "theater": 79.0, "comedy": 45.0, "festival": 115.0, "family": 38.0},
        "EUR": {"concert": 49.0, "sports": 66.0, "theater": 58.0, "comedy": 34.0, "festival": 90.0, "family": 27.0},
    }
    return round(base_prices[currency][category] + (index % 8) * 3.5, 2)


def title_case(value: str) -> str:
    return value.replace("_", " ").title()


def format_timestamp(value: datetime) -> str:
    return value.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def write_json(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def write_text(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


if __name__ == "__main__":
    main()
