#!/usr/bin/env python3

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path


ROOT = Path(__file__).resolve().parent
SOURCE_DIR = ROOT / "source"
EVENT_DIR = ROOT / "events"
SOURCE_NAME = "News"

SCHEMA = [
    {"name": "article_id", "type": "text", "required": True},
    {"name": "headline", "type": "text", "required": True},
    {"name": "summary", "type": "text", "required": False},
    {"name": "article_url", "type": "text", "required": True},
    {"name": "canonical_url", "type": "text", "required": False},
    {"name": "publisher_name", "type": "text", "required": True},
    {"name": "publisher_domain", "type": "text", "required": True},
    {"name": "section", "type": "text", "required": False},
    {"name": "author_name", "type": "text", "required": False},
    {"name": "language", "type": "text", "required": False},
    {"name": "image_url", "type": "text", "required": False},
    {"name": "published_at", "type": "timestamptz", "required": True},
    {"name": "updated_at", "type": "timestamptz", "required": False},
    {"name": "is_breaking", "type": "boolean", "required": False},
    {"name": "tags", "type": "jsonb", "required": False},
    {"name": "entities", "type": "jsonb", "required": False},
    {"name": "raw_metadata", "type": "jsonb", "required": False},
]


@dataclass(frozen=True)
class Company:
    company: str
    source_slug: str
    event_slug: str
    city: str
    state: str
    country: str
    domain: str
    language: str
    edition: str
    sections: tuple[str, ...]


COMPANIES = [
    Company("Africanews", "africanews", "africanews", "Chassieu, Lyon", "Auvergne-Rhône-Alpes", "France", "africanews.com", "en", "Sub-Saharan Africa", ("africa", "business", "culture")),
    Company("BBC", "bbc", "bbc", "London", "England", "United Kingdom", "bbc.com", "en-GB", "Europe", ("world", "business", "society")),
    Company("CNA", "cna", "cna", "Singapore", "South East", "Singapore", "channelnewsasia.com", "en", "Asia", ("asia", "business", "technology")),
    Company("Gestion", "gestion", "gestion", "Lima", "Lima Region", "Peru", "gestion.pe", "es", "Latin America", ("economia", "mercados", "empresas")),
    Company("SBS News", "sbs-news", "sbs_news", "Sydney", "New South Wales", "Australia", "sbs.com.au", "en-AU", "Oceania", ("world", "australia", "politics")),
    Company("The Indian Express", "the-indian-express", "the_indian_express", "Gautam Buddha Nagar", "Uttar Pradesh", "India", "indianexpress.com", "en-IN", "South Asia", ("india", "technology", "cities")),
]

ADJECTIVES = ("Market", "Regional", "Policy", "Consumer", "Technology", "Election", "Travel", "Climate")
NOUNS = ("outlook", "momentum", "coverage", "strategy", "demand", "planning", "investment", "rebound")
AUTHORS = ("Amelia Hart", "Daniel Kim", "Nia Okafor", "Priya Shah", "Lucas Meyer", "Sofia Chen")
ENTITY_ORGS = ("UNESCO", "UNICEF", "World Bank", "WHO", "OECD", "ASEAN")
ENTITY_PEOPLE = ("Amina Rahman", "Daniel Kim", "Priya Shah", "Kenji Watanabe", "Sofia Chen", "Lucas Meyer")


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
  -w "{company.domain}" \\
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
    base_time = datetime(2025, 5, 25, 4, 59, tzinfo=UTC)
    records = []
    prefix = company.event_slug

    for index in range(1, 1001):
        article_id = f"{prefix}-{index:06d}"
        section = company.sections[index % len(company.sections)]
        published_at = base_time + timedelta(hours=11 * index)
        updated_at = published_at + timedelta(minutes=31 + (index % 70))
        adjective = ADJECTIVES[index % len(ADJECTIVES)]
        noun = NOUNS[(index // 2) % len(NOUNS)]
        author = AUTHORS[index % len(AUTHORS)]
        content_type = "breaking" if index % 19 == 0 else "analysis" if index % 3 else "feature"
        is_breaking = content_type == "breaking"
        article_url = build_article_url(company, section, published_at, article_id)

        records.append(
            {
                "source": SOURCE_NAME,
                "company": company.company,
                "city": company.city,
                "state": company.state,
                "country": company.country,
                "payload": {
                    "article_id": article_id,
                    "headline": f"{adjective} {noun} shifts as {section} coverage evolves",
                    "summary": f"{company.company} tracks how {section} and audience trends are shaping coverage for readers across {company.edition}.",
                    "article_url": article_url,
                    "canonical_url": article_url,
                    "publisher_name": company.company,
                    "publisher_domain": company.domain,
                    "section": section,
                    "author_name": author,
                    "language": company.language,
                    "image_url": f"https://images.{company.domain}/{prefix}/2025/{index:06d}.jpg",
                    "published_at": format_timestamp(published_at),
                    "updated_at": format_timestamp(updated_at),
                    "is_breaking": is_breaking,
                    "tags": [section, noun.lower(), company.edition.lower().replace(' ', '-')],
                    "entities": [
                        {"type": "person", "name": ENTITY_PEOPLE[index % len(ENTITY_PEOPLE)]},
                        {"type": "org", "name": ENTITY_ORGS[index % len(ENTITY_ORGS)]},
                    ],
                    "raw_metadata": {
                        "content_type": content_type,
                        "edition": company.edition,
                        "word_count": 500 + (index % 320),
                        "synthetic_seed": article_id,
                    },
                },
            }
        )

    return records


def build_article_url(company: Company, section: str, published_at: datetime, article_id: str) -> str:
    path_date = published_at.strftime("%Y/%m/%d")
    return f"https://www.{company.domain}/{section}/{path_date}/{article_id}.html"


def format_timestamp(value: datetime) -> str:
    return value.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def write_json(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def write_text(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


if __name__ == "__main__":
    main()
