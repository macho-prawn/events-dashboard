#!/usr/bin/env python3

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path


ROOT = Path(__file__).resolve().parent
SOURCE_DIR = ROOT / "source"
EVENT_DIR = ROOT / "events"
SOURCE_NAME = "ECommerce"

SCHEMA = [
    {"name": "order_id", "type": "text", "required": True},
    {"name": "order_number", "type": "text", "required": True},
    {"name": "storefront", "type": "text", "required": True},
    {"name": "customer_name", "type": "text", "required": True},
    {"name": "customer_email", "type": "text", "required": True},
    {"name": "currency", "type": "text", "required": True},
    {"name": "subtotal_amount", "type": "numeric", "required": True},
    {"name": "shipping_amount", "type": "numeric", "required": True},
    {"name": "tax_amount", "type": "numeric", "required": True},
    {"name": "discount_amount", "type": "numeric", "required": False},
    {"name": "total_amount", "type": "numeric", "required": True},
    {"name": "item_count", "type": "integer", "required": True},
    {"name": "payment_status", "type": "text", "required": True},
    {"name": "fulfillment_status", "type": "text", "required": True},
    {"name": "placed_at", "type": "timestamptz", "required": True},
    {"name": "updated_at", "type": "timestamptz", "required": False},
    {"name": "is_expedited", "type": "boolean", "required": False},
    {"name": "shipping_address", "type": "jsonb", "required": False},
    {"name": "line_items", "type": "jsonb", "required": False},
    {"name": "order_metadata", "type": "jsonb", "required": False},
]


@dataclass(frozen=True)
class Company:
    company: str
    source_slug: str
    event_slug: str
    city: str
    state: str
    country: str
    currency: str
    domain: str
    sales_channel: str
    postal_code: str


COMPANIES = [
    Company(
        company="Amazon",
        source_slug="amazon",
        event_slug="amazon",
        city="Seattle",
        state="Washington",
        country="United States",
        currency="USD",
        domain="amazon.com",
        sales_channel="marketplace",
        postal_code="98109",
    ),
    Company(
        company="Shopify",
        source_slug="shopify",
        event_slug="shopify",
        city="Ottawa",
        state="Ontario",
        country="Canada",
        currency="CAD",
        domain="shopify.com",
        sales_channel="shop_app",
        postal_code="K1N 5T5",
    ),
    Company(
        company="Ebay",
        source_slug="ebay",
        event_slug="ebay",
        city="San Jose",
        state="California",
        country="United States",
        currency="USD",
        domain="ebay.com",
        sales_channel="auctions",
        postal_code="95125",
    ),
    Company(
        company="Wayfair",
        source_slug="wayfair",
        event_slug="wayfair",
        city="Boston",
        state="Massachusetts",
        country="United States",
        currency="USD",
        domain="wayfair.com",
        sales_channel="direct",
        postal_code="02108",
    ),
    Company(
        company="Carousell",
        source_slug="carousell",
        event_slug="carousell",
        city="Singapore",
        state="South East",
        country="Singapore",
        currency="SGD",
        domain="carousell.com",
        sales_channel="classifieds_marketplace",
        postal_code="018956",
    ),
    Company(
        company="Target",
        source_slug="target",
        event_slug="target",
        city="Minneapolis",
        state="Minnesota",
        country="United States",
        currency="USD",
        domain="target.com",
        sales_channel="retail_store",
        postal_code="55403",
    ),
]

FIRST_NAMES = [
    "Avery",
    "Jordan",
    "Morgan",
    "Riley",
    "Taylor",
    "Harper",
    "Parker",
    "Rowan",
]
LAST_NAMES = [
    "Nguyen",
    "Patel",
    "Kim",
    "Garcia",
    "Smith",
    "Johnson",
    "Khan",
    "Lopez",
]
PAYMENT_STATUSES = ["paid", "captured", "authorized", "refunded"]
FULFILLMENT_STATUSES = ["processing", "picking", "shipped", "delivered"]
PRODUCTS = [
    ("home", "Accent Lamp", 39.0),
    ("apparel", "Cotton Hoodie", 54.0),
    ("beauty", "Skincare Set", 31.5),
    ("electronics", "Wireless Earbuds", 78.0),
    ("outdoors", "Trail Bottle", 24.0),
    ("office", "Desk Organizer", 18.0),
]


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
    file_name = f"{company.event_slug}_events.json"
    return f"""#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${{BASH_SOURCE[0]}}")" && pwd)"
ROOT_DIR="$(cd "${{SCRIPT_DIR}}/../../.." && pwd)"
DOCKER_ENV_FILE="${{ROOT_DIR}}/.env"
EVENTS_FILE="${{SCRIPT_DIR}}/{file_name}"

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
    base_time = datetime(2025, 9, 1, 9, 0, tzinfo=UTC)
    events: list[dict[str, object]] = []
    prefix = company.event_slug

    for index in range(1, 1001):
        order_id = f"{prefix}-{index:06d}"
        order_number = f"{prefix[:3].upper()}-{2025 + (index % 2)}-{index:06d}"
        customer_name = build_customer_name(index)
        customer_email = f"{prefix}-{index:06d}@example.test"
        placed_at = base_time + timedelta(minutes=47 * index)
        updated_at = placed_at + timedelta(hours=(index % 7) + 1)
        is_expedited = index % 5 == 0
        item_count = (index % 3) + 1
        discount_amount = round(5.0 + (index % 4) * 1.5, 2) if index % 4 == 0 else None
        payment_status = PAYMENT_STATUSES[index % len(PAYMENT_STATUSES)]
        fulfillment_status = FULFILLMENT_STATUSES[index % len(FULFILLMENT_STATUSES)]

        line_items = []
        subtotal_amount = 0.0
        for item_offset in range(item_count):
            sku_seed = index + item_offset
            category, title, base_price = PRODUCTS[sku_seed % len(PRODUCTS)]
            quantity = (sku_seed % 2) + 1
            unit_price = round(base_price + ((index + item_offset) % 9) * 2.35, 2)
            line_total = round(quantity * unit_price, 2)
            subtotal_amount += line_total
            line_items.append(
                {
                    "sku": f"{prefix[:4].upper()}-{category[:3].upper()}-{index:06d}-{item_offset + 1}",
                    "title": f"{company.company} {title}",
                    "category": category,
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "line_total": line_total,
                }
            )

        subtotal_amount = round(subtotal_amount, 2)
        shipping_amount = round(6.75 + item_count * 1.9 + (4.5 if is_expedited else 0.0), 2)
        tax_rate = 0.18 if company.country == "Germany" else 0.13 if company.country == "Canada" else 0.09
        tax_amount = round(subtotal_amount * tax_rate, 2)
        total_amount = round(subtotal_amount + shipping_amount + tax_amount - (discount_amount or 0.0), 2)

        events.append(
            {
                "source": SOURCE_NAME,
                "company": company.company,
                "city": company.city,
                "state": company.state,
                "country": company.country,
                "payload": {
                    "order_id": order_id,
                    "order_number": order_number,
                    "storefront": f"https://{company.domain}",
                    "customer_name": customer_name,
                    "customer_email": customer_email,
                    "currency": company.currency,
                    "subtotal_amount": subtotal_amount,
                    "shipping_amount": shipping_amount,
                    "tax_amount": tax_amount,
                    "discount_amount": discount_amount,
                    "total_amount": total_amount,
                    "item_count": item_count,
                    "payment_status": payment_status,
                    "fulfillment_status": fulfillment_status,
                    "placed_at": format_timestamp(placed_at),
                    "updated_at": format_timestamp(updated_at),
                    "is_expedited": is_expedited,
                    "shipping_address": {
                        "recipient": customer_name,
                        "city": company.city,
                        "state": company.state,
                        "country": company.country,
                        "postal_code": company.postal_code,
                    },
                    "line_items": line_items,
                    "order_metadata": {
                        "sales_channel": company.sales_channel,
                        "payment_method": "card" if index % 3 else "wallet",
                        "promotion_code": None if discount_amount is None else f"PROMO-{(index % 12) + 1:02d}",
                        "synthetic_seed": order_id,
                    },
                },
            }
        )

    return events


def build_customer_name(index: int) -> str:
    first_name = FIRST_NAMES[index % len(FIRST_NAMES)]
    last_name = LAST_NAMES[(index // 3) % len(LAST_NAMES)]
    return f"{first_name} {last_name}"


def format_timestamp(value: datetime) -> str:
    return value.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def write_json(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def write_text(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


if __name__ == "__main__":
    main()
