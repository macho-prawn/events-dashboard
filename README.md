# events-dashboard

REST API for source-managed event ingestion with JWT authentication. The service is built with Go, Huma, GORM, and PostgreSQL, and is intended to run through Docker Compose.

## Table of Contents

- [Overview](#overview)
- [Project Layout](#project-layout)
- [Requirements](#requirements)
- [Configuration](#configuration)
- [Running the Stack](#running-the-stack)
- [Authentication Model](#authentication-model)
- [Common Workflow](#common-workflow)
- [API Summary](#api-summary)
- [Endpoint Details](#endpoint-details)
- [Validation and Normalization Rules](#validation-and-normalization-rules)
- [Testing](#testing)

## Overview

The current design stores source metadata in a `sources` table and stores event payloads in source-owned child tables.

Key behaviors:

- A source record is uniquely identified by `source + company + city + state + country`.
- A child event table is shared by all locations for the same `source + company`.
- The API validates locations against the embedded airport location reference data.
- The allowed source names are `Events`, `News`, `ECommerce`, and `Flights`.
- Startup runs database migrations, removes the legacy shared `events` table, and seeds the singleton `api_key_access` row if needed.

## Project Layout

- [README.md](/home/macho_prawn/bootcamp/week2/gh-repo/events-dashboard/README.md)
- [docker/](/home/macho_prawn/bootcamp/week2/gh-repo/events-dashboard/docker)
- [docker-compose.yml](/home/macho_prawn/bootcamp/week2/gh-repo/events-dashboard/docker-compose.yml)
- [.env](/home/macho_prawn/bootcamp/week2/gh-repo/events-dashboard/.env)
- [docker/backend/](/home/macho_prawn/bootcamp/week2/gh-repo/events-dashboard/docker/backend)
- [docker/backend/Dockerfile](/home/macho_prawn/bootcamp/week2/gh-repo/events-dashboard/docker/backend/Dockerfile)
- [docker/backend/go.mod](/home/macho_prawn/bootcamp/week2/gh-repo/events-dashboard/docker/backend/go.mod)
- [docker/backend/cmd/api/main.go](/home/macho_prawn/bootcamp/week2/gh-repo/events-dashboard/docker/backend/cmd/api/main.go)
- [docker/backend/internal/api/](/home/macho_prawn/bootcamp/week2/gh-repo/events-dashboard/docker/backend/internal/api)
- [docker/backend/internal/store/store.go](/home/macho_prawn/bootcamp/week2/gh-repo/events-dashboard/docker/backend/internal/store/store.go)
- [docker/backend/internal/reference/airport_locations.csv](/home/macho_prawn/bootcamp/week2/gh-repo/events-dashboard/docker/backend/internal/reference/airport_locations.csv)
- [scripts/generate-jwt.sh](/home/macho_prawn/bootcamp/week2/gh-repo/events-dashboard/scripts/generate-jwt.sh)
- [scripts/create-source.sh](/home/macho_prawn/bootcamp/week2/gh-repo/events-dashboard/scripts/create-source.sh)

## Requirements

- Docker with Compose support
- `curl`
- `jq`
- `openssl`
- a writable host directory for `DB_VOLUME`

## Configuration

The API listens on `APP_PORT`, which defaults to `8081`.

Default values in [.env](/home/macho_prawn/bootcamp/week2/gh-repo/events-dashboard/.env):

- `APP_PORT=8081`
- `DB_HOST=db`
- `DB_PORT=5432`
- `DB_NAME=events`
- `DB_USER=events`
- `DB_PASSWORD=events`
- `DB_UID=70`
- `DB_GID=70`
- `DB_VOLUME=/home/macho_prawn/bootcamp/week2/volumes/database`

Runtime notes:

- The app accepts either `HOST` or `APP_HOST` for the bind address. Default: `0.0.0.0`.
- The app accepts either `PORT` or `APP_PORT` for the public port. Default: `8081`.
- `DATABASE_URL` can be provided directly. Otherwise it is assembled from the `DB_*` variables.
- `docker-compose.yml` bind-mounts the PostgreSQL data directory from `DB_VOLUME`.

## Running the Stack

Create the database directory if it does not already exist:

```bash
mkdir -p /home/macho_prawn/bootcamp/week2/volumes/database
```

Start the services from the repository root:

```bash
docker compose -f docker-compose.yml --env-file .env up --build
```

The API will be available at `http://127.0.0.1:8081` unless you change `APP_PORT`.

Health check:

```bash
curl -sS http://127.0.0.1:8081/healthz
```

## Authentication Model

The service uses two JWT types.

### Access JWT

- Required for `GET /api-key`, `POST /source`, and `GET /source`
- Signed with the access secret stored in `api_key_access`
- Non-expiring with the current implementation

### Ingestion/Search JWT

- Required for `POST /events` and `GET /search`
- Signed with the ingestion secret stored in `api_key_access`
- Short-lived
- Default TTL is `3600` seconds

Generate tokens with the helper script:

```bash
scripts/generate-jwt.sh -t access
scripts/generate-jwt.sh -t ingestion
```

Notes:

- `scripts/generate-jwt.sh -t access` reads JWT config directly from the database container and builds the access token locally.
- `scripts/generate-jwt.sh -t ingestion` first builds the access token, then calls `GET /api-key` to fetch a short-lived ingestion token.

## Common Workflow

1. Start the stack.
2. Generate an access token.
3. Create a source record and its child table.
4. Generate an ingestion token.
5. Insert events.
6. Search events.

Example:

```bash
ACCESS_JWT="$(scripts/generate-jwt.sh -t access)"
```

Create a source:

```bash
ACCESS_JWT="$ACCESS_JWT" scripts/create-source.sh \
  -s "Events" \
  -c "Acme" \
  -i "Boston" \
  -t "Massachusetts" \
  -n "United States" \
  -j '[{"name":"invoice_number","type":"text","required":true},{"name":"amount","type":"numeric","required":false}]'
```

Fetch an ingestion token:

```bash
INGESTION_JWT="$(scripts/generate-jwt.sh -t ingestion)"
```

Insert an event:

```bash
curl -sS \
  -X POST \
  -H "Authorization: Bearer $INGESTION_JWT" \
  -H "Content-Type: application/json" \
  -d '{
    "source": "Events",
    "company": "Acme",
    "city": "Boston",
    "state": "Massachusetts",
    "country": "United States",
    "payload": {
      "invoice_number": "INV-100",
      "amount": 12.5
    }
  }' \
  http://127.0.0.1:8081/events
```

Search:

```bash
curl -sS \
  -H "Authorization: Bearer $INGESTION_JWT" \
  "http://127.0.0.1:8081/search?source=Events&company=Acme&q=INV"
```

## API Summary

| Method | Path | Auth | Purpose |
| --- | --- | --- | --- |
| `GET` | `/healthz` | none | Health check |
| `GET` | `/api-key` | access JWT | Issue ingestion/search JWT |
| `POST` | `/source` | access JWT | Create a source row and child table |
| `GET` | `/source` | access JWT | List source rows |
| `POST` | `/events` | ingestion/search JWT | Insert an event into the matching child table |
| `GET` | `/search` | ingestion/search JWT | Search events inside the child table for a `source + company` owner |

## Endpoint Details

### `GET /healthz`

Checks whether the API can reach PostgreSQL.

```bash
curl -sS http://127.0.0.1:8081/healthz
```

### `GET /api-key`

Returns a short-lived ingestion/search JWT.

Response fields:

- `apiKey`
- `expiresAt`

```bash
curl -sS \
  -H "Authorization: Bearer $ACCESS_JWT" \
  http://127.0.0.1:8081/api-key
```

### `POST /source`

Creates a source record. If this is the first record for a given `source + company`, the API creates a child table named from that owner pair. If the owner already exists, the incoming schema must match the existing schema exactly.

Required body fields:

- `source`
- `company`
- `city`
- `state`
- `country`
- `tableSchema`

`tableSchema` must be a non-empty JSON array of column definitions:

- `name`
- `type`
- `required`

Allowed column types:

- `text`
- `integer`
- `bigint`
- `boolean`
- `numeric`
- `timestamptz`
- `jsonb`

Example:

```bash
curl -sS \
  -X POST \
  -H "Authorization: Bearer $ACCESS_JWT" \
  -H "Content-Type: application/json" \
  -d '{
    "source": "Events",
    "company": "Acme",
    "city": "Boston",
    "state": "Massachusetts",
    "country": "United States",
    "tableSchema": [
      {"name":"invoice_number","type":"text","required":true},
      {"name":"amount","type":"numeric","required":false}
    ]
  }' \
  http://127.0.0.1:8081/source
```

### `GET /source`

Lists source records and their child-table metadata.

```bash
curl -sS \
  -H "Authorization: Bearer $ACCESS_JWT" \
  http://127.0.0.1:8081/source
```

### `POST /events`

Creates an event row in the child table associated with the exact source identity.

Required body fields:

- `source`
- `company`
- `city`
- `state`
- `country`
- `payload`

`payload` must:

- include every required schema field
- omit unsupported fields
- use values compatible with the declared column types

Type expectations:

- `text` -> JSON string
- `integer` and `bigint` -> integer value
- `numeric` -> numeric value
- `boolean` -> boolean value
- `timestamptz` -> RFC3339 string
- `jsonb` -> valid JSON value

Example:

```bash
curl -sS \
  -X POST \
  -H "Authorization: Bearer $INGESTION_JWT" \
  -H "Content-Type: application/json" \
  -d '{
    "source": "Events",
    "company": "Acme",
    "city": "Boston",
    "state": "Massachusetts",
    "country": "United States",
    "payload": {
      "invoice_number": "INV-100",
      "amount": 12.5
    }
  }' \
  http://127.0.0.1:8081/events
```

### `GET /search`

Searches the child table for the `source + company` owner.

Required query params:

- `source`
- `company`

Optional query params:

- `city`
- `state`
- `country`
- `q`
- `page`

Search behavior:

- `source` and `company` select the owner table
- `city`, `state`, and `country` further filter results inside that table
- `q` performs `ILIKE` search only across dynamic `text` payload columns
- `page` defaults to `1`
- page size is fixed at `50`

Examples:

```bash
curl -sS \
  -H "Authorization: Bearer $INGESTION_JWT" \
  "http://127.0.0.1:8081/search?source=Events&company=Acme"
```

```bash
curl -sS \
  -H "Authorization: Bearer $INGESTION_JWT" \
  "http://127.0.0.1:8081/search?source=Events&company=Acme&city=Boston&state=Massachusetts&country=United%20States"
```

```bash
curl -sS \
  -H "Authorization: Bearer $INGESTION_JWT" \
  "http://127.0.0.1:8081/search?source=Events&company=Acme&q=INV&page=2"
```

## Validation and Normalization Rules

Source rules:

- `source` must be exactly one of `Events`, `News`, `ECommerce`, or `Flights`
- `company`, `city`, and `state` are normalized to title case
- `country` is trimmed and validated against the embedded reference data

Location rules:

- `city + state + country` must match an entry in the embedded airport location reference
- invalid or mismatched locations are rejected

Schema rules:

- `tableSchema` must contain at least one column
- column names are normalized to lowercase snake_case
- reserved column names are rejected: `id`, `source_parent_id`, `source`, `company`, `city`, `state`, `country`, `created_at`
- duplicate normalized column names are rejected
- an existing `source + company` owner cannot be reused with a different schema

Event rules:

- the source identity must already exist
- required payload fields cannot be missing or `null`
- unknown payload fields are rejected

Operational notes:

- CORS headers are stripped from responses, and `OPTIONS` requests return `405 Method Not Allowed`
- child tables are named as `events_<normalized-source>_<normalized-company>`
- if the generated child table name exceeds PostgreSQL's 63-character limit, it is truncated and suffixed with a hash

## Testing

Run the Go test suite from [docker/backend/](/home/macho_prawn/bootcamp/week2/gh-repo/events-dashboard/docker/backend):

```bash
go test ./...
```
