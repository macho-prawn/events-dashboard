package store

import (
	"context"
	"strings"
	"testing"

	"github.com/macho_prawn/events-dashboard/internal/models"
	"github.com/macho_prawn/events-dashboard/internal/testutil"
)

type replayProtectedSourceFixture struct {
	source    string
	company   string
	city      string
	state     string
	country   string
	tableName string
	replayKey string
}

var replayProtectedSourceFixtures = []replayProtectedSourceFixture{
	{source: "ECommerce", company: "Shopify", city: "Ottawa", state: "Ontario", country: "Canada", tableName: "events_ecommerce_shopify", replayKey: "order_id"},
	{source: "Events", company: "Acme", city: "Boston", state: "Massachusetts", country: "United States", tableName: "events_events_acme", replayKey: "invoice_number"},
	{source: "Flights", company: "Delta Air Lines", city: "Atlanta", state: "Georgia", country: "United States", tableName: "events_flights_delta_air_lines", replayKey: "flight_id"},
	{source: "Flights", company: "Emirates", city: "Dubai", state: "Dubai Emirate", country: "United Arab Emirates", tableName: "events_flights_emirates", replayKey: "flight_id"},
	{source: "Flights", company: "Qantas", city: "Sydney", state: "New South Wales", country: "Australia", tableName: "events_flights_qantas", replayKey: "flight_id"},
	{source: "Flights", company: "Singapore Airlines", city: "Singapore", state: "South East", country: "Singapore", tableName: "events_flights_singapore_airlines", replayKey: "flight_id"},
	{source: "Flights", company: "Southwest Airlines", city: "Dallas", state: "Texas", country: "United States", tableName: "events_flights_southwest_airlines", replayKey: "flight_id"},
	{source: "Flights", company: "United Airlines", city: "Chicago", state: "Illinois", country: "United States", tableName: "events_flights_united_airlines", replayKey: "flight_id"},
	{source: "News", company: "Africanews", city: "Chassieu, Lyon", state: "Auvergne-Rhône-Alpes", country: "France", tableName: "events_news_africanews", replayKey: "article_id"},
	{source: "News", company: "Bbc", city: "London", state: "England", country: "United Kingdom", tableName: "events_news_bbc", replayKey: "article_id"},
	{source: "News", company: "Cna", city: "Singapore", state: "South East", country: "Singapore", tableName: "events_news_cna", replayKey: "article_id"},
	{source: "News", company: "Gestion", city: "Lima", state: "Lima Region", country: "Peru", tableName: "events_news_gestion", replayKey: "article_id"},
	{source: "News", company: "Sbs News", city: "Sydney", state: "New South Wales", country: "Australia", tableName: "events_news_sbs_news", replayKey: "article_id"},
	{source: "News", company: "The Indian Express", city: "Gautam Buddha Nagar", state: "Uttar Pradesh", country: "India", tableName: "events_news_the_indian_express", replayKey: "article_id"},
}

func TestAutoMigrateReplayProtectionForCurrentSourceTables(t *testing.T) {
	databaseURL := testutil.NewIsolatedPostgresDatabase(t)

	store, err := NewPostgresStore(databaseURL)
	if err != nil {
		t.Fatalf("NewPostgresStore() error = %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})

	if err := store.db.AutoMigrate(&models.APIKeyAccess{}, &models.Source{}); err != nil {
		t.Fatalf("seed AutoMigrate() error = %v", err)
	}

	sourceIDs := make(map[string]uint, len(replayProtectedSourceFixtures))
	for _, fixture := range replayProtectedSourceFixtures {
		record := models.Source{
			Source:         fixture.source,
			Company:        fixture.company,
			City:           fixture.city,
			State:          fixture.state,
			Country:        fixture.country,
			ChildTableName: fixture.tableName,
			TableSchema: models.TableSchema{
				{Name: fixture.replayKey, Type: "text", Required: true},
				{Name: "headline_or_status", Type: "text", Required: false},
			},
		}
		if err := store.db.Create(&record).Error; err != nil {
			t.Fatalf("Create(source %s) error = %v", fixture.tableName, err)
		}
		sourceIDs[fixture.tableName] = record.ID

		if err := createChildTable(store.db, fixture.tableName, record.TableSchema); err != nil {
			t.Fatalf("createChildTable(%s) error = %v", fixture.tableName, err)
		}
	}

	deltaSourceID := sourceIDs["events_flights_delta_air_lines"]
	if err := store.db.Exec(`
		INSERT INTO events_flights_delta_air_lines
			(id, source_parent_id, source, company, city, state, country, flight_id, headline_or_status)
		VALUES
			(11, ?, 'Flights', 'Delta Air Lines', 'Atlanta', 'Georgia', 'United States', 'delta-001', 'scheduled'),
			(12, ?, 'Flights', 'Delta Air Lines', 'Atlanta', 'Georgia', 'United States', 'delta-001', 'delayed'),
			(13, ?, 'Flights', 'Delta Air Lines', 'Atlanta', 'Georgia', 'United States', 'delta-002', 'boarding')
	`, deltaSourceID, deltaSourceID, deltaSourceID).Error; err != nil {
		t.Fatalf("seed duplicate delta rows error = %v", err)
	}

	eventsSourceID := sourceIDs["events_events_acme"]
	if err := store.db.Exec(`
		INSERT INTO events_events_acme
			(id, source_parent_id, source, company, city, state, country, invoice_number, headline_or_status)
		VALUES
			(21, ?, 'Events', 'Acme', 'Boston', 'Massachusetts', 'United States', 'inv-001', 'first'),
			(22, ?, 'Events', 'Acme', 'Boston', 'Massachusetts', 'United States', 'inv-001', 'duplicate'),
			(23, ?, 'Events', 'Acme', 'Boston', 'Massachusetts', 'United States', 'inv-002', 'second')
	`, eventsSourceID, eventsSourceID, eventsSourceID).Error; err != nil {
		t.Fatalf("seed duplicate events rows error = %v", err)
	}

	ecommerceSourceID := sourceIDs["events_ecommerce_shopify"]
	if err := store.db.Exec(`
		INSERT INTO events_ecommerce_shopify
			(id, source_parent_id, source, company, city, state, country, order_id, headline_or_status)
		VALUES
			(31, ?, 'ECommerce', 'Shopify', 'Ottawa', 'Ontario', 'Canada', 'order-001', 'first'),
			(32, ?, 'ECommerce', 'Shopify', 'Ottawa', 'Ontario', 'Canada', 'order-001', 'duplicate'),
			(33, ?, 'ECommerce', 'Shopify', 'Ottawa', 'Ontario', 'Canada', 'order-002', 'second')
	`, ecommerceSourceID, ecommerceSourceID, ecommerceSourceID).Error; err != nil {
		t.Fatalf("seed duplicate ecommerce rows error = %v", err)
	}

	if err := store.AutoMigrate(); err != nil {
		t.Fatalf("AutoMigrate() error = %v", err)
	}

	type flightRow struct {
		ID       int64
		FlightID string
		Status   string
	}

	var remaining []flightRow
	if err := store.db.Raw(`
		SELECT id, flight_id, headline_or_status AS status
		FROM events_flights_delta_air_lines
		WHERE flight_id = 'delta-001'
		ORDER BY id ASC
	`).Scan(&remaining).Error; err != nil {
		t.Fatalf("query remaining delta rows error = %v", err)
	}

	if len(remaining) != 1 {
		t.Fatalf("remaining duplicate delta rows = %d, want 1", len(remaining))
	}
	if remaining[0].ID != 11 {
		t.Fatalf("remaining delta row id = %d, want earliest id 11", remaining[0].ID)
	}
	if remaining[0].Status != "scheduled" {
		t.Fatalf("remaining delta row status = %q, want earliest row payload", remaining[0].Status)
	}

	type genericRow struct {
		ID   int64
		Key  string
		Note string
	}

	var remainingEvents []genericRow
	if err := store.db.Raw(`
		SELECT id, invoice_number AS key, headline_or_status AS note
		FROM events_events_acme
		WHERE invoice_number = 'inv-001'
		ORDER BY id ASC
	`).Scan(&remainingEvents).Error; err != nil {
		t.Fatalf("query remaining events rows error = %v", err)
	}
	if len(remainingEvents) != 1 {
		t.Fatalf("remaining duplicate events rows = %d, want 1", len(remainingEvents))
	}
	if remainingEvents[0].ID != 21 {
		t.Fatalf("remaining events row id = %d, want earliest id 21", remainingEvents[0].ID)
	}
	if remainingEvents[0].Note != "first" {
		t.Fatalf("remaining events row note = %q, want earliest row payload", remainingEvents[0].Note)
	}

	var remainingECommerce []genericRow
	if err := store.db.Raw(`
		SELECT id, order_id AS key, headline_or_status AS note
		FROM events_ecommerce_shopify
		WHERE order_id = 'order-001'
		ORDER BY id ASC
	`).Scan(&remainingECommerce).Error; err != nil {
		t.Fatalf("query remaining ecommerce rows error = %v", err)
	}
	if len(remainingECommerce) != 1 {
		t.Fatalf("remaining duplicate ecommerce rows = %d, want 1", len(remainingECommerce))
	}
	if remainingECommerce[0].ID != 31 {
		t.Fatalf("remaining ecommerce row id = %d, want earliest id 31", remainingECommerce[0].ID)
	}
	if remainingECommerce[0].Note != "first" {
		t.Fatalf("remaining ecommerce row note = %q, want earliest row payload", remainingECommerce[0].Note)
	}

	for _, fixture := range replayProtectedSourceFixtures {
		if !hasUniqueReplayIndex(t, store, fixture.tableName, fixture.replayKey) {
			t.Fatalf("table %s missing unique replay index on (source_parent_id, %s)", fixture.tableName, fixture.replayKey)
		}
	}
}

func hasUniqueReplayIndex(t *testing.T, store *PostgresStore, tableName string, replayKey string) bool {
	t.Helper()

	var exists bool
	if err := store.db.Raw(`
		SELECT EXISTS (
			SELECT 1
			FROM pg_index ix
			JOIN pg_class tbl ON tbl.oid = ix.indrelid
			WHERE tbl.relname = ?
				AND ix.indisunique
				AND (
					SELECT array_agg(att.attname::text ORDER BY ord.n)
					FROM unnest(ix.indkey) WITH ORDINALITY AS ord(attnum, n)
					JOIN pg_attribute att ON att.attrelid = tbl.oid AND att.attnum = ord.attnum
				) = ARRAY['source_parent_id', ?]::text[]
		)
	`, tableName, replayKey).Scan(&exists).Error; err != nil {
		t.Fatalf("index lookup for %s error = %v", tableName, err)
	}

	return exists
}

func TestCreateEventRejectsDuplicateReplayKeyPerSourceParent(t *testing.T) {
	databaseURL := testutil.NewIsolatedPostgresDatabase(t)

	store, err := NewPostgresStore(databaseURL)
	if err != nil {
		t.Fatalf("NewPostgresStore() error = %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})

	if err := store.AutoMigrate(); err != nil {
		t.Fatalf("AutoMigrate() error = %v", err)
	}

	ctx := context.Background()
	first, err := store.CreateSource(ctx, "Flights", "Delta Air Lines", "Atlanta", "Georgia", "United States", "", models.TableSchema{
		{Name: "flight_id", Type: "text", Required: true},
	})
	if err != nil {
		t.Fatalf("CreateSource(first) error = %v", err)
	}

	second, err := store.CreateSource(ctx, "Flights", "Delta Air Lines", "Boston", "Massachusetts", "United States", "", models.TableSchema{
		{Name: "flight_id", Type: "text", Required: true},
	})
	if err != nil {
		t.Fatalf("CreateSource(second) error = %v", err)
	}

	_, err = store.CreateEvent(ctx, "Flights", "Delta Air Lines", first.City, first.State, first.Country, map[string]any{"flight_id": "shared-1"})
	if err != nil {
		t.Fatalf("CreateEvent(first insert) error = %v", err)
	}

	_, err = store.CreateEvent(ctx, "Flights", "Delta Air Lines", first.City, first.State, first.Country, map[string]any{"flight_id": "shared-1"})
	if err == nil || err != ErrDuplicateEvent {
		t.Fatalf("CreateEvent(duplicate same source parent) error = %v, want %v", err, ErrDuplicateEvent)
	}

	_, err = store.CreateEvent(ctx, "Flights", "Delta Air Lines", second.City, second.State, second.Country, map[string]any{"flight_id": "shared-1"})
	if err != nil {
		t.Fatalf("CreateEvent(same replay key different source parent) error = %v", err)
	}
}

func TestCreateEventRejectsDuplicateReplayKeyForEventsAndECommerce(t *testing.T) {
	databaseURL := testutil.NewIsolatedPostgresDatabase(t)

	store, err := NewPostgresStore(databaseURL)
	if err != nil {
		t.Fatalf("NewPostgresStore() error = %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})

	if err := store.AutoMigrate(); err != nil {
		t.Fatalf("AutoMigrate() error = %v", err)
	}

	ctx := context.Background()

	eventSource, err := store.CreateSource(ctx, "Events", "Acme", "Boston", "Massachusetts", "United States", "", models.TableSchema{
		{Name: "invoice_number", Type: "text", Required: true},
	})
	if err != nil {
		t.Fatalf("CreateSource(Events) error = %v", err)
	}
	ecommerceSource, err := store.CreateSource(ctx, "ECommerce", "Shopify", "Ottawa", "Ontario", "Canada", "", models.TableSchema{
		{Name: "order_id", Type: "text", Required: true},
	})
	if err != nil {
		t.Fatalf("CreateSource(ECommerce) error = %v", err)
	}

	_, err = store.CreateEvent(ctx, "Events", "Acme", eventSource.City, eventSource.State, eventSource.Country, map[string]any{"invoice_number": "inv-1"})
	if err != nil {
		t.Fatalf("CreateEvent(Events first insert) error = %v", err)
	}
	_, err = store.CreateEvent(ctx, "Events", "Acme", eventSource.City, eventSource.State, eventSource.Country, map[string]any{"invoice_number": "inv-1"})
	if err == nil || err != ErrDuplicateEvent {
		t.Fatalf("CreateEvent(Events duplicate) error = %v, want %v", err, ErrDuplicateEvent)
	}

	_, err = store.CreateEvent(ctx, "ECommerce", "Shopify", ecommerceSource.City, ecommerceSource.State, ecommerceSource.Country, map[string]any{"order_id": "order-1"})
	if err != nil {
		t.Fatalf("CreateEvent(ECommerce first insert) error = %v", err)
	}
	_, err = store.CreateEvent(ctx, "ECommerce", "Shopify", ecommerceSource.City, ecommerceSource.State, ecommerceSource.Country, map[string]any{"order_id": "order-1"})
	if err == nil || err != ErrDuplicateEvent {
		t.Fatalf("CreateEvent(ECommerce duplicate) error = %v, want %v", err, ErrDuplicateEvent)
	}
}

func TestReplayProtectedFixturesUseExpectedTableNames(t *testing.T) {
	for _, fixture := range replayProtectedSourceFixtures {
		actual, err := buildChildTableName(fixture.source, fixture.company)
		if err != nil {
			t.Fatalf("buildChildTableName(%s, %s) error = %v", fixture.source, fixture.company, err)
		}
		if actual != fixture.tableName {
			t.Fatalf("buildChildTableName(%s, %s) = %s, want %s", fixture.source, fixture.company, actual, fixture.tableName)
		}
		if strings.TrimSpace(fixture.replayKey) == "" {
			t.Fatalf("fixture %s missing replay key", fixture.tableName)
		}
	}
}
