package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/macho_prawn/events-dashboard/internal/auth"
	"github.com/macho_prawn/events-dashboard/internal/models"
	"github.com/macho_prawn/events-dashboard/internal/store"
	"github.com/macho_prawn/events-dashboard/internal/testutil"
)

func TestCreateEventReplayProtectionReturnsConflictAndAllowsDifferentOwners(t *testing.T) {
	eventStore := newIntegrationStore(t)
	server := newIntegrationServer(t, eventStore)

	ctx := context.Background()

	if _, err := eventStore.CreateSource(ctx, "News", "BBC", "London", "England", "United Kingdom", models.TableSchema{
		{Name: "article_id", Type: "text", Required: true},
		{Name: "headline", Type: "text", Required: true},
	}); err != nil {
		t.Fatalf("CreateSource(news primary) error = %v", err)
	}
	if _, err := eventStore.CreateSource(ctx, "News", "CNA", "Singapore", "South East", "Singapore", models.TableSchema{
		{Name: "article_id", Type: "text", Required: true},
		{Name: "headline", Type: "text", Required: true},
	}); err != nil {
		t.Fatalf("CreateSource(news secondary) error = %v", err)
	}
	if _, err := eventStore.CreateSource(ctx, "Flights", "Delta Air Lines", "Atlanta", "Georgia", "United States", models.TableSchema{
		{Name: "flight_id", Type: "text", Required: true},
	}); err != nil {
		t.Fatalf("CreateSource(flights primary) error = %v", err)
	}
	if _, err := eventStore.CreateSource(ctx, "Flights", "United Airlines", "Chicago", "Illinois", "United States", models.TableSchema{
		{Name: "flight_id", Type: "text", Required: true},
	}); err != nil {
		t.Fatalf("CreateSource(flights secondary) error = %v", err)
	}
	if _, err := eventStore.CreateSource(ctx, "Events", "Acme", "Boston", "Massachusetts", "United States", models.TableSchema{
		{Name: "invoice_number", Type: "text", Required: true},
	}); err != nil {
		t.Fatalf("CreateSource(events primary) error = %v", err)
	}
	if _, err := eventStore.CreateSource(ctx, "ECommerce", "Shopify", "Ottawa", "Ontario", "Canada", models.TableSchema{
		{Name: "order_id", Type: "text", Required: true},
	}); err != nil {
		t.Fatalf("CreateSource(ecommerce primary) error = %v", err)
	}

	t.Run("news article id", func(t *testing.T) {
		first := postEvent(t, server, map[string]any{
			"source":  "News",
			"company": "BBC",
			"city":    "London",
			"state":   "England",
			"country": "United Kingdom",
			"payload": map[string]any{
				"article_id": "article-001",
				"headline":   "first article",
			},
		})
		if first.Code != http.StatusOK {
			t.Fatalf("first POST /events status = %d, body = %s", first.Code, first.Body.String())
		}

		duplicate := postEvent(t, server, map[string]any{
			"source":  "News",
			"company": "BBC",
			"city":    "London",
			"state":   "England",
			"country": "United Kingdom",
			"payload": map[string]any{
				"article_id": "article-001",
				"headline":   "first article",
			},
		})
		if duplicate.Code != http.StatusConflict {
			t.Fatalf("duplicate POST /events status = %d, body = %s", duplicate.Code, duplicate.Body.String())
		}

		secondOwner := postEvent(t, server, map[string]any{
			"source":  "News",
			"company": "CNA",
			"city":    "Singapore",
			"state":   "South East",
			"country": "Singapore",
			"payload": map[string]any{
				"article_id": "article-001",
				"headline":   "second owner article",
			},
		})
		if secondOwner.Code != http.StatusOK {
			t.Fatalf("same article_id under different owner status = %d, body = %s", secondOwner.Code, secondOwner.Body.String())
		}
	})

	t.Run("flights flight id", func(t *testing.T) {
		first := postEvent(t, server, map[string]any{
			"source":  "Flights",
			"company": "Delta Air Lines",
			"city":    "Atlanta",
			"state":   "Georgia",
			"country": "United States",
			"payload": map[string]any{
				"flight_id": "flight-001",
			},
		})
		if first.Code != http.StatusOK {
			t.Fatalf("first POST /events status = %d, body = %s", first.Code, first.Body.String())
		}

		duplicate := postEvent(t, server, map[string]any{
			"source":  "Flights",
			"company": "Delta Air Lines",
			"city":    "Atlanta",
			"state":   "Georgia",
			"country": "United States",
			"payload": map[string]any{
				"flight_id": "flight-001",
			},
		})
		if duplicate.Code != http.StatusConflict {
			t.Fatalf("duplicate POST /events status = %d, body = %s", duplicate.Code, duplicate.Body.String())
		}

		secondOwner := postEvent(t, server, map[string]any{
			"source":  "Flights",
			"company": "United Airlines",
			"city":    "Chicago",
			"state":   "Illinois",
			"country": "United States",
			"payload": map[string]any{
				"flight_id": "flight-001",
			},
		})
		if secondOwner.Code != http.StatusOK {
			t.Fatalf("same flight_id under different owner status = %d, body = %s", secondOwner.Code, secondOwner.Body.String())
		}
	})

	t.Run("events invoice number", func(t *testing.T) {
		first := postEvent(t, server, map[string]any{
			"source":  "Events",
			"company": "Acme",
			"city":    "Boston",
			"state":   "Massachusetts",
			"country": "United States",
			"payload": map[string]any{
				"invoice_number": "inv-001",
			},
		})
		if first.Code != http.StatusOK {
			t.Fatalf("first POST /events status = %d, body = %s", first.Code, first.Body.String())
		}

		duplicate := postEvent(t, server, map[string]any{
			"source":  "Events",
			"company": "Acme",
			"city":    "Boston",
			"state":   "Massachusetts",
			"country": "United States",
			"payload": map[string]any{
				"invoice_number": "inv-001",
			},
		})
		if duplicate.Code != http.StatusConflict {
			t.Fatalf("duplicate POST /events status = %d, body = %s", duplicate.Code, duplicate.Body.String())
		}
	})

	t.Run("ecommerce order id", func(t *testing.T) {
		first := postEvent(t, server, map[string]any{
			"source":  "ECommerce",
			"company": "Shopify",
			"city":    "Ottawa",
			"state":   "Ontario",
			"country": "Canada",
			"payload": map[string]any{
				"order_id": "order-001",
			},
		})
		if first.Code != http.StatusOK {
			t.Fatalf("first POST /events status = %d, body = %s", first.Code, first.Body.String())
		}

		duplicate := postEvent(t, server, map[string]any{
			"source":  "ECommerce",
			"company": "Shopify",
			"city":    "Ottawa",
			"state":   "Ontario",
			"country": "Canada",
			"payload": map[string]any{
				"order_id": "order-001",
			},
		})
		if duplicate.Code != http.StatusConflict {
			t.Fatalf("duplicate POST /events status = %d, body = %s", duplicate.Code, duplicate.Body.String())
		}
	})
}

func newIntegrationStore(t *testing.T) *store.PostgresStore {
	t.Helper()

	databaseURL := testutil.NewIsolatedPostgresDatabase(t)
	eventStore, err := store.NewPostgresStore(databaseURL)
	if err != nil {
		t.Fatalf("NewPostgresStore() error = %v", err)
	}
	t.Cleanup(func() {
		if err := eventStore.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})

	if err := eventStore.AutoMigrate(); err != nil {
		t.Fatalf("AutoMigrate() error = %v", err)
	}

	_, err = eventStore.EnsureAPIKeyAccess(context.Background(), models.APIKeyAccess{
		ID:                     1,
		AccessSigningSecret:    "access-secret",
		AccessIssuer:           "access-issuer",
		AccessSubject:          "access-subject",
		IngestionSigningSecret: "top-secret",
		IngestionIssuer:        "issuer",
		IngestionSubject:       "subject",
		IngestionTTLSeconds:    int(time.Hour.Seconds()),
	})
	if err != nil {
		t.Fatalf("EnsureAPIKeyAccess() error = %v", err)
	}

	return eventStore
}

func newIntegrationServer(t *testing.T, eventStore *store.PostgresStore) http.Handler {
	t.Helper()

	keyManager, err := auth.NewManager("top-secret", "issuer", "subject", time.Hour)
	if err != nil {
		t.Fatalf("NewManager(ingestion) error = %v", err)
	}

	accessKeyManager, err := auth.NewManager("access-secret", "access-issuer", "access-subject", 0)
	if err != nil {
		t.Fatalf("NewManager(access) error = %v", err)
	}

	server, err := NewPublicServer(Dependencies{
		Store:            eventStore,
		KeyManager:       keyManager,
		AccessKeyManager: accessKeyManager,
	})
	if err != nil {
		t.Fatalf("NewPublicServer() error = %v", err)
	}

	return server
}

func postEvent(t *testing.T, server http.Handler, body map[string]any) *httptest.ResponseRecorder {
	t.Helper()

	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+newIngestionToken(t))

	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, req)
	return recorder
}
