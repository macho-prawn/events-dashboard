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
)

type stubStore struct {
	createSourceFn func(context.Context, string, string, string, string, string, models.TableSchema) (*models.Source, error)
	listSourcesFn  func(context.Context) ([]models.Source, error)
	createEventFn  func(context.Context, string, string, string, string, string, map[string]any) (*store.ChildEventRow, error)
	searchEventsFn func(context.Context, string, string, string, string, string, string, int, int) ([]store.ChildEventRow, int64, error)
	pingErr        error
}

func (s *stubStore) CreateSource(ctx context.Context, source string, company string, city string, state string, country string, schema models.TableSchema) (*models.Source, error) {
	if s.createSourceFn == nil {
		return nil, nil
	}
	return s.createSourceFn(ctx, source, company, city, state, country, schema)
}

func (s *stubStore) ListSources(ctx context.Context) ([]models.Source, error) {
	if s.listSourcesFn == nil {
		return nil, nil
	}
	return s.listSourcesFn(ctx)
}

func (s *stubStore) CreateEvent(ctx context.Context, source string, company string, city string, state string, country string, payload map[string]any) (*store.ChildEventRow, error) {
	if s.createEventFn == nil {
		return nil, nil
	}
	return s.createEventFn(ctx, source, company, city, state, country, payload)
}

func (s *stubStore) SearchEvents(ctx context.Context, source string, company string, city string, state string, country string, query string, page int, pageSize int) ([]store.ChildEventRow, int64, error) {
	if s.searchEventsFn == nil {
		return nil, 0, nil
	}
	return s.searchEventsFn(ctx, source, company, city, state, country, query, page, pageSize)
}

func (s *stubStore) Ping(context.Context) error {
	return s.pingErr
}

func newTestServer(t *testing.T, store *stubStore) http.Handler {
	t.Helper()

	keyManager, err := auth.NewManager("top-secret", "issuer", "subject", time.Hour)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	accessKeyManager, err := auth.NewManager("access-secret", "access-issuer", "access-subject", 0)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	server, err := NewPublicServer(Dependencies{
		Store:            store,
		KeyManager:       keyManager,
		AccessKeyManager: accessKeyManager,
	})
	if err != nil {
		t.Fatalf("NewPublicServer() error = %v", err)
	}

	return server
}

func newAccessToken(t *testing.T) string {
	t.Helper()
	manager, err := auth.NewManager("access-secret", "access-issuer", "access-subject", 0)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	token, _, err := manager.Token()
	if err != nil {
		t.Fatalf("Token() error = %v", err)
	}
	return token
}

func newIngestionToken(t *testing.T) string {
	t.Helper()
	manager, err := auth.NewManager("top-secret", "issuer", "subject", time.Hour)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	token, _, err := manager.Token()
	if err != nil {
		t.Fatalf("Token() error = %v", err)
	}
	return token
}

func TestGetAPIKeySuccess(t *testing.T) {
	server := newTestServer(t, &stubStore{})
	req := httptest.NewRequest(http.MethodGet, "/api-key", nil)
	req.Header.Set("Authorization", "Bearer "+newAccessToken(t))

	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestCreateSourceSuccess(t *testing.T) {
	server := newTestServer(t, &stubStore{
		createSourceFn: func(_ context.Context, source string, company string, city string, state string, country string, schema models.TableSchema) (*models.Source, error) {
			return &models.Source{
				ID:             1,
				Source:         source,
				Company:        company,
				City:           city,
				State:          state,
				Country:        country,
				ChildTableName: "events_stripe_acme",
				TableSchema:    schema,
				CreatedAt:      time.Date(2026, time.March, 8, 0, 0, 0, 0, time.UTC),
				UpdatedAt:      time.Date(2026, time.March, 8, 0, 0, 0, 0, time.UTC),
			}, nil
		},
	})

	body := map[string]any{
		"source":  "Events",
		"company": "acme",
		"city":    "Boston",
		"state":   "Massachusetts",
		"country": "United States",
		"tableSchema": []map[string]any{
			{"name": "invoice_number", "type": "text", "required": true},
		},
	}
	payload, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/source", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+newAccessToken(t))

	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestCreateSourceDuplicate(t *testing.T) {
	server := newTestServer(t, &stubStore{
		createSourceFn: func(context.Context, string, string, string, string, string, models.TableSchema) (*models.Source, error) {
			return nil, store.ErrDuplicateSource
		},
	})

	req := httptest.NewRequest(http.MethodPost, "/source", bytes.NewReader([]byte(`{"source":"Events","company":"acme","city":"Boston","state":"Massachusetts","country":"United States","tableSchema":[{"name":"invoice_number","type":"text","required":true}]}`)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+newAccessToken(t))

	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusConflict {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestCreateSourceInvalidSource(t *testing.T) {
	server := newTestServer(t, &stubStore{
		createSourceFn: func(context.Context, string, string, string, string, string, models.TableSchema) (*models.Source, error) {
			return nil, store.ErrInvalidSource
		},
	})

	req := httptest.NewRequest(http.MethodPost, "/source", bytes.NewReader([]byte(`{"source":"stripe","company":"acme","city":"Boston","state":"Massachusetts","country":"United States","tableSchema":[{"name":"invoice_number","type":"text","required":true}]}`)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+newAccessToken(t))

	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestListSourcesSuccess(t *testing.T) {
	server := newTestServer(t, &stubStore{
		listSourcesFn: func(context.Context) ([]models.Source, error) {
			return []models.Source{{
				ID:             1,
				Source:         "Events",
				Company:        "acme",
				City:           "Boston",
				State:          "Massachusetts",
				Country:        "United States",
				ChildTableName: "events_stripe_acme",
				TableSchema: models.TableSchema{
					{Name: "invoice_number", Type: "text", Required: true},
				},
			}}, nil
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/source", nil)
	req.Header.Set("Authorization", "Bearer "+newAccessToken(t))

	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestCreateEventSuccess(t *testing.T) {
	server := newTestServer(t, &stubStore{
		createEventFn: func(_ context.Context, source string, company string, city string, state string, country string, payload map[string]any) (*store.ChildEventRow, error) {
			if source != "Events" || company != "acme" || city != "Boston" || state != "Massachusetts" || country != "United States" {
				t.Fatalf("unexpected metadata: %s %s %s %s %s", source, company, city, state, country)
			}
			if payload["invoice_number"] != "INV-1" {
				t.Fatalf("unexpected payload: %#v", payload)
			}
			return &store.ChildEventRow{
				ID:        42,
				CreatedAt: time.Date(2026, time.March, 8, 0, 0, 0, 0, time.UTC),
			}, nil
		},
	})

	req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader([]byte(`{"source":"Events","company":"acme","city":"Boston","state":"Massachusetts","country":"United States","payload":{"invoice_number":"INV-1"}}`)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+newIngestionToken(t))

	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestCreateEventUnknownSource(t *testing.T) {
	server := newTestServer(t, &stubStore{
		createEventFn: func(context.Context, string, string, string, string, string, map[string]any) (*store.ChildEventRow, error) {
			return nil, store.ErrSourceNotFound
		},
	})

	req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader([]byte(`{"source":"Events","company":"acme","city":"Boston","state":"Massachusetts","country":"United States","payload":{"invoice_number":"INV-1"}}`)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+newIngestionToken(t))

	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestCreateEventDuplicate(t *testing.T) {
	server := newTestServer(t, &stubStore{
		createEventFn: func(context.Context, string, string, string, string, string, map[string]any) (*store.ChildEventRow, error) {
			return nil, store.ErrDuplicateEvent
		},
	})

	req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader([]byte(`{"source":"Flights","company":"delta air lines","city":"Atlanta","state":"Georgia","country":"United States","payload":{"flight_id":"delta-air-lines-000001"}}`)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+newIngestionToken(t))

	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusConflict {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestSearchEventsSuccess(t *testing.T) {
	server := newTestServer(t, &stubStore{
		searchEventsFn: func(_ context.Context, source string, company string, city string, state string, country string, query string, page int, pageSize int) ([]store.ChildEventRow, int64, error) {
			if source != "Events" || company != "acme" || city != "Boston" || state != "Massachusetts" || country != "United States" || query != "INV" || page != 2 || pageSize != searchPageSize {
				t.Fatalf("unexpected search params: %s %s %s %s %s %s %d %d", source, company, city, state, country, query, page, pageSize)
			}
			return []store.ChildEventRow{{
				ID:             42,
				SourceParentID: 7,
				Source:         "Events",
				Company:        "acme",
				City:           "Boston",
				State:          "Massachusetts",
				Country:        "United States",
				CreatedAt:      time.Date(2026, time.March, 8, 0, 0, 0, 0, time.UTC),
				Payload:        map[string]any{"invoice_number": "INV-1"},
			}}, 1, nil
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/search?source=Events&company=acme&city=Boston&state=Massachusetts&country=United%20States&q=INV&page=2", nil)
	req.Header.Set("Authorization", "Bearer "+newIngestionToken(t))

	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestSearchEventsRequiresSourceAndCompany(t *testing.T) {
	server := newTestServer(t, &stubStore{})
	req := httptest.NewRequest(http.MethodGet, "/search?q=INV", nil)
	req.Header.Set("Authorization", "Bearer "+newIngestionToken(t))

	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestSearchEventsInvalidSource(t *testing.T) {
	server := newTestServer(t, &stubStore{
		searchEventsFn: func(context.Context, string, string, string, string, string, string, int, int) ([]store.ChildEventRow, int64, error) {
			return nil, 0, store.ErrInvalidSource
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/search?source=stripe&company=acme", nil)
	req.Header.Set("Authorization", "Bearer "+newIngestionToken(t))

	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestHealth(t *testing.T) {
	server := newTestServer(t, &stubStore{})
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)

	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}
