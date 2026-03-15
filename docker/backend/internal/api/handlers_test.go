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
	createSourceFn          func(context.Context, string, string, string, string, string, string, models.TableSchema) (*models.Source, error)
	listSourcesFn           func(context.Context) ([]models.Source, error)
	createProjectFn         func(context.Context, string, string, *time.Time) (*models.Project, error)
	listProjectsFn          func(context.Context) ([]models.Project, error)
	getLatestProjectFn      func(context.Context) (*models.Project, error)
	deleteProjectFn         func(context.Context, string) error
	updateProjectJWTFn      func(context.Context, string, string, *time.Time) (*models.Project, error)
	attachProjectOwnerFn    func(context.Context, string, string, string) (*models.SourceOwner, error)
	detachProjectOwnerFn    func(context.Context, string, string, string) error
	listProjectOwnersFn     func(context.Context, string) ([]models.SourceOwner, error)
	getSourceOwnerEventsFn  func(context.Context, string, string) (*models.SourceOwner, []store.ChildEventRow, error)
	getProjectOwnerEventsFn func(context.Context, string, string, string) (*models.SourceOwner, []store.ChildEventRow, error)
	createEventFn           func(context.Context, string, string, string, string, string, map[string]any) (*store.ChildEventRow, error)
	searchEventsFn          func(context.Context, string, string, string, string, string, string, int, int) ([]store.ChildEventRow, int64, error)
	pingErr                 error
}

func (s *stubStore) CreateSource(ctx context.Context, source string, company string, city string, state string, country string, websiteDomain string, schema models.TableSchema) (*models.Source, error) {
	if s.createSourceFn == nil {
		return nil, nil
	}
	return s.createSourceFn(ctx, source, company, city, state, country, websiteDomain, schema)
}

func (s *stubStore) ListSources(ctx context.Context) ([]models.Source, error) {
	if s.listSourcesFn == nil {
		return nil, nil
	}
	return s.listSourcesFn(ctx)
}

func (s *stubStore) CreateProject(ctx context.Context, projectName string, ingestionJWT string, ingestionJWTExpiresAt *time.Time) (*models.Project, error) {
	if s.createProjectFn == nil {
		return nil, nil
	}
	return s.createProjectFn(ctx, projectName, ingestionJWT, ingestionJWTExpiresAt)
}

func (s *stubStore) ListProjects(ctx context.Context) ([]models.Project, error) {
	if s.listProjectsFn == nil {
		return nil, nil
	}
	return s.listProjectsFn(ctx)
}

func (s *stubStore) GetLatestProject(ctx context.Context) (*models.Project, error) {
	if s.getLatestProjectFn == nil {
		return nil, nil
	}
	return s.getLatestProjectFn(ctx)
}

func (s *stubStore) DeleteProject(ctx context.Context, projectName string) error {
	if s.deleteProjectFn == nil {
		return nil
	}
	return s.deleteProjectFn(ctx, projectName)
}

func (s *stubStore) UpdateProjectIngestionJWT(ctx context.Context, projectName string, ingestionJWT string, ingestionJWTExpiresAt *time.Time) (*models.Project, error) {
	if s.updateProjectJWTFn == nil {
		return nil, nil
	}
	return s.updateProjectJWTFn(ctx, projectName, ingestionJWT, ingestionJWTExpiresAt)
}

func (s *stubStore) AttachProjectSourceOwner(ctx context.Context, projectName string, source string, company string) (*models.SourceOwner, error) {
	if s.attachProjectOwnerFn == nil {
		return nil, nil
	}
	return s.attachProjectOwnerFn(ctx, projectName, source, company)
}

func (s *stubStore) DetachProjectSourceOwner(ctx context.Context, projectName string, source string, company string) error {
	if s.detachProjectOwnerFn == nil {
		return nil
	}
	return s.detachProjectOwnerFn(ctx, projectName, source, company)
}

func (s *stubStore) ListProjectSourceOwners(ctx context.Context, projectName string) ([]models.SourceOwner, error) {
	if s.listProjectOwnersFn == nil {
		return nil, nil
	}
	return s.listProjectOwnersFn(ctx, projectName)
}

func (s *stubStore) GetSourceOwnerEvents(ctx context.Context, source string, company string) (*models.SourceOwner, []store.ChildEventRow, error) {
	if s.getSourceOwnerEventsFn == nil {
		return nil, nil, nil
	}
	return s.getSourceOwnerEventsFn(ctx, source, company)
}

func (s *stubStore) GetProjectOwnerEvents(ctx context.Context, projectName string, source string, company string) (*models.SourceOwner, []store.ChildEventRow, error) {
	if s.getProjectOwnerEventsFn == nil {
		return nil, nil, nil
	}
	return s.getProjectOwnerEventsFn(ctx, projectName, source, company)
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

func newStaticIngestionToken(t *testing.T) string {
	t.Helper()
	manager, err := auth.NewManager("top-secret", "issuer", "subject", 0)
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
		createSourceFn: func(_ context.Context, source string, company string, city string, state string, country string, websiteDomain string, schema models.TableSchema) (*models.Source, error) {
			return &models.Source{
				ID:             1,
				SourceOwnerID:  9,
				Source:         source,
				Company:        company,
				WebsiteDomain:  websiteDomain,
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
		"source":        "Events",
		"company":       "acme",
		"city":          "Boston",
		"state":         "Massachusetts",
		"country":       "United States",
		"websiteDomain": "acme.test",
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
	if !bytes.Contains(recorder.Body.Bytes(), []byte(`"websiteDomain":"acme.test"`)) {
		t.Fatalf("body = %s, want websiteDomain in create source response", recorder.Body.String())
	}
}

func TestCreateSourceDuplicate(t *testing.T) {
	server := newTestServer(t, &stubStore{
		createSourceFn: func(context.Context, string, string, string, string, string, string, models.TableSchema) (*models.Source, error) {
			return nil, store.ErrDuplicateSource
		},
	})

	req := httptest.NewRequest(http.MethodPost, "/source", bytes.NewReader([]byte(`{"source":"Events","company":"acme","city":"Boston","state":"Massachusetts","country":"United States","websiteDomain":"acme.test","tableSchema":[{"name":"invoice_number","type":"text","required":true}]}`)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+newAccessToken(t))

	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusConflict {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestCreateSourceRequiresWebsiteDomain(t *testing.T) {
	server := newTestServer(t, &stubStore{})

	req := httptest.NewRequest(http.MethodPost, "/source", bytes.NewReader([]byte(`{"source":"Events","company":"acme","city":"Boston","state":"Massachusetts","country":"United States","tableSchema":[{"name":"invoice_number","type":"text","required":true}]}`)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+newAccessToken(t))

	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	if !bytes.Contains(recorder.Body.Bytes(), []byte(`websiteDomain`)) {
		t.Fatalf("body = %s, want websiteDomain validation error", recorder.Body.String())
	}
}

func TestCreateSourceInvalidSource(t *testing.T) {
	server := newTestServer(t, &stubStore{
		createSourceFn: func(context.Context, string, string, string, string, string, string, models.TableSchema) (*models.Source, error) {
			return nil, store.ErrInvalidSource
		},
	})

	req := httptest.NewRequest(http.MethodPost, "/source", bytes.NewReader([]byte(`{"source":"stripe","company":"acme","city":"Boston","state":"Massachusetts","country":"United States","websiteDomain":"acme.test","tableSchema":[{"name":"invoice_number","type":"text","required":true}]}`)))
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
				SourceOwnerID:  9,
				Source:         "Events",
				Company:        "acme",
				WebsiteDomain:  "acme.test",
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
	if !bytes.Contains(recorder.Body.Bytes(), []byte(`"websiteDomain":"acme.test"`)) {
		t.Fatalf("body = %s, want websiteDomain in list sources response", recorder.Body.String())
	}
}

func TestCreateProjectSuccess(t *testing.T) {
	ingestionJWT := newIngestionToken(t)
	server := newTestServer(t, &stubStore{
		createProjectFn: func(_ context.Context, projectName string, storedJWT string, ingestionJWTExpiresAt *time.Time) (*models.Project, error) {
			if projectName != "Alpha1" {
				t.Fatalf("projectName = %q, want %q", projectName, "Alpha1")
			}
			if storedJWT != ingestionJWT {
				t.Fatalf("ingestionJWT = %q, want %q", storedJWT, ingestionJWT)
			}
			if ingestionJWTExpiresAt == nil {
				t.Fatal("ingestionJWTExpiresAt = nil, want non-nil")
			}
			return &models.Project{
				ID:                    1,
				ProjectName:           projectName,
				IngestionJWT:          storedJWT,
				IngestionJWTExpiresAt: ingestionJWTExpiresAt,
				CreatedAt:             time.Date(2026, time.March, 8, 0, 0, 0, 0, time.UTC),
				UpdatedAt:             time.Date(2026, time.March, 8, 0, 0, 0, 0, time.UTC),
			}, nil
		},
	})

	req := httptest.NewRequest(http.MethodPost, "/projects", bytes.NewReader([]byte(`{"projectName":"Alpha1","ingestionJwt":"`+ingestionJWT+`"}`)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+newAccessToken(t))

	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestCreateProjectRejectsInvalidIngestionJWT(t *testing.T) {
	server := newTestServer(t, &stubStore{})

	req := httptest.NewRequest(http.MethodPost, "/projects", bytes.NewReader([]byte(`{"projectName":"Alpha1","ingestionJwt":"invalid"}`)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+newAccessToken(t))

	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestCreateProjectDuplicate(t *testing.T) {
	server := newTestServer(t, &stubStore{
		createProjectFn: func(context.Context, string, string, *time.Time) (*models.Project, error) {
			return nil, store.ErrDuplicateProject
		},
	})

	req := httptest.NewRequest(http.MethodPost, "/projects", bytes.NewReader([]byte(`{"projectName":"Alpha1","ingestionJwt":"`+newIngestionToken(t)+`"}`)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+newAccessToken(t))

	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusConflict {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestListProjectsSuccess(t *testing.T) {
	server := newTestServer(t, &stubStore{
		listProjectsFn: func(context.Context) ([]models.Project, error) {
			return []models.Project{{
				ID:           1,
				ProjectName:  "Alpha1",
				IngestionJWT: "jwt-token",
				CreatedAt:    time.Date(2026, time.March, 8, 0, 0, 0, 0, time.UTC),
				UpdatedAt:    time.Date(2026, time.March, 8, 0, 0, 0, 0, time.UTC),
			}}, nil
		},
		listProjectOwnersFn: func(context.Context, string) ([]models.SourceOwner, error) {
			return []models.SourceOwner{{
				ID:             7,
				Source:         "Events",
				Company:        "Acme",
				ChildTableName: "events_events_acme",
				TableSchema: models.TableSchema{
					{Name: "invoice_number", Type: "text", Required: true},
				},
			}}, nil
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/projects", nil)
	req.Header.Set("Authorization", "Bearer "+newAccessToken(t))

	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestDeleteProjectSuccess(t *testing.T) {
	server := newTestServer(t, &stubStore{
		deleteProjectFn: func(_ context.Context, projectName string) error {
			if projectName != "Alpha1" {
				t.Fatalf("projectName = %q, want %q", projectName, "Alpha1")
			}
			return nil
		},
	})

	req := httptest.NewRequest(http.MethodDelete, "/projects/Alpha1", nil)
	req.Header.Set("Authorization", "Bearer "+newAccessToken(t))

	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestDeleteProjectNotFound(t *testing.T) {
	server := newTestServer(t, &stubStore{
		deleteProjectFn: func(context.Context, string) error {
			return store.ErrProjectNotFound
		},
	})

	req := httptest.NewRequest(http.MethodDelete, "/projects/Alpha1", nil)
	req.Header.Set("Authorization", "Bearer "+newAccessToken(t))

	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestAttachProjectOwnerSuccess(t *testing.T) {
	server := newTestServer(t, &stubStore{
		attachProjectOwnerFn: func(_ context.Context, projectName string, source string, company string) (*models.SourceOwner, error) {
			if projectName != "Alpha1" || source != "Events" || company != "Acme" {
				t.Fatalf("unexpected attach params: %q %q %q", projectName, source, company)
			}
			return &models.SourceOwner{
				ID:             7,
				Source:         source,
				Company:        company,
				ChildTableName: "events_events_acme",
				TableSchema: models.TableSchema{
					{Name: "invoice_number", Type: "text", Required: true},
				},
			}, nil
		},
	})

	req := httptest.NewRequest(http.MethodPost, "/projects/Alpha1/owners", bytes.NewReader([]byte(`{"source":"Events","company":"Acme"}`)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+newAccessToken(t))

	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestListProjectOwnersSuccess(t *testing.T) {
	server := newTestServer(t, &stubStore{
		listProjectOwnersFn: func(_ context.Context, projectName string) ([]models.SourceOwner, error) {
			if projectName != "Alpha1" {
				t.Fatalf("projectName = %q, want %q", projectName, "Alpha1")
			}
			return []models.SourceOwner{{
				ID:             7,
				Source:         "Events",
				Company:        "Acme",
				ChildTableName: "events_events_acme",
				TableSchema: models.TableSchema{
					{Name: "invoice_number", Type: "text", Required: true},
				},
			}}, nil
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/projects/Alpha1/owners", nil)
	req.Header.Set("Authorization", "Bearer "+newAccessToken(t))

	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestDeleteProjectOwnerSuccess(t *testing.T) {
	server := newTestServer(t, &stubStore{
		detachProjectOwnerFn: func(_ context.Context, projectName string, source string, company string) error {
			if projectName != "Alpha1" {
				t.Fatalf("projectName = %q, want %q", projectName, "Alpha1")
			}
			if source != "Events" {
				t.Fatalf("source = %q, want %q", source, "Events")
			}
			if company != "Acme" {
				t.Fatalf("company = %q, want %q", company, "Acme")
			}
			return nil
		},
	})

	req := httptest.NewRequest(http.MethodDelete, "/projects/Alpha1/owners?source=Events&company=Acme", nil)
	req.Header.Set("Authorization", "Bearer "+newAccessToken(t))

	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestDeleteProjectOwnerNotFound(t *testing.T) {
	server := newTestServer(t, &stubStore{
		detachProjectOwnerFn: func(_ context.Context, projectName string, source string, company string) error {
			return store.ErrProjectOwnerLinkNotFound
		},
	})

	req := httptest.NewRequest(http.MethodDelete, "/projects/Alpha1/owners?source=Events&company=Acme", nil)
	req.Header.Set("Authorization", "Bearer "+newAccessToken(t))

	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestDeleteProjectOwnerBadRequest(t *testing.T) {
	server := newTestServer(t, &stubStore{})

	req := httptest.NewRequest(http.MethodDelete, "/projects/Alpha1/owners?source=Events", nil)
	req.Header.Set("Authorization", "Bearer "+newAccessToken(t))

	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestGetProjectDashboardSuccess(t *testing.T) {
	server := newTestServer(t, &stubStore{
		getProjectOwnerEventsFn: func(_ context.Context, projectName string, source string, company string) (*models.SourceOwner, []store.ChildEventRow, error) {
			if projectName != "Alpha1" {
				t.Fatalf("projectName = %q, want %q", projectName, "Alpha1")
			}
			if source != "Events" {
				t.Fatalf("source = %q, want %q", source, "Events")
			}
			if company != "Acme" {
				t.Fatalf("company = %q, want %q", company, "Acme")
			}

			return &models.SourceOwner{
					ID:             7,
					Source:         source,
					Company:        company,
					ChildTableName: "events_events_acme",
					TableSchema: models.TableSchema{
						{Name: "invoice_number", Type: "text", Required: true},
						{Name: "status", Type: "text", Required: false},
					},
				}, []store.ChildEventRow{
					{
						ID:        1,
						Source:    source,
						Company:   company,
						City:      "Boston",
						State:     "Massachusetts",
						Country:   "United States",
						CreatedAt: time.Now().UTC().Add(-2 * time.Hour),
						Payload: map[string]any{
							"invoice_number": "INV-1",
							"status":         "paid",
						},
					},
				}, nil
		},
		listProjectsFn: func(context.Context) ([]models.Project, error) {
			return []models.Project{
				{ID: 1, ProjectName: "Alpha1"},
				{ID: 2, ProjectName: "Beta2"},
			}, nil
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/projects/Alpha1/dashboard?source=Events&company=Acme", nil)
	req.Header.Set("Authorization", "Bearer "+newIngestionToken(t))

	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}

	var payload DashboardAnalytics
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if payload.TopMetrics.TotalRecords != 1 {
		t.Fatalf("totalRecords = %d, want 1", payload.TopMetrics.TotalRecords)
	}
	if payload.TopMetrics.ProjectsCount != 2 {
		t.Fatalf("projectsCount = %d, want 2", payload.TopMetrics.ProjectsCount)
	}
	if len(payload.Coverage.Fields) == 0 {
		t.Fatal("coverage fields = 0, want non-zero")
	}
}

func TestGetProjectDashboardNotFound(t *testing.T) {
	server := newTestServer(t, &stubStore{
		getProjectOwnerEventsFn: func(context.Context, string, string, string) (*models.SourceOwner, []store.ChildEventRow, error) {
			return nil, nil, store.ErrProjectOwnerLinkNotFound
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/projects/Alpha1/dashboard?source=Events&company=Acme", nil)
	req.Header.Set("Authorization", "Bearer "+newIngestionToken(t))

	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestGetProjectDashboardBadRequest(t *testing.T) {
	server := newTestServer(t, &stubStore{})

	req := httptest.NewRequest(http.MethodGet, "/projects/Alpha1/dashboard?source=Events", nil)
	req.Header.Set("Authorization", "Bearer "+newIngestionToken(t))

	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestGetSourceCompanyAnalyticsSuccess(t *testing.T) {
	server := newTestServer(t, &stubStore{
		getSourceOwnerEventsFn: func(_ context.Context, source string, company string) (*models.SourceOwner, []store.ChildEventRow, error) {
			if source != "Flights" {
				t.Fatalf("source = %q, want %q", source, "Flights")
			}
			if company != "Delta Air Lines" {
				t.Fatalf("company = %q, want %q", company, "Delta Air Lines")
			}

			now := time.Date(2026, time.March, 10, 12, 0, 0, 0, time.UTC)
			return &models.SourceOwner{
					ID:             1,
					Source:         source,
					Company:        company,
					ChildTableName: "events_flights_delta_air_lines",
				}, []store.ChildEventRow{
					{
						ID:        1,
						Source:    source,
						Company:   company,
						CreatedAt: now,
						Payload: map[string]any{
							"status":                 "scheduled",
							"origin_iata":            "ATL",
							"destination_iata":       "LHR",
							"scheduled_departure_at": now,
							"actual_departure_at":    now.Add(10 * time.Minute),
							"scheduled_arrival_at":   now.Add(8 * time.Hour),
							"actual_arrival_at":      now.Add(8*time.Hour + 20*time.Minute),
						},
					},
					{
						ID:        2,
						Source:    source,
						Company:   company,
						CreatedAt: now.Add(30 * time.Minute),
						Payload: map[string]any{
							"status":                 "delayed",
							"origin_iata":            "ATL",
							"destination_iata":       "CDG",
							"scheduled_departure_at": now.Add(time.Hour),
							"actual_departure_at":    now.Add(90 * time.Minute),
							"scheduled_arrival_at":   now.Add(9 * time.Hour),
							"actual_arrival_at":      now.Add(9*time.Hour + 25*time.Minute),
						},
					},
				}, nil
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/analytics/Flights/Delta%20Air%20Lines", nil)
	req.Header.Set("Authorization", "Bearer "+newIngestionToken(t))

	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}

	var payload SourceCompanyAnalytics
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if payload.Source != "Flights" {
		t.Fatalf("source = %q, want %q", payload.Source, "Flights")
	}
	if payload.Company != "Delta Air Lines" {
		t.Fatalf("company = %q, want %q", payload.Company, "Delta Air Lines")
	}
	if payload.TotalRecords != 2 {
		t.Fatalf("totalRecords = %d, want 2", payload.TotalRecords)
	}
	if len(payload.Charts) != 6 {
		t.Fatalf("charts len = %d, want 6", len(payload.Charts))
	}
	if payload.Charts[0].Kind != "donut" {
		t.Fatalf("charts[0].kind = %q, want %q", payload.Charts[0].Kind, "donut")
	}
	if payload.Charts[2].Kind != "route-map" {
		t.Fatalf("charts[2].kind = %q, want %q", payload.Charts[2].Kind, "route-map")
	}
	if payload.Charts[2].Items[0].FromCode != "ATL" || payload.Charts[2].Items[0].ToCode != "CDG" {
		t.Fatalf("route item = %q -> %q, want ATL -> CDG", payload.Charts[2].Items[0].FromCode, payload.Charts[2].Items[0].ToCode)
	}
}

func TestGetSourceCompanyAnalyticsUsesReferenceAirportCoordinates(t *testing.T) {
	server := newTestServer(t, &stubStore{
		getSourceOwnerEventsFn: func(_ context.Context, source string, company string) (*models.SourceOwner, []store.ChildEventRow, error) {
			now := time.Date(2026, time.March, 15, 12, 0, 0, 0, time.UTC)
			return &models.SourceOwner{
					ID:             25,
					Source:         source,
					Company:        company,
					ChildTableName: "events_flights_indigo",
				}, []store.ChildEventRow{
					{
						ID:        1,
						Source:    source,
						Company:   company,
						CreatedAt: now,
						Payload: map[string]any{
							"status":                 "boarding",
							"origin_iata":            "DEL",
							"destination_iata":       "BLR",
							"scheduled_departure_at": now,
							"actual_departure_at":    now.Add(10 * time.Minute),
							"scheduled_arrival_at":   now.Add(2 * time.Hour),
							"actual_arrival_at":      now.Add(2 * time.Hour),
						},
					},
				}, nil
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/analytics/Flights/Indigo", nil)
	req.Header.Set("Authorization", "Bearer "+newIngestionToken(t))

	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}

	var payload SourceCompanyAnalytics
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	if len(payload.Charts) < 3 || payload.Charts[2].Kind != "route-map" {
		t.Fatalf("route map chart missing: %+v", payload.Charts)
	}
	if len(payload.Charts[2].Items) != 1 {
		t.Fatalf("route map items len = %d, want 1", len(payload.Charts[2].Items))
	}
	if payload.Charts[2].Items[0].FromCode != "DEL" || payload.Charts[2].Items[0].ToCode != "BLR" {
		t.Fatalf("route item = %q -> %q, want DEL -> BLR", payload.Charts[2].Items[0].FromCode, payload.Charts[2].Items[0].ToCode)
	}
	if payload.Charts[2].Items[0].FromLat == 0 || payload.Charts[2].Items[0].FromLng == 0 {
		t.Fatalf("from coordinates missing: %+v", payload.Charts[2].Items[0])
	}
	if payload.Charts[2].Items[0].ToLat == 0 || payload.Charts[2].Items[0].ToLng == 0 {
		t.Fatalf("to coordinates missing: %+v", payload.Charts[2].Items[0])
	}
}

func TestGetSourceCompanyAnalyticsNotFound(t *testing.T) {
	server := newTestServer(t, &stubStore{
		getSourceOwnerEventsFn: func(context.Context, string, string) (*models.SourceOwner, []store.ChildEventRow, error) {
			return nil, nil, store.ErrSourceOwnerNotFound
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/analytics/Flights/Unknown", nil)
	req.Header.Set("Authorization", "Bearer "+newIngestionToken(t))

	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestUpdateProjectIngestionJWTSuccess(t *testing.T) {
	ingestionJWT := newIngestionToken(t)
	server := newTestServer(t, &stubStore{
		updateProjectJWTFn: func(_ context.Context, projectName string, storedJWT string, ingestionJWTExpiresAt *time.Time) (*models.Project, error) {
			if projectName != "Alpha1" {
				t.Fatalf("projectName = %q, want %q", projectName, "Alpha1")
			}
			if storedJWT != ingestionJWT {
				t.Fatalf("ingestionJWT = %q, want %q", storedJWT, ingestionJWT)
			}
			if ingestionJWTExpiresAt == nil {
				t.Fatal("ingestionJWTExpiresAt = nil, want non-nil")
			}
			return &models.Project{
				ID:                    1,
				ProjectName:           projectName,
				IngestionJWT:          storedJWT,
				IngestionJWTExpiresAt: ingestionJWTExpiresAt,
				CreatedAt:             time.Date(2026, time.March, 8, 0, 0, 0, 0, time.UTC),
				UpdatedAt:             time.Date(2026, time.March, 8, 1, 0, 0, 0, time.UTC),
			}, nil
		},
	})

	req := httptest.NewRequest(http.MethodPut, "/projects/Alpha1/ingestion-jwt", bytes.NewReader([]byte(`{"ingestionJwt":"`+ingestionJWT+`"}`)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+newAccessToken(t))

	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestUpdateAllProjectsIngestionJWT(t *testing.T) {
	ingestionJWT := newIngestionToken(t)
	expiresAt := time.Now().UTC().Add(30 * time.Minute)
	updateCalls := 0
	server := newTestServer(t, &stubStore{
		listProjectsFn: func(context.Context) ([]models.Project, error) {
			return []models.Project{{
				ID:                    1,
				ProjectName:           "Alpha1",
				IngestionJWT:          ingestionJWT,
				IngestionJWTExpiresAt: &expiresAt,
				CreatedAt:             time.Date(2026, time.March, 8, 0, 0, 0, 0, time.UTC),
				UpdatedAt:             time.Date(2026, time.March, 8, 1, 0, 0, 0, time.UTC),
			}, {
				ID:          2,
				ProjectName: "Beta2",
				CreatedAt:   time.Date(2026, time.March, 8, 0, 0, 0, 0, time.UTC),
				UpdatedAt:   time.Date(2026, time.March, 8, 1, 0, 0, 0, time.UTC),
			}}, nil
		},
		updateProjectJWTFn: func(_ context.Context, projectName string, candidate string, candidateExpiresAt *time.Time) (*models.Project, error) {
			updateCalls += 1
			return &models.Project{
				ProjectName:           projectName,
				IngestionJWT:          candidate,
				IngestionJWTExpiresAt: candidateExpiresAt,
			}, nil
		},
		listProjectOwnersFn: func(context.Context, string) ([]models.SourceOwner, error) {
			return []models.SourceOwner{{
				ID:      7,
				Source:  "Events",
				Company: "Acme",
			}}, nil
		},
	})

	req := httptest.NewRequest(http.MethodPut, "/projects/ingestion-jwt", bytes.NewReader([]byte(`{"ingestionJwt":"`+ingestionJWT+`"}`)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+newAccessToken(t))
	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	if updateCalls != 2 {
		t.Fatalf("updateCalls = %d, want 2", updateCalls)
	}
}

func TestUpdateAllProjectsIngestionJWTRejectsNonExpiringToken(t *testing.T) {
	server := newTestServer(t, &stubStore{})

	req := httptest.NewRequest(http.MethodPut, "/projects/ingestion-jwt", bytes.NewReader([]byte(`{"ingestionJwt":"`+newStaticIngestionToken(t)+`"}`)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+newAccessToken(t))
	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestBootstrapProjectSuccess(t *testing.T) {
	ingestionJWT := newIngestionToken(t)
	expiresAt := time.Now().UTC().Add(30 * time.Minute)
	server := newTestServer(t, &stubStore{
		listProjectsFn: func(context.Context) ([]models.Project, error) {
			return []models.Project{{
				ID:                    1,
				ProjectName:           "Alpha1",
				IngestionJWT:          ingestionJWT,
				IngestionJWTExpiresAt: &expiresAt,
				CreatedAt:             time.Date(2026, time.March, 8, 0, 0, 0, 0, time.UTC),
				UpdatedAt:             time.Date(2026, time.March, 8, 1, 0, 0, 0, time.UTC),
			}}, nil
		},
		listSourcesFn: func(context.Context) ([]models.Source, error) {
			return []models.Source{{
				ID:            1,
				Source:        "Events",
				Company:       "Acme",
				WebsiteDomain: "acme.test",
			}}, nil
		},
		listProjectOwnersFn: func(context.Context, string) ([]models.SourceOwner, error) {
			return []models.SourceOwner{{
				ID:      7,
				Source:  "Events",
				Company: "Acme",
			}}, nil
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/projects/bootstrap", nil)
	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	if !bytes.Contains(recorder.Body.Bytes(), []byte(`"needsAccessJwt":false`)) {
		t.Fatalf("body = %s, want needsAccessJwt false", recorder.Body.String())
	}
	if !bytes.Contains(recorder.Body.Bytes(), []byte(`"accessJwt":"eyJ`)) {
		t.Fatalf("body = %s, want access JWT in bootstrap", recorder.Body.String())
	}
	if !bytes.Contains(recorder.Body.Bytes(), []byte(`"sources":[{"id":1`)) {
		t.Fatalf("body = %s, want sources in bootstrap", recorder.Body.String())
	}
	if !bytes.Contains(recorder.Body.Bytes(), []byte(`"websiteDomain":"acme.test"`)) {
		t.Fatalf("body = %s, want websiteDomain in bootstrap sources", recorder.Body.String())
	}
}

func TestBootstrapProjectRepairsInvalidProjectsFromReusableJWT(t *testing.T) {
	ingestionJWT := newIngestionToken(t)
	expiresAt := time.Now().UTC().Add(30 * time.Minute)
	expiredAt := time.Now().UTC().Add(-30 * time.Minute)
	updateCalls := 0
	listCalls := 0
	server := newTestServer(t, &stubStore{
		listProjectsFn: func(context.Context) ([]models.Project, error) {
			listCalls += 1
			if listCalls == 1 {
				return []models.Project{{
					ID:                    1,
					ProjectName:           "Alpha1",
					IngestionJWT:          ingestionJWT,
					IngestionJWTExpiresAt: &expiresAt,
				}, {
					ID:                    2,
					ProjectName:           "Beta2",
					IngestionJWT:          ingestionJWT,
					IngestionJWTExpiresAt: &expiredAt,
				}}, nil
			}

			return []models.Project{{
				ID:                    1,
				ProjectName:           "Alpha1",
				IngestionJWT:          ingestionJWT,
				IngestionJWTExpiresAt: &expiresAt,
			}, {
				ID:                    2,
				ProjectName:           "Beta2",
				IngestionJWT:          ingestionJWT,
				IngestionJWTExpiresAt: &expiresAt,
			}}, nil
		},
		listSourcesFn: func(context.Context) ([]models.Source, error) {
			return nil, nil
		},
		updateProjectJWTFn: func(_ context.Context, projectName string, candidate string, candidateExpiresAt *time.Time) (*models.Project, error) {
			updateCalls += 1
			if projectName != "Beta2" {
				t.Fatalf("projectName = %s, want Beta2", projectName)
			}
			return &models.Project{
				ProjectName:           projectName,
				IngestionJWT:          candidate,
				IngestionJWTExpiresAt: candidateExpiresAt,
			}, nil
		},
		listProjectOwnersFn: func(context.Context, string) ([]models.SourceOwner, error) {
			return nil, nil
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/projects/bootstrap", nil)
	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	if updateCalls != 1 {
		t.Fatalf("updateCalls = %d, want 1", updateCalls)
	}
}

func TestBootstrapProjectExpiredJWTMasksStoredToken(t *testing.T) {
	expiredAt := time.Now().UTC().Add(-30 * time.Minute)
	server := newTestServer(t, &stubStore{
		listProjectsFn: func(context.Context) ([]models.Project, error) {
			return []models.Project{{
				ID:                    1,
				ProjectName:           "Alpha1",
				IngestionJWT:          newIngestionToken(t),
				IngestionJWTExpiresAt: &expiredAt,
			}}, nil
		},
		listSourcesFn: func(context.Context) ([]models.Source, error) {
			return nil, nil
		},
		listProjectOwnersFn: func(context.Context, string) ([]models.SourceOwner, error) {
			return nil, nil
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/projects/bootstrap", nil)
	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	if !bytes.Contains(recorder.Body.Bytes(), []byte(`"needsAccessJwt":true`)) {
		t.Fatalf("body = %s, want needsAccessJwt true", recorder.Body.String())
	}
	if bytes.Contains(recorder.Body.Bytes(), []byte(`"ingestionJwt":"eyJ`)) {
		t.Fatalf("body = %s, want stored ingestion JWT masked out", recorder.Body.String())
	}
}

func TestBootstrapProjectMissingExpiryMasksStoredToken(t *testing.T) {
	server := newTestServer(t, &stubStore{
		listProjectsFn: func(context.Context) ([]models.Project, error) {
			return []models.Project{{
				ID:           1,
				ProjectName:  "Alpha1",
				IngestionJWT: newStaticIngestionToken(t),
			}}, nil
		},
		listSourcesFn: func(context.Context) ([]models.Source, error) {
			return nil, nil
		},
		listProjectOwnersFn: func(context.Context, string) ([]models.SourceOwner, error) {
			return nil, nil
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/projects/bootstrap", nil)
	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	if !bytes.Contains(recorder.Body.Bytes(), []byte(`"needsAccessJwt":true`)) {
		t.Fatalf("body = %s, want needsAccessJwt true", recorder.Body.String())
	}
	if bytes.Contains(recorder.Body.Bytes(), []byte(`"ingestionJwt":"eyJ`)) {
		t.Fatalf("body = %s, want stored ingestion JWT masked out", recorder.Body.String())
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
