package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/macho_prawn/events-dashboard/internal/models"
	"github.com/macho_prawn/events-dashboard/internal/testutil"
	"gorm.io/gorm"
)

func TestNormalizeProjectName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		input     string
		want      string
		wantError error
	}{
		{name: "valid", input: "Acme2026", want: "Acme2026"},
		{name: "trimmed", input: "  Alpha1  ", want: "Alpha1"},
		{name: "empty", input: "   ", wantError: ErrInvalidProject},
		{name: "too long", input: "LongProject1", wantError: ErrInvalidProject},
		{name: "symbol", input: "Acme-1", wantError: ErrInvalidProject},
		{name: "space", input: "Acme 1", wantError: ErrInvalidProject},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got, err := normalizeProjectName(test.input)
			if !errors.Is(err, test.wantError) {
				t.Fatalf("normalizeProjectName(%q) error = %v, want %v", test.input, err, test.wantError)
			}
			if got != test.want {
				t.Fatalf("normalizeProjectName(%q) = %q, want %q", test.input, got, test.want)
			}
		})
	}
}

func TestCreateAndListProjects(t *testing.T) {
	databaseURL := testutil.NewIsolatedPostgresDatabase(t)

	projectStore, err := NewPostgresStore(databaseURL)
	if err != nil {
		t.Fatalf("NewPostgresStore() error = %v", err)
	}
	t.Cleanup(func() {
		if err := projectStore.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})

	if err := projectStore.AutoMigrate(); err != nil {
		t.Fatalf("AutoMigrate() error = %v", err)
	}

	ctx := context.Background()
	firstExpiry := time.Now().UTC().Add(30 * time.Minute)
	secondExpiry := time.Now().UTC().Add(45 * time.Minute)

	first, err := projectStore.CreateProject(ctx, "Alpha9", "jwt-one", &firstExpiry)
	if err != nil {
		t.Fatalf("CreateProject(first) error = %v", err)
	}
	if first.ProjectName != "Alpha9" {
		t.Fatalf("first.ProjectName = %q, want %q", first.ProjectName, "Alpha9")
	}
	if first.IngestionJWTExpiresAt == nil || !first.IngestionJWTExpiresAt.Equal(firstExpiry) {
		t.Fatalf("first.IngestionJWTExpiresAt = %v, want %v", first.IngestionJWTExpiresAt, firstExpiry)
	}

	second, err := projectStore.CreateProject(ctx, "Beta2", "jwt-two", &secondExpiry)
	if err != nil {
		t.Fatalf("CreateProject(second) error = %v", err)
	}
	if second.ProjectName != "Beta2" {
		t.Fatalf("second.ProjectName = %q, want %q", second.ProjectName, "Beta2")
	}

	if _, err := projectStore.CreateProject(ctx, "Alpha9", "jwt-three", &secondExpiry); !errors.Is(err, ErrDuplicateProject) {
		t.Fatalf("CreateProject(duplicate) error = %v, want %v", err, ErrDuplicateProject)
	}

	if _, err := projectStore.CreateProject(ctx, "bad name", "jwt-four", &secondExpiry); !errors.Is(err, ErrInvalidProject) {
		t.Fatalf("CreateProject(invalid name) error = %v, want %v", err, ErrInvalidProject)
	}

	if _, err := projectStore.CreateProject(ctx, "Gamma3", "   ", &secondExpiry); !errors.Is(err, ErrInvalidProject) {
		t.Fatalf("CreateProject(empty jwt) error = %v, want %v", err, ErrInvalidProject)
	}

	projects, err := projectStore.ListProjects(ctx)
	if err != nil {
		t.Fatalf("ListProjects() error = %v", err)
	}

	if len(projects) != 2 {
		t.Fatalf("len(projects) = %d, want 2", len(projects))
	}
	if projects[0].ProjectName != "Alpha9" || projects[1].ProjectName != "Beta2" {
		t.Fatalf("project order = %#v, want Alpha9 then Beta2", []models.Project{projects[0], projects[1]})
	}

	updatedExpiry := time.Now().UTC().Add(time.Hour)
	updated, err := projectStore.UpdateProjectIngestionJWT(ctx, "Alpha9", "jwt-updated", &updatedExpiry)
	if err != nil {
		t.Fatalf("UpdateProjectIngestionJWT() error = %v", err)
	}
	if updated.IngestionJWT != "jwt-updated" {
		t.Fatalf("updated.IngestionJWT = %q, want %q", updated.IngestionJWT, "jwt-updated")
	}
	if updated.IngestionJWTExpiresAt == nil || !updated.IngestionJWTExpiresAt.Equal(updatedExpiry) {
		t.Fatalf("updated.IngestionJWTExpiresAt = %v, want %v", updated.IngestionJWTExpiresAt, updatedExpiry)
	}

	latest, err := projectStore.GetLatestProject(ctx)
	if err != nil {
		t.Fatalf("GetLatestProject() error = %v", err)
	}
	if latest == nil || latest.ProjectName != "Alpha9" {
		t.Fatalf("latest = %#v, want Alpha9", latest)
	}

	if _, err := projectStore.UpdateProjectIngestionJWT(ctx, "Missing1", "jwt-updated", &updatedExpiry); !errors.Is(err, ErrProjectNotFound) {
		t.Fatalf("UpdateProjectIngestionJWT(missing) error = %v, want %v", err, ErrProjectNotFound)
	}
}

func TestEnsureSourceOwnerTxPreservesExistingWebsiteDomainDuringBackfill(t *testing.T) {
	databaseURL := testutil.NewIsolatedPostgresDatabase(t)

	projectStore, err := NewPostgresStore(databaseURL)
	if err != nil {
		t.Fatalf("NewPostgresStore() error = %v", err)
	}
	t.Cleanup(func() {
		if err := projectStore.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})

	if err := projectStore.AutoMigrate(); err != nil {
		t.Fatalf("AutoMigrate() error = %v", err)
	}

	schema := models.TableSchema{
		{Name: "flight_id", Type: "text", Required: true},
	}

	err = projectStore.db.Transaction(func(tx *gorm.DB) error {
		owner := models.SourceOwner{
			Source:         "Flights",
			Company:        "Indigo",
			WebsiteDomain:  "goindigo.in",
			ChildTableName: "events_flights_indigo",
			TableSchema:    schema,
		}
		if err := tx.Create(&owner).Error; err != nil {
			return err
		}

		updatedOwner, err := ensureSourceOwnerTx(tx, "Flights", "Indigo", "", "events_flights_indigo", schema)
		if err != nil {
			return err
		}
		if updatedOwner.WebsiteDomain != "goindigo.in" {
			t.Fatalf("updatedOwner.WebsiteDomain = %q, want %q", updatedOwner.WebsiteDomain, "goindigo.in")
		}

		var reloaded models.SourceOwner
		if err := tx.Where("id = ?", owner.ID).First(&reloaded).Error; err != nil {
			return err
		}
		if reloaded.WebsiteDomain != "goindigo.in" {
			t.Fatalf("reloaded.WebsiteDomain = %q, want %q", reloaded.WebsiteDomain, "goindigo.in")
		}

		return nil
	})
	if err != nil {
		t.Fatalf("transaction error = %v", err)
	}
}
