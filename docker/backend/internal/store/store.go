package store

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/macho_prawn/events-dashboard/internal/models"
	"github.com/macho_prawn/events-dashboard/internal/reference"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var (
	ErrDuplicateSource          = errors.New("duplicate source")
	ErrDuplicateEvent           = errors.New("duplicate event")
	ErrDuplicateProject         = errors.New("duplicate project")
	ErrDuplicateProjectLink     = errors.New("duplicate project source owner link")
	ErrProjectOwnerLinkNotFound = errors.New("project source owner link not found")
	ErrProjectNotFound          = errors.New("project not found")
	ErrSourceNotFound           = errors.New("source not found")
	ErrSourceOwnerNotFound      = errors.New("source owner not found")
	ErrSourceSchemaMismatch     = errors.New("source schema mismatch")
	ErrInvalidTableSchema       = errors.New("invalid table schema")
	ErrInvalidSource            = errors.New("invalid source")
	ErrInvalidSourceOwner       = errors.New("invalid source/company")
	ErrInvalidWebsiteDomain     = errors.New("invalid website domain")
	ErrInvalidPayload           = errors.New("invalid payload")
	ErrInvalidLocation          = errors.New("invalid city/state/country")
	ErrInvalidProject           = errors.New("invalid project")
)

var (
	identifierPattern = regexp.MustCompile(`[^a-z0-9_]+`)
	allowedTypes      = map[string]string{
		"text":        "TEXT",
		"integer":     "INTEGER",
		"bigint":      "BIGINT",
		"boolean":     "BOOLEAN",
		"numeric":     "NUMERIC",
		"timestamptz": "TIMESTAMPTZ",
		"jsonb":       "JSONB",
	}
	reservedColumns = map[string]struct{}{
		"id":               {},
		"source_parent_id": {},
		"source":           {},
		"company":          {},
		"city":             {},
		"state":            {},
		"country":          {},
		"created_at":       {},
	}
	allowedSources = map[string]struct{}{
		"Events":    {},
		"News":      {},
		"ECommerce": {},
		"Flights":   {},
	}
	projectNamePattern = regexp.MustCompile(`^[A-Za-z0-9]+$`)
	// Legacy-only defaults for backfill and compatibility with older source records.
	defaultWebsiteDomains = map[string]string{
		"ecommerce::amazon":           "amazon.com",
		"ecommerce::carousell":        "carousell.com",
		"ecommerce::ebay":             "ebay.com",
		"ecommerce::shopify":          "shopify.com",
		"ecommerce::target":           "target.com",
		"ecommerce::wayfair":          "wayfair.com",
		"events::bookmyshow":          "bookmyshow.com",
		"events::dice":                "dice.fm",
		"events::eventbrite":          "eventbrite.com",
		"events::eventim":             "eventim.de",
		"events::moshtix":             "moshtix.com.au",
		"events::sistic":              "sistic.com.sg",
		"flights::delta air lines":    "delta.com",
		"flights::emirates":           "emirates.com",
		"flights::qantas":             "qantas.com",
		"flights::singapore airlines": "singaporeair.com",
		"flights::southwest airlines": "southwest.com",
		"flights::united airlines":    "united.com",
		"news::africanews":            "africanews.com",
		"news::bbc":                   "bbc.com",
		"news::cna":                   "channelnewsasia.com",
		"news::gestion":               "gestion.pe",
		"news::sbs news":              "sbs.com.au",
		"news::the indian express":    "indianexpress.com",
	}
)

type EventStore interface {
	CreateSource(ctx context.Context, source string, company string, city string, state string, country string, websiteDomain string, schema models.TableSchema) (*models.Source, error)
	ListSources(ctx context.Context) ([]models.Source, error)
	CreateProject(ctx context.Context, projectName string, ingestionJWT string, ingestionJWTExpiresAt *time.Time) (*models.Project, error)
	ListProjects(ctx context.Context) ([]models.Project, error)
	GetLatestProject(ctx context.Context) (*models.Project, error)
	DeleteProject(ctx context.Context, projectName string) error
	UpdateProjectIngestionJWT(ctx context.Context, projectName string, ingestionJWT string, ingestionJWTExpiresAt *time.Time) (*models.Project, error)
	AttachProjectSourceOwner(ctx context.Context, projectName string, source string, company string) (*models.SourceOwner, error)
	DetachProjectSourceOwner(ctx context.Context, projectName string, source string, company string) error
	ListProjectSourceOwners(ctx context.Context, projectName string) ([]models.SourceOwner, error)
	GetSourceOwnerEvents(ctx context.Context, source string, company string) (*models.SourceOwner, []ChildEventRow, error)
	GetProjectOwnerEvents(ctx context.Context, projectName string, source string, company string) (*models.SourceOwner, []ChildEventRow, error)
	CreateEvent(ctx context.Context, source string, company string, city string, state string, country string, payload map[string]any) (*ChildEventRow, error)
	SearchEvents(ctx context.Context, source string, company string, city string, state string, country string, query string, page int, pageSize int) ([]ChildEventRow, int64, error)
	Ping(ctx context.Context) error
}

type ChildEventRow struct {
	ID             int64          `json:"id"`
	SourceParentID uint           `json:"sourceParentId"`
	Source         string         `json:"source"`
	Company        string         `json:"company"`
	City           string         `json:"city"`
	State          string         `json:"state"`
	Country        string         `json:"country"`
	CreatedAt      time.Time      `json:"createdAt"`
	Payload        map[string]any `json:"payload"`
}

type PostgresStore struct {
	db *gorm.DB
}

func NewPostgresStore(databaseURL string) (*PostgresStore, error) {
	db, err := gorm.Open(postgres.Open(databaseURL), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	return &PostgresStore{db: db}, nil
}

func (s *PostgresStore) AutoMigrate() error {
	if err := s.db.AutoMigrate(
		&models.APIKeyAccess{},
		&models.SourceOwner{},
		&models.Source{},
		&models.Project{},
		&models.ProjectSourceOwner{},
	); err != nil {
		return err
	}

	// Remove the legacy shared events table from the earlier design.
	if err := s.db.Exec("DROP TABLE IF EXISTS events").Error; err != nil {
		return err
	}

	if err := s.db.Exec("ALTER TABLE sources ADD COLUMN IF NOT EXISTS state TEXT").Error; err != nil {
		return err
	}
	if err := s.db.Exec("ALTER TABLE sources ADD COLUMN IF NOT EXISTS source_owner_id BIGINT").Error; err != nil {
		return err
	}
	if err := s.db.Exec("ALTER TABLE projects ADD COLUMN IF NOT EXISTS ingestion_jwt_expires_at TIMESTAMPTZ").Error; err != nil {
		return err
	}

	if err := s.migrateLegacySourceTables(context.Background()); err != nil {
		return err
	}
	if err := s.backfillSourceOwners(context.Background()); err != nil {
		return err
	}
	if err := s.backfillSourceOwnerWebsiteDomains(context.Background()); err != nil {
		return err
	}

	return s.enforceReplayProtection(context.Background())
}

func (s *PostgresStore) EnsureAPIKeyAccess(ctx context.Context, seed models.APIKeyAccess) (*models.APIKeyAccess, error) {
	var access models.APIKeyAccess
	err := s.db.WithContext(ctx).First(&access, 1).Error
	if err == nil {
		updated := false
		if access.AccessSigningSecret == "" {
			access.AccessSigningSecret = seed.AccessSigningSecret
			updated = true
		}
		if access.AccessIssuer == "" {
			access.AccessIssuer = seed.AccessIssuer
			updated = true
		}
		if access.AccessSubject == "" {
			access.AccessSubject = seed.AccessSubject
			updated = true
		}
		if access.IngestionSigningSecret == "" {
			access.IngestionSigningSecret = seed.IngestionSigningSecret
			updated = true
		}
		if access.IngestionIssuer == "" {
			access.IngestionIssuer = seed.IngestionIssuer
			updated = true
		}
		if access.IngestionSubject == "" {
			access.IngestionSubject = seed.IngestionSubject
			updated = true
		}
		if access.IngestionTTLSeconds <= 0 {
			access.IngestionTTLSeconds = seed.IngestionTTLSeconds
			updated = true
		}
		if updated {
			if err := s.db.WithContext(ctx).Save(&access).Error; err != nil {
				return nil, err
			}
		}
		return &access, nil
	}

	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}

	access = seed
	access.ID = 1
	if err := s.db.WithContext(ctx).Create(&access).Error; err != nil {
		return nil, err
	}

	return &access, nil
}

func (s *PostgresStore) CreateSource(ctx context.Context, source string, company string, city string, state string, country string, websiteDomain string, schema models.TableSchema) (*models.Source, error) {
	normalizedSchema, err := normalizeSchema(schema)
	if err != nil {
		return nil, err
	}
	normalizedWebsiteDomain, err := normalizeRequiredWebsiteDomain(websiteDomain)
	if err != nil {
		return nil, err
	}

	source, company, city, state, country, err = normalizeSourceIdentity(source, company, city, state, country)
	if err != nil {
		return nil, err
	}
	location, err := validateLocation(city, state, country)
	if err != nil {
		return nil, err
	}

	childTableName, err := buildChildTableName(source, company)
	if err != nil {
		return nil, err
	}

	record := &models.Source{
		SourceOwnerID:  0,
		Source:         source,
		Company:        company,
		City:           location.City,
		State:          location.State,
		Country:        location.CountryName,
		ChildTableName: childTableName,
		TableSchema:    normalizedSchema,
	}

	err = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var duplicate models.Source
		err := tx.Where("source = ? AND company = ? AND city = ? AND state = ? AND country = ?", record.Source, record.Company, record.City, record.State, record.Country).First(&duplicate).Error
		if err == nil {
			return ErrDuplicateSource
		}
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}

		owner, err := ensureSourceOwnerTx(tx, record.Source, record.Company, normalizedWebsiteDomain, childTableName, normalizedSchema)
		if err != nil {
			return err
		}
		record.SourceOwnerID = owner.ID
		record.WebsiteDomain = owner.WebsiteDomain

		return tx.Create(record).Error
	})
	if err != nil {
		return nil, err
	}

	return record, nil
}

func (s *PostgresStore) ListSources(ctx context.Context) ([]models.Source, error) {
	var sources []models.Source
	if err := s.db.WithContext(ctx).Order("source ASC, company ASC, city ASC, state ASC, country ASC").Find(&sources).Error; err != nil {
		return nil, err
	}

	if err := s.attachWebsiteDomains(ctx, sources); err != nil {
		return nil, err
	}

	return sources, nil
}

func (s *PostgresStore) CreateProject(ctx context.Context, projectName string, ingestionJWT string, ingestionJWTExpiresAt *time.Time) (*models.Project, error) {
	normalizedName, err := normalizeProjectName(projectName)
	if err != nil {
		return nil, err
	}

	trimmedJWT := strings.TrimSpace(ingestionJWT)
	if trimmedJWT == "" {
		return nil, fmt.Errorf("%w: ingestion JWT is required", ErrInvalidProject)
	}

	project := &models.Project{
		ProjectName:           normalizedName,
		IngestionJWT:          trimmedJWT,
		IngestionJWTExpiresAt: ingestionJWTExpiresAt,
	}

	if err := s.db.WithContext(ctx).Create(project).Error; err != nil {
		if isUniqueViolation(err) {
			return nil, ErrDuplicateProject
		}
		return nil, err
	}

	return project, nil
}

func (s *PostgresStore) ListProjects(ctx context.Context) ([]models.Project, error) {
	var projects []models.Project
	if err := s.db.WithContext(ctx).Order("project_name ASC").Find(&projects).Error; err != nil {
		return nil, err
	}

	return projects, nil
}

func (s *PostgresStore) GetLatestProject(ctx context.Context) (*models.Project, error) {
	var project models.Project
	err := s.db.WithContext(ctx).Order("updated_at DESC, id DESC").First(&project).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return &project, nil
}

func (s *PostgresStore) DeleteProject(ctx context.Context, projectName string) error {
	normalizedName, err := normalizeProjectName(projectName)
	if err != nil {
		return err
	}

	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var project models.Project
		if err := tx.Where("project_name = ?", normalizedName).First(&project).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrProjectNotFound
			}
			return err
		}

		if err := tx.Where("project_id = ?", project.ID).Delete(&models.ProjectSourceOwner{}).Error; err != nil {
			return err
		}

		if err := tx.Delete(&models.Project{}, project.ID).Error; err != nil {
			return err
		}

		return nil
	})
}

func (s *PostgresStore) UpdateProjectIngestionJWT(ctx context.Context, projectName string, ingestionJWT string, ingestionJWTExpiresAt *time.Time) (*models.Project, error) {
	normalizedName, err := normalizeProjectName(projectName)
	if err != nil {
		return nil, err
	}

	trimmedJWT := strings.TrimSpace(ingestionJWT)
	if trimmedJWT == "" {
		return nil, fmt.Errorf("%w: ingestion JWT is required", ErrInvalidProject)
	}

	project, err := s.findProjectByName(ctx, normalizedName)
	if err != nil {
		return nil, err
	}

	project.IngestionJWT = trimmedJWT
	project.IngestionJWTExpiresAt = ingestionJWTExpiresAt
	if err := s.db.WithContext(ctx).Save(project).Error; err != nil {
		return nil, err
	}

	return project, nil
}

func (s *PostgresStore) AttachProjectSourceOwner(ctx context.Context, projectName string, source string, company string) (*models.SourceOwner, error) {
	project, err := s.findProjectByName(ctx, projectName)
	if err != nil {
		return nil, err
	}
	owner, err := s.findSourceOwner(ctx, source, company)
	if err != nil {
		return nil, err
	}

	link := &models.ProjectSourceOwner{
		ProjectID:     project.ID,
		SourceOwnerID: owner.ID,
	}
	if err := s.db.WithContext(ctx).Create(link).Error; err != nil {
		if isUniqueViolation(err) {
			return nil, ErrDuplicateProjectLink
		}
		return nil, err
	}

	return owner, nil
}

func (s *PostgresStore) ListProjectSourceOwners(ctx context.Context, projectName string) ([]models.SourceOwner, error) {
	project, err := s.findProjectByName(ctx, projectName)
	if err != nil {
		return nil, err
	}

	var owners []models.SourceOwner
	err = s.db.WithContext(ctx).
		Table("source_owners").
		Select("source_owners.*").
		Joins("JOIN project_source_owners ON project_source_owners.source_owner_id = source_owners.id").
		Where("project_source_owners.project_id = ?", project.ID).
		Order("source_owners.source ASC, source_owners.company ASC").
		Find(&owners).Error
	if err != nil {
		return nil, err
	}

	return owners, nil
}

func (s *PostgresStore) DetachProjectSourceOwner(ctx context.Context, projectName string, source string, company string) error {
	project, err := s.findProjectByName(ctx, projectName)
	if err != nil {
		return err
	}
	owner, err := s.findSourceOwner(ctx, source, company)
	if err != nil {
		return err
	}

	result := s.db.WithContext(ctx).
		Where("project_id = ? AND source_owner_id = ?", project.ID, owner.ID).
		Delete(&models.ProjectSourceOwner{})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return ErrProjectOwnerLinkNotFound
	}

	return nil
}

func (s *PostgresStore) GetProjectOwnerEvents(ctx context.Context, projectName string, source string, company string) (*models.SourceOwner, []ChildEventRow, error) {
	project, err := s.findProjectByName(ctx, projectName)
	if err != nil {
		return nil, nil, err
	}
	owner, err := s.findSourceOwner(ctx, source, company)
	if err != nil {
		return nil, nil, err
	}

	var link models.ProjectSourceOwner
	if err := s.db.WithContext(ctx).
		Where("project_id = ? AND source_owner_id = ?", project.ID, owner.ID).
		First(&link).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil, ErrProjectOwnerLinkNotFound
		}
		return nil, nil, err
	}

	events, err := s.readSourceOwnerEvents(ctx, owner)
	if err != nil {
		return nil, nil, err
	}

	return owner, events, nil
}

func (s *PostgresStore) GetSourceOwnerEvents(ctx context.Context, source string, company string) (*models.SourceOwner, []ChildEventRow, error) {
	owner, err := s.findSourceOwner(ctx, source, company)
	if err != nil {
		return nil, nil, err
	}

	events, err := s.readSourceOwnerEvents(ctx, owner)
	if err != nil {
		return nil, nil, err
	}

	return owner, events, nil
}

func (s *PostgresStore) readSourceOwnerEvents(ctx context.Context, owner *models.SourceOwner) ([]ChildEventRow, error) {
	schema, err := normalizeSchema(owner.TableSchema)
	if err != nil {
		return nil, err
	}

	sqlDB, err := s.db.DB()
	if err != nil {
		return nil, err
	}

	rowsQuery := fmt.Sprintf(
		"SELECT * FROM %s ORDER BY created_at DESC",
		pq.QuoteIdentifier(owner.ChildTableName),
	)
	rows, err := sqlDB.QueryContext(ctx, rowsQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	events, err := scanChildRows(rows, schemaByName(schema))
	if err != nil {
		return nil, err
	}

	return events, nil
}

func (s *PostgresStore) CreateEvent(ctx context.Context, source string, company string, city string, state string, country string, payload map[string]any) (*ChildEventRow, error) {
	parent, err := s.findSourceByIdentity(ctx, source, company, city, state, country)
	if err != nil {
		return nil, err
	}

	owner, err := s.sourceOwnerForSource(ctx, parent)
	if err != nil {
		return nil, err
	}

	schema, err := normalizeSchema(owner.TableSchema)
	if err != nil {
		return nil, err
	}

	values, err := preparePayload(schema, payload)
	if err != nil {
		return nil, err
	}

	columns := []string{"source_parent_id", "source", "company", "city", "state", "country"}
	args := []any{parent.ID, parent.Source, parent.Company, parent.City, parent.State, parent.Country}
	for _, column := range schema {
		columns = append(columns, column.Name)
		args = append(args, values[column.Name])
	}

	insert := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s) RETURNING id, created_at",
		pq.QuoteIdentifier(owner.ChildTableName),
		joinIdentifiers(columns),
		buildPlaceholders(len(columns)),
	)

	sqlDB, err := s.db.DB()
	if err != nil {
		return nil, err
	}

	row := sqlDB.QueryRowContext(ctx, insert, args...)
	result := &ChildEventRow{
		SourceParentID: parent.ID,
		Source:         parent.Source,
		Company:        parent.Company,
		City:           parent.City,
		State:          parent.State,
		Country:        parent.Country,
		Payload:        values,
	}
	if err := row.Scan(&result.ID, &result.CreatedAt); err != nil {
		if isUniqueViolation(err) {
			return nil, ErrDuplicateEvent
		}
		return nil, err
	}

	return result, nil
}

func (s *PostgresStore) SearchEvents(ctx context.Context, source string, company string, city string, state string, country string, query string, page int, pageSize int) ([]ChildEventRow, int64, error) {
	owner, err := s.findSourceOwner(ctx, source, company)
	if err != nil {
		return nil, 0, err
	}

	schema, err := normalizeSchema(owner.TableSchema)
	if err != nil {
		return nil, 0, err
	}

	textColumns := searchableColumns(schema)
	conditions := []string{}
	args := []any{}
	if trimmed := strings.TrimSpace(city); trimmed != "" {
		conditions = append(conditions, fmt.Sprintf("LOWER(city) = LOWER($%d)", len(args)+1))
		args = append(args, trimmed)
	}
	if trimmed := strings.TrimSpace(state); trimmed != "" {
		conditions = append(conditions, fmt.Sprintf("LOWER(state) = LOWER($%d)", len(args)+1))
		args = append(args, trimmed)
	}
	if trimmed := strings.TrimSpace(country); trimmed != "" {
		conditions = append(conditions, fmt.Sprintf("LOWER(country) = LOWER($%d)", len(args)+1))
		args = append(args, strings.TrimSpace(trimmed))
	}
	if trimmed := strings.TrimSpace(query); trimmed != "" {
		if len(textColumns) == 0 {
			return []ChildEventRow{}, 0, nil
		}

		textConditions := make([]string, 0, len(textColumns))
		pattern := "%" + trimmed + "%"
		for _, column := range textColumns {
			textConditions = append(textConditions, fmt.Sprintf("%s ILIKE $%d", pq.QuoteIdentifier(column), len(args)+1))
			args = append(args, pattern)
		}
		conditions = append(conditions, "("+strings.Join(textConditions, " OR ")+")")
	}
	whereSQL := ""
	if len(conditions) > 0 {
		whereSQL = " WHERE " + strings.Join(conditions, " AND ")
	}

	sqlDB, err := s.db.DB()
	if err != nil {
		return nil, 0, err
	}

	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s%s", pq.QuoteIdentifier(owner.ChildTableName), whereSQL)
	var total int64
	if err := sqlDB.QueryRowContext(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	selectArgs := append([]any{}, args...)
	selectArgs = append(selectArgs, pageSize, (page-1)*pageSize)
	rowsQuery := fmt.Sprintf(
		"SELECT * FROM %s%s ORDER BY created_at DESC LIMIT $%d OFFSET $%d",
		pq.QuoteIdentifier(owner.ChildTableName),
		whereSQL,
		len(selectArgs)-1,
		len(selectArgs),
	)

	rows, err := sqlDB.QueryContext(ctx, rowsQuery, selectArgs...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	columnTypes := schemaByName(schema)
	events, err := scanChildRows(rows, columnTypes)
	if err != nil {
		return nil, 0, err
	}

	return events, total, nil
}

func (s *PostgresStore) Ping(ctx context.Context) error {
	sqlDB, err := s.db.DB()
	if err != nil {
		return err
	}

	return sqlDB.PingContext(ctx)
}

func (s *PostgresStore) Close() error {
	sqlDB, err := s.db.DB()
	if err != nil {
		return err
	}

	return sqlDB.Close()
}

func (s *PostgresStore) findSourceByIdentity(ctx context.Context, source string, company string, city string, state string, country string) (*models.Source, error) {
	var record models.Source
	var err error
	source, company, city, state, country, err = normalizeSourceIdentity(source, company, city, state, country)
	if err != nil {
		return nil, err
	}
	location, err := validateLocation(city, state, country)
	if err != nil {
		return nil, err
	}

	err = s.db.WithContext(ctx).
		Where("source = ? AND company = ? AND city = ? AND state = ? AND country = ?", source, company, location.City, location.State, location.CountryName).
		First(&record).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrSourceNotFound
		}
		return nil, err
	}

	return &record, nil
}

func (s *PostgresStore) findSourceOwner(ctx context.Context, source string, company string) (*models.SourceOwner, error) {
	var record models.SourceOwner
	var err error
	source, err = validateSource(source)
	if err != nil {
		return nil, err
	}
	company = properCase(company)
	err = s.db.WithContext(ctx).
		Where("source = ? AND company = ?", source, company).
		First(&record).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrSourceOwnerNotFound
		}
		return nil, err
	}

	return &record, nil
}

func (s *PostgresStore) findProjectByName(ctx context.Context, projectName string) (*models.Project, error) {
	normalizedName, err := normalizeProjectName(projectName)
	if err != nil {
		return nil, err
	}

	var project models.Project
	err = s.db.WithContext(ctx).Where("project_name = ?", normalizedName).First(&project).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrProjectNotFound
		}
		return nil, err
	}

	return &project, nil
}

func (s *PostgresStore) sourceOwnerForSource(ctx context.Context, source *models.Source) (*models.SourceOwner, error) {
	if source.SourceOwnerID != 0 {
		var owner models.SourceOwner
		err := s.db.WithContext(ctx).Where("id = ?", source.SourceOwnerID).First(&owner).Error
		if err == nil {
			return &owner, nil
		}
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, err
		}
	}

	return s.findSourceOwner(ctx, source.Source, source.Company)
}

func (s *PostgresStore) attachWebsiteDomains(ctx context.Context, sources []models.Source) error {
	if len(sources) == 0 {
		return nil
	}

	ownerIDs := make([]uint, 0, len(sources))
	seenOwnerIDs := make(map[uint]struct{}, len(sources))
	for _, source := range sources {
		if source.SourceOwnerID == 0 {
			continue
		}
		if _, seen := seenOwnerIDs[source.SourceOwnerID]; seen {
			continue
		}
		seenOwnerIDs[source.SourceOwnerID] = struct{}{}
		ownerIDs = append(ownerIDs, source.SourceOwnerID)
	}

	ownerDomains := make(map[uint]string, len(ownerIDs))
	if len(ownerIDs) > 0 {
		var owners []models.SourceOwner
		if err := s.db.WithContext(ctx).Where("id IN ?", ownerIDs).Find(&owners).Error; err != nil {
			return err
		}
		for _, owner := range owners {
			ownerDomains[owner.ID] = owner.WebsiteDomain
		}
	}

	for index := range sources {
		if domain := ownerDomains[sources[index].SourceOwnerID]; domain != "" {
			sources[index].WebsiteDomain = domain
			continue
		}

		owner, err := s.findSourceOwner(ctx, sources[index].Source, sources[index].Company)
		if err == nil {
			sources[index].WebsiteDomain = owner.WebsiteDomain
			continue
		}
		if errors.Is(err, ErrSourceOwnerNotFound) {
			continue
		}
		return err
	}

	return nil
}

func normalizeSchema(schema models.TableSchema) (models.TableSchema, error) {
	if len(schema) == 0 {
		return nil, fmt.Errorf("%w: tableSchema must include at least one column", ErrInvalidTableSchema)
	}

	normalized := make(models.TableSchema, 0, len(schema))
	seen := map[string]struct{}{}
	for _, column := range schema {
		name := normalizeIdentifier(column.Name)
		if name == "" {
			return nil, fmt.Errorf("%w: invalid column name %q", ErrInvalidTableSchema, column.Name)
		}
		if _, reserved := reservedColumns[name]; reserved {
			return nil, fmt.Errorf("%w: column name %q is reserved", ErrInvalidTableSchema, name)
		}
		if _, exists := seen[name]; exists {
			return nil, fmt.Errorf("%w: duplicate column %q", ErrInvalidTableSchema, name)
		}

		columnType := strings.ToLower(strings.TrimSpace(column.Type))
		if _, allowed := allowedTypes[columnType]; !allowed {
			return nil, fmt.Errorf("%w: unsupported column type %q", ErrInvalidTableSchema, column.Type)
		}

		seen[name] = struct{}{}
		normalized = append(normalized, models.TableColumn{
			Name:     name,
			Type:     columnType,
			Required: column.Required,
		})
	}

	return normalized, nil
}

func normalizeProjectName(projectName string) (string, error) {
	trimmed := strings.TrimSpace(projectName)
	if trimmed == "" {
		return "", fmt.Errorf("%w: project name is required", ErrInvalidProject)
	}
	if len(trimmed) > 10 {
		return "", fmt.Errorf("%w: project name must be 10 characters or fewer", ErrInvalidProject)
	}
	if !projectNamePattern.MatchString(trimmed) {
		return "", fmt.Errorf("%w: project name must be alphanumeric only", ErrInvalidProject)
	}

	return trimmed, nil
}

func buildChildTableName(source string, company string) (string, error) {
	normalizedSource := normalizeIdentifier(source)
	normalizedCompany := normalizeIdentifier(company)
	if normalizedSource == "" || normalizedCompany == "" {
		return "", ErrInvalidSourceOwner
	}

	tableName := "events_" + normalizedSource + "_" + normalizedCompany
	if len(tableName) <= 63 {
		return tableName, nil
	}

	hash := fmt.Sprintf("%x", md5Bytes(tableName))[:8]
	maxPrefix := 63 - len(hash) - 1
	return tableName[:maxPrefix] + "_" + hash, nil
}

func createChildTable(tx *gorm.DB, tableName string, schema models.TableSchema) error {
	definitions := []string{
		"id BIGSERIAL PRIMARY KEY",
		"source_parent_id BIGINT NOT NULL REFERENCES sources(id) ON DELETE RESTRICT",
		"source TEXT NOT NULL",
		"company TEXT NOT NULL",
		"city TEXT NOT NULL",
		"state TEXT NOT NULL",
		"country TEXT NOT NULL",
		"created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()",
	}
	for _, column := range schema {
		definition := fmt.Sprintf("%s %s", pq.QuoteIdentifier(column.Name), allowedTypes[column.Type])
		if column.Required {
			definition += " NOT NULL"
		}
		definitions = append(definitions, definition)
	}

	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", pq.QuoteIdentifier(tableName), strings.Join(definitions, ", "))
	if err := tx.Exec(query).Error; err != nil {
		return err
	}

	if err := tx.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS state TEXT", pq.QuoteIdentifier(tableName))).Error; err != nil {
		return err
	}
	for _, column := range schema {
		if err := tx.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s", pq.QuoteIdentifier(tableName), pq.QuoteIdentifier(column.Name), allowedTypes[column.Type])).Error; err != nil {
			return err
		}
	}

	return nil
}

func ensureSourceOwnerTx(tx *gorm.DB, source string, company string, websiteDomain string, childTableName string, schema models.TableSchema) (*models.SourceOwner, error) {
	var owner models.SourceOwner
	err := tx.Where("source = ? AND company = ?", source, company).First(&owner).Error
	if err == nil {
		if owner.ChildTableName != childTableName || !schemaEqual(owner.TableSchema, schema) {
			return nil, ErrSourceSchemaMismatch
		}
		normalizedInput := strings.TrimSpace(strings.ToLower(websiteDomain))
		nextWebsiteDomain := owner.WebsiteDomain
		switch {
		case normalizedInput != "":
			nextWebsiteDomain = normalizeWebsiteDomain(source, company, websiteDomain)
		case strings.TrimSpace(owner.WebsiteDomain) == "":
			nextWebsiteDomain = normalizeWebsiteDomain(source, company, websiteDomain)
		}
		if owner.WebsiteDomain != nextWebsiteDomain {
			owner.WebsiteDomain = nextWebsiteDomain
			if err := tx.Model(&owner).Update("website_domain", nextWebsiteDomain).Error; err != nil {
				return nil, err
			}
		}
		if err := createChildTable(tx, childTableName, schema); err != nil {
			return nil, err
		}
		if err := ensureSourceReplayProtection(tx, source, childTableName, schema); err != nil {
			return nil, err
		}
		return &owner, nil
	}
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}

	if err := createChildTable(tx, childTableName, schema); err != nil {
		return nil, err
	}
	if err := ensureSourceReplayProtection(tx, source, childTableName, schema); err != nil {
		return nil, err
	}

	owner = models.SourceOwner{
		Source:         source,
		Company:        company,
		WebsiteDomain:  normalizeWebsiteDomain(source, company, websiteDomain),
		ChildTableName: childTableName,
		TableSchema:    schema,
	}
	if err := tx.Create(&owner).Error; err != nil {
		if isUniqueViolation(err) {
			return nil, ErrSourceSchemaMismatch
		}
		return nil, err
	}

	return &owner, nil
}

func (s *PostgresStore) migrateLegacySourceTables(ctx context.Context) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec("DROP INDEX IF EXISTS idx_source_identity").Error; err != nil {
			return err
		}

		var sources []models.Source
		if err := tx.Order("id ASC").Find(&sources).Error; err != nil {
			return err
		}

		for _, source := range sources {
			normalized, err := normalizeLegacySource(source)
			if err != nil {
				return err
			}

			if err := createChildTable(tx, normalized.ChildTableName, normalized.TableSchema); err != nil {
				return err
			}

			if err := tx.Model(&models.Source{}).
				Where("id = ?", normalized.ID).
				Updates(map[string]any{
					"source":  normalized.Source,
					"company": normalized.Company,
					"city":    normalized.City,
					"state":   normalized.State,
					"country": normalized.Country,
				}).Error; err != nil {
				return err
			}

			updateChildQuery := fmt.Sprintf(
				"UPDATE %s SET source = ?, company = ?, city = ?, state = ?, country = ? WHERE source_parent_id = ?",
				pq.QuoteIdentifier(normalized.ChildTableName),
			)
			if err := tx.Exec(
				updateChildQuery,
				normalized.Source,
				normalized.Company,
				normalized.City,
				normalized.State,
				normalized.Country,
				normalized.ID,
			).Error; err != nil {
				return err
			}
		}

		if err := tx.Exec("ALTER TABLE sources ALTER COLUMN state SET NOT NULL").Error; err != nil {
			return err
		}
		if err := tx.Exec("CREATE UNIQUE INDEX IF NOT EXISTS idx_source_identity ON sources (source, company, city, state, country)").Error; err != nil {
			return err
		}
		if err := tx.Exec("CREATE INDEX IF NOT EXISTS idx_source_owner ON sources (source, company)").Error; err != nil {
			return err
		}

		return nil
	})
}

func (s *PostgresStore) backfillSourceOwners(ctx context.Context) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var sources []models.Source
		if err := tx.Order("id ASC").Find(&sources).Error; err != nil {
			return err
		}

		for _, source := range sources {
			normalized, err := normalizeLegacySource(source)
			if err != nil {
				return err
			}

			owner, err := ensureSourceOwnerTx(tx, normalized.Source, normalized.Company, "", normalized.ChildTableName, normalized.TableSchema)
			if err != nil {
				return err
			}

			if err := tx.Model(&models.Source{}).
				Where("id = ?", normalized.ID).
				Updates(map[string]any{
					"source_owner_id":  owner.ID,
					"child_table_name": owner.ChildTableName,
					"table_schema":     owner.TableSchema,
				}).Error; err != nil {
				return err
			}
		}

		return nil
	})
}

func (s *PostgresStore) backfillSourceOwnerWebsiteDomains(ctx context.Context) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var owners []models.SourceOwner
		if err := tx.Order("id ASC").Find(&owners).Error; err != nil {
			return err
		}

		for _, owner := range owners {
			nextDomain := normalizeWebsiteDomain(owner.Source, owner.Company, owner.WebsiteDomain)
			if owner.WebsiteDomain == nextDomain {
				continue
			}
			if err := tx.Model(&models.SourceOwner{}).
				Where("id = ?", owner.ID).
				Update("website_domain", nextDomain).Error; err != nil {
				return err
			}
		}

		return nil
	})
}

func (s *PostgresStore) enforceReplayProtection(ctx context.Context) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var owners []models.SourceOwner
		if err := tx.Order("id ASC").Find(&owners).Error; err != nil {
			return err
		}

		for _, owner := range owners {
			schema, err := normalizeSchema(owner.TableSchema)
			if err != nil {
				return err
			}

			if err := ensureSourceReplayProtection(tx, owner.Source, owner.ChildTableName, schema); err != nil {
				return err
			}
		}

		return nil
	})
}

func normalizeLegacySource(source models.Source) (models.Source, error) {
	normalizedSource, err := validateSource(source.Source)
	if err != nil {
		return models.Source{}, err
	}
	source.Source = normalizedSource
	source.Company = properCase(source.Company)

	location, err := normalizeLegacyLocation(source)
	if err != nil {
		return models.Source{}, err
	}

	source.City = location.City
	source.State = location.State
	source.Country = location.CountryName

	return source, nil
}

func replayProtectionColumnForSource(source string, schema models.TableSchema) string {
	normalizedSource, err := validateSource(source)
	if err != nil {
		return ""
	}

	switch normalizedSource {
	case "News":
		return firstSchemaColumnMatch(schema, "article_id")
	case "Flights":
		return firstSchemaColumnMatch(schema, "flight_id")
	case "Events":
		return firstSchemaColumnMatch(schema, "invoice_number", "event_id")
	case "ECommerce":
		return firstSchemaColumnMatch(schema, "order_id", "invoice_number", "transaction_id")
	}

	return ""
}

func normalizeWebsiteDomain(source string, company string, websiteDomain string) string {
	trimmed := strings.TrimSpace(strings.ToLower(websiteDomain))
	if trimmed != "" {
		trimmed = strings.TrimPrefix(strings.TrimPrefix(trimmed, "https://"), "http://")
		if slash := strings.Index(trimmed, "/"); slash >= 0 {
			trimmed = trimmed[:slash]
		}
		return strings.TrimSpace(trimmed)
	}

	return defaultWebsiteDomains[strings.ToLower(source)+"::"+strings.ToLower(company)]
}

func normalizeRequiredWebsiteDomain(websiteDomain string) (string, error) {
	trimmed := strings.TrimSpace(strings.ToLower(websiteDomain))
	if trimmed == "" {
		return "", fmt.Errorf("%w: websiteDomain is required", ErrInvalidWebsiteDomain)
	}

	trimmed = strings.TrimPrefix(strings.TrimPrefix(trimmed, "https://"), "http://")
	if slash := strings.Index(trimmed, "/"); slash >= 0 {
		trimmed = trimmed[:slash]
	}
	trimmed = strings.TrimSpace(trimmed)
	if trimmed == "" {
		return "", fmt.Errorf("%w: websiteDomain is required", ErrInvalidWebsiteDomain)
	}

	return trimmed, nil
}

func firstSchemaColumnMatch(schema models.TableSchema, candidates ...string) string {
	for _, candidate := range candidates {
		if schemaHasColumn(schema, candidate) {
			return candidate
		}
	}

	return ""
}

func ensureSourceReplayProtection(tx *gorm.DB, source string, tableName string, schema models.TableSchema) error {
	replayKey := replayProtectionColumnForSource(source, schema)
	if replayKey == "" {
		return nil
	}
	if err := dedupeChildTableByReplayKey(tx, tableName, replayKey); err != nil {
		return err
	}

	return ensureReplayProtectionIndex(tx, tableName, replayKey)
}

func schemaHasColumn(schema models.TableSchema, name string) bool {
	for _, column := range schema {
		if column.Name == name {
			return true
		}
	}

	return false
}

func dedupeChildTableByReplayKey(tx *gorm.DB, tableName string, replayKey string) error {
	query := fmt.Sprintf(
		`DELETE FROM %s
		WHERE id IN (
			SELECT id FROM (
				SELECT id, ROW_NUMBER() OVER (
					PARTITION BY source_parent_id, %s
					ORDER BY id ASC
				) AS row_num
				FROM %s
				WHERE %s IS NOT NULL
			) duplicates
			WHERE row_num > 1
		)`,
		pq.QuoteIdentifier(tableName),
		pq.QuoteIdentifier(replayKey),
		pq.QuoteIdentifier(tableName),
		pq.QuoteIdentifier(replayKey),
	)

	return tx.Exec(query).Error
}

func ensureReplayProtectionIndex(tx *gorm.DB, tableName string, replayKey string) error {
	indexName := replayProtectionIndexName(tableName, replayKey)
	query := fmt.Sprintf(
		"CREATE UNIQUE INDEX IF NOT EXISTS %s ON %s (source_parent_id, %s)",
		pq.QuoteIdentifier(indexName),
		pq.QuoteIdentifier(tableName),
		pq.QuoteIdentifier(replayKey),
	)

	return tx.Exec(query).Error
}

func replayProtectionIndexName(tableName string, replayKey string) string {
	indexName := "uniq_" + tableName + "_" + replayKey
	if len(indexName) <= 63 {
		return indexName
	}

	hash := fmt.Sprintf("%x", md5Bytes(indexName))[:8]
	maxPrefix := 63 - len(hash) - 1
	return indexName[:maxPrefix] + "_" + hash
}

func preparePayload(schema models.TableSchema, payload map[string]any) (map[string]any, error) {
	normalized := map[string]any{}
	for key := range payload {
		normalized[normalizeIdentifier(key)] = payload[key]
	}

	values := make(map[string]any, len(schema))
	for _, column := range schema {
		value, exists := normalized[column.Name]
		if !exists {
			if column.Required {
				return nil, fmt.Errorf("%w: missing required field %q", ErrInvalidPayload, column.Name)
			}
			values[column.Name] = nil
			continue
		}

		converted, err := convertPayloadValue(column, value)
		if err != nil {
			return nil, err
		}
		values[column.Name] = converted
		delete(normalized, column.Name)
	}

	if len(normalized) > 0 {
		extras := make([]string, 0, len(normalized))
		for key := range normalized {
			extras = append(extras, key)
		}
		return nil, fmt.Errorf("%w: unsupported fields %s", ErrInvalidPayload, strings.Join(extras, ", "))
	}

	return values, nil
}

func convertPayloadValue(column models.TableColumn, value any) (any, error) {
	if value == nil {
		if column.Required {
			return nil, fmt.Errorf("%w: field %q cannot be null", ErrInvalidPayload, column.Name)
		}
		return nil, nil
	}

	switch column.Type {
	case "text":
		text, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("%w: field %q must be text", ErrInvalidPayload, column.Name)
		}
		return text, nil
	case "integer", "bigint":
		number, ok := numericValue(value)
		if !ok || math.Trunc(number) != number {
			return nil, fmt.Errorf("%w: field %q must be an integer", ErrInvalidPayload, column.Name)
		}
		return int64(number), nil
	case "numeric":
		number, ok := numericValue(value)
		if !ok {
			return nil, fmt.Errorf("%w: field %q must be numeric", ErrInvalidPayload, column.Name)
		}
		return number, nil
	case "boolean":
		boolean, ok := value.(bool)
		if !ok {
			return nil, fmt.Errorf("%w: field %q must be boolean", ErrInvalidPayload, column.Name)
		}
		return boolean, nil
	case "timestamptz":
		switch candidate := value.(type) {
		case string:
			parsed, err := time.Parse(time.RFC3339, candidate)
			if err != nil {
				return nil, fmt.Errorf("%w: field %q must be RFC3339 timestamp", ErrInvalidPayload, column.Name)
			}
			return parsed.UTC(), nil
		case time.Time:
			return candidate.UTC(), nil
		default:
			return nil, fmt.Errorf("%w: field %q must be timestamp string", ErrInvalidPayload, column.Name)
		}
	case "jsonb":
		data, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("%w: field %q must be valid JSON", ErrInvalidPayload, column.Name)
		}
		return string(data), nil
	default:
		return nil, fmt.Errorf("%w: unsupported type %q", ErrInvalidTableSchema, column.Type)
	}
}

func searchableColumns(schema models.TableSchema) []string {
	columns := make([]string, 0, len(schema))
	for _, column := range schema {
		if column.Type == "text" {
			columns = append(columns, column.Name)
		}
	}

	return columns
}

func schemaEqual(left models.TableSchema, right models.TableSchema) bool {
	if len(left) != len(right) {
		return false
	}

	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}

	return true
}

func joinIdentifiers(columns []string) string {
	quoted := make([]string, 0, len(columns))
	for _, column := range columns {
		quoted = append(quoted, pq.QuoteIdentifier(column))
	}

	return strings.Join(quoted, ", ")
}

func buildPlaceholders(count int) string {
	placeholders := make([]string, 0, count)
	for index := 1; index <= count; index++ {
		placeholders = append(placeholders, "$"+strconv.Itoa(index))
	}

	return strings.Join(placeholders, ", ")
}

func scanChildRows(rows *sql.Rows, columnTypes map[string]string) ([]ChildEventRow, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	results := []ChildEventRow{}
	for rows.Next() {
		values := make([]any, len(columns))
		destinations := make([]any, len(columns))
		for index := range values {
			destinations[index] = &values[index]
		}

		if err := rows.Scan(destinations...); err != nil {
			return nil, err
		}

		record := ChildEventRow{Payload: map[string]any{}}
		for index, column := range columns {
			switch column {
			case "id":
				record.ID = asInt64(values[index])
			case "source_parent_id":
				record.SourceParentID = uint(asInt64(values[index]))
			case "source":
				record.Source = asString(values[index])
			case "company":
				record.Company = asString(values[index])
			case "city":
				record.City = asString(values[index])
			case "state":
				record.State = asString(values[index])
			case "country":
				record.Country = asString(values[index])
			case "created_at":
				record.CreatedAt = asTime(values[index])
			default:
				record.Payload[column] = decodeColumnValue(columnTypes[column], values[index])
			}
		}
		results = append(results, record)
	}

	return results, rows.Err()
}

func schemaByName(schema models.TableSchema) map[string]string {
	index := make(map[string]string, len(schema))
	for _, column := range schema {
		index[column.Name] = column.Type
	}

	return index
}

func decodeColumnValue(columnType string, value any) any {
	if value == nil {
		return nil
	}

	switch columnType {
	case "integer", "bigint":
		return asInt64(value)
	case "numeric":
		return asFloat64(value)
	case "boolean":
		switch candidate := value.(type) {
		case bool:
			return candidate
		case []byte:
			return string(candidate) == "t"
		case string:
			return candidate == "t" || candidate == "true"
		default:
			return candidate
		}
	case "timestamptz":
		return asTime(value)
	case "jsonb":
		switch candidate := value.(type) {
		case []byte:
			var decoded any
			if err := json.Unmarshal(candidate, &decoded); err == nil {
				return decoded
			}
			return string(candidate)
		case string:
			var decoded any
			if err := json.Unmarshal([]byte(candidate), &decoded); err == nil {
				return decoded
			}
			return candidate
		default:
			return candidate
		}
	default:
		return asString(value)
	}
}

func normalizeIdentifier(value string) string {
	normalized := strings.ToLower(strings.TrimSpace(value))
	normalized = identifierPattern.ReplaceAllString(normalized, "_")
	normalized = strings.Trim(normalized, "_")
	normalized = strings.TrimSpace(normalized)
	normalized = strings.Join(strings.FieldsFunc(normalized, func(r rune) bool { return r == '_' }), "_")
	return normalized
}

func normalizeSourceIdentity(source string, company string, city string, state string, country string) (string, string, string, string, string, error) {
	validatedSource, err := validateSource(source)
	if err != nil {
		return "", "", "", "", "", err
	}
	return validatedSource, properCase(company), properCase(city), properCase(state), strings.TrimSpace(country), nil
}

func properCase(value string) string {
	return cases.Title(language.English).String(strings.ToLower(strings.TrimSpace(value)))
}

func validateSource(source string) (string, error) {
	source = strings.TrimSpace(source)
	for candidate := range allowedSources {
		if strings.EqualFold(candidate, source) {
			return candidate, nil
		}
	}
	if _, ok := allowedSources[source]; !ok {
		return "", fmt.Errorf("%w: source must be one of Events, News, ECommerce, Flights", ErrInvalidSource)
	}

	return source, nil
}

func validateLocation(city string, state string, country string) (reference.Airport, error) {
	city = properCase(city)
	state = properCase(state)
	country = strings.TrimSpace(country)

	location, err := reference.LookupLocation(city, state, country)
	if err != nil {
		return reference.Airport{}, fmt.Errorf("%w: %v", ErrInvalidLocation, err)
	}

	if !equalFoldTrim(city, location.City) {
		return reference.Airport{}, fmt.Errorf("%w: city %q does not match country %q", ErrInvalidLocation, city, country)
	}
	if !equalFoldTrim(state, location.State) {
		return reference.Airport{}, fmt.Errorf("%w: state %q does not match country %q", ErrInvalidLocation, state, country)
	}

	return location, nil
}

func normalizeLegacyLocation(source models.Source) (reference.Airport, error) {
	location, err := validateLocation(source.City, source.State, source.Country)
	if err != nil {
		return reference.Airport{}, fmt.Errorf("%w: source id %d: %v", ErrInvalidLocation, source.ID, err)
	}

	return location, nil
}

func equalFoldTrim(left string, right string) bool {
	return strings.EqualFold(strings.TrimSpace(left), strings.TrimSpace(right))
}

func isUniqueViolation(err error) bool {
	var pqErr *pq.Error
	if errors.As(err, &pqErr) {
		return pqErr.Code == "23505"
	}

	var sqlStateErr interface{ SQLState() string }
	if errors.As(err, &sqlStateErr) {
		return sqlStateErr.SQLState() == "23505"
	}

	return false
}

func numericValue(value any) (float64, bool) {
	switch candidate := value.(type) {
	case float64:
		return candidate, true
	case float32:
		return float64(candidate), true
	case int:
		return float64(candidate), true
	case int64:
		return float64(candidate), true
	case int32:
		return float64(candidate), true
	case json.Number:
		number, err := candidate.Float64()
		return number, err == nil
	case string:
		number, err := strconv.ParseFloat(candidate, 64)
		return number, err == nil
	default:
		return 0, false
	}
}

func asString(value any) string {
	switch candidate := value.(type) {
	case string:
		return candidate
	case []byte:
		return string(candidate)
	default:
		return fmt.Sprint(candidate)
	}
}

func asInt64(value any) int64 {
	switch candidate := value.(type) {
	case int64:
		return candidate
	case int32:
		return int64(candidate)
	case int:
		return int64(candidate)
	case float64:
		return int64(candidate)
	case []byte:
		number, _ := strconv.ParseInt(string(candidate), 10, 64)
		return number
	case string:
		number, _ := strconv.ParseInt(candidate, 10, 64)
		return number
	default:
		return 0
	}
}

func asFloat64(value any) float64 {
	switch candidate := value.(type) {
	case float64:
		return candidate
	case float32:
		return float64(candidate)
	case int64:
		return float64(candidate)
	case int:
		return float64(candidate)
	case []byte:
		number, _ := strconv.ParseFloat(string(candidate), 64)
		return number
	case string:
		number, _ := strconv.ParseFloat(candidate, 64)
		return number
	default:
		return 0
	}
}

func asTime(value any) time.Time {
	switch candidate := value.(type) {
	case time.Time:
		return candidate.UTC()
	case []byte:
		parsed, _ := time.Parse(time.RFC3339Nano, string(candidate))
		return parsed.UTC()
	case string:
		parsed, _ := time.Parse(time.RFC3339Nano, candidate)
		return parsed.UTC()
	default:
		return time.Time{}
	}
}

func md5Bytes(value string) []byte {
	sum := md5.Sum([]byte(value))
	return sum[:]
}
