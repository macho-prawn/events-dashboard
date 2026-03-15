package api

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/macho_prawn/events-dashboard/internal/auth"
	"github.com/macho_prawn/events-dashboard/internal/models"
	"github.com/macho_prawn/events-dashboard/internal/reference"
	"github.com/macho_prawn/events-dashboard/internal/store"
)

const searchPageSize = 50

var dashboardPalette = []string{"#166534", "#16a34a", "#65a30d", "#0f766e", "#2563eb"}

type Handler struct {
	store            store.EventStore
	keyManager       *auth.Manager
	accessKeyManager *auth.Manager
}

type GetAPIKeyInput struct {
	Authorization string `header:"Authorization" doc:"Bearer JWT used to access the API key endpoint."`
}

type APIKeyResponse struct {
	APIKey    string     `json:"apiKey"`
	ExpiresAt *time.Time `json:"expiresAt,omitempty"`
}

type GetAPIKeyOutput struct {
	Body APIKeyResponse
}

type SourceSchemaColumn struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Required bool   `json:"required"`
}

type CreateSourceBody struct {
	Source        string               `json:"source" minLength:"1"`
	Company       string               `json:"company" minLength:"1"`
	City          string               `json:"city" minLength:"1"`
	State         string               `json:"state" minLength:"1"`
	Country       string               `json:"country" minLength:"1"`
	WebsiteDomain string               `json:"websiteDomain" minLength:"1"`
	TableSchema   []SourceSchemaColumn `json:"tableSchema"`
}

type CreateSourceInput struct {
	Authorization string           `header:"Authorization" doc:"Bearer JWT used to access source management endpoints."`
	Body          CreateSourceBody `json:"body"`
}

type SourceRecord struct {
	ID             uint                 `json:"id"`
	SourceOwnerID  uint                 `json:"sourceOwnerId"`
	Source         string               `json:"source"`
	Company        string               `json:"company"`
	WebsiteDomain  string               `json:"websiteDomain,omitempty"`
	City           string               `json:"city"`
	State          string               `json:"state"`
	Country        string               `json:"country"`
	ChildTableName string               `json:"childTableName"`
	TableSchema    []SourceSchemaColumn `json:"tableSchema"`
	CreatedAt      time.Time            `json:"createdAt"`
	UpdatedAt      time.Time            `json:"updatedAt"`
}

type CreateSourceOutput struct {
	Body SourceRecord
}

type CreateProjectBody struct {
	ProjectName  string `json:"projectName" minLength:"1" maxLength:"10"`
	IngestionJWT string `json:"ingestionJwt" minLength:"1"`
}

type CreateProjectInput struct {
	Authorization string            `header:"Authorization" doc:"Bearer JWT used to access project management endpoints."`
	Body          CreateProjectBody `json:"body"`
}

type ProjectOwnerRecord struct {
	ID             uint                 `json:"id"`
	Source         string               `json:"source"`
	Company        string               `json:"company"`
	WebsiteDomain  string               `json:"websiteDomain,omitempty"`
	ChildTableName string               `json:"childTableName"`
	TableSchema    []SourceSchemaColumn `json:"tableSchema"`
}

type ProjectRecord struct {
	ID                    uint                 `json:"id"`
	ProjectName           string               `json:"projectName"`
	IngestionJWT          string               `json:"ingestionJwt"`
	IngestionJWTExpiresAt *time.Time           `json:"ingestionJwtExpiresAt,omitempty"`
	Owners                []ProjectOwnerRecord `json:"owners"`
	CreatedAt             time.Time            `json:"createdAt"`
	UpdatedAt             time.Time            `json:"updatedAt"`
}

type CreateProjectOutput struct {
	Body ProjectRecord
}

type UpdateProjectJWTBody struct {
	IngestionJWT string `json:"ingestionJwt" minLength:"1"`
}

type UpdateProjectJWTInput struct {
	Authorization string               `header:"Authorization" doc:"Bearer JWT used to access project management endpoints."`
	ProjectName   string               `path:"projectName"`
	Body          UpdateProjectJWTBody `json:"body"`
}

type UpdateProjectJWTOutput struct {
	Body ProjectRecord
}

type UpdateAllProjectsJWTInput struct {
	Authorization string               `header:"Authorization" doc:"Bearer JWT used to access project management endpoints."`
	Body          UpdateProjectJWTBody `json:"body"`
}

type UpdateAllProjectsJWTOutput struct {
	Body struct {
		Projects []ProjectRecord `json:"projects"`
	}
}

type ListProjectsInput struct {
	Authorization string `header:"Authorization" doc:"Bearer JWT used to access project management endpoints."`
}

type ListProjectsOutput struct {
	Body struct {
		Projects []ProjectRecord `json:"projects"`
	}
}

type DeleteProjectInput struct {
	Authorization string `header:"Authorization" doc:"Bearer JWT used to access project management endpoints."`
	ProjectName   string `path:"projectName"`
}

type DeleteProjectOutput struct{}

type BootstrapProjectOutput struct {
	Body struct {
		AccessJWT      string          `json:"accessJwt,omitempty"`
		NeedsAccessJWT bool            `json:"needsAccessJwt"`
		Projects       []ProjectRecord `json:"projects"`
		Sources        []SourceRecord  `json:"sources"`
	}
}

type AttachProjectOwnerBody struct {
	Source  string `json:"source" minLength:"1"`
	Company string `json:"company" minLength:"1"`
}

type AttachProjectOwnerInput struct {
	Authorization string                 `header:"Authorization" doc:"Bearer JWT used to access project management endpoints."`
	ProjectName   string                 `path:"projectName"`
	Body          AttachProjectOwnerBody `json:"body"`
}

type AttachProjectOwnerOutput struct {
	Body ProjectOwnerRecord
}

type DeleteProjectOwnerInput struct {
	Authorization string `header:"Authorization" doc:"Bearer JWT used to access project management endpoints."`
	ProjectName   string `path:"projectName"`
	Source        string `query:"source"`
	Company       string `query:"company"`
}

type DeleteProjectOwnerOutput struct{}

type ListProjectOwnersInput struct {
	Authorization string `header:"Authorization" doc:"Bearer JWT used to access project management endpoints."`
	ProjectName   string `path:"projectName"`
}

type ListProjectOwnersOutput struct {
	Body struct {
		Owners []ProjectOwnerRecord `json:"owners"`
	}
}

type ListSourcesInput struct {
	Authorization string `header:"Authorization" doc:"Bearer JWT used to access source management endpoints."`
}

type ListSourcesOutput struct {
	Body struct {
		Sources []SourceRecord `json:"sources"`
	}
}

type CreateEventBody struct {
	Source  string         `json:"source" minLength:"1"`
	Company string         `json:"company" minLength:"1"`
	City    string         `json:"city" minLength:"1"`
	State   string         `json:"state" minLength:"1"`
	Country string         `json:"country" minLength:"1"`
	Payload map[string]any `json:"payload"`
}

type CreateEventInput struct {
	Authorization string          `header:"Authorization" doc:"Bearer ingestion/search JWT."`
	Body          CreateEventBody `json:"body"`
}

type CreateEventOutput struct {
	Body struct {
		ID        int64     `json:"id"`
		CreatedAt time.Time `json:"createdAt"`
	}
}

type SearchEventsInput struct {
	Authorization string `header:"Authorization" doc:"Bearer ingestion/search JWT."`
	Source        string `query:"source"`
	Company       string `query:"company"`
	City          string `query:"city"`
	State         string `query:"state"`
	Country       string `query:"country"`
	Query         string `query:"q"`
	Page          int    `query:"page"`
}

type SearchEventRecord struct {
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

type SearchEventsOutput struct {
	Body struct {
		Page     int                 `json:"page"`
		PageSize int                 `json:"pageSize"`
		Total    int64               `json:"total"`
		Results  []SearchEventRecord `json:"results"`
	}
}

type GetProjectDashboardInput struct {
	Authorization string `header:"Authorization" doc:"Bearer ingestion/search JWT."`
	ProjectName   string `path:"projectName"`
	Source        string `query:"source"`
	Company       string `query:"company"`
}

type DashboardTopMetrics struct {
	ProjectsCount  int    `json:"projectsCount"`
	TotalRecords   int    `json:"totalRecords"`
	LocationsCount int    `json:"locationsCount"`
	Last24Hours    int    `json:"last24Hours"`
	Last7Days      int    `json:"last7Days"`
	ActiveDelta    string `json:"activeDelta"`
	RecordsDelta   string `json:"recordsDelta"`
	LocationsDelta string `json:"locationsDelta"`
}

type DashboardCoverageField struct {
	Name     string `json:"name"`
	Percent  int    `json:"percent"`
	Required bool   `json:"required"`
}

type DashboardSummaryItem struct {
	Label string `json:"label"`
	Value int    `json:"value"`
	Color string `json:"color"`
}

type DashboardCoverage struct {
	Overall int                      `json:"overall"`
	Fields  []DashboardCoverageField `json:"fields"`
	Summary []DashboardSummaryItem   `json:"summary"`
}

type DashboardLocationSegment struct {
	Label   string `json:"label"`
	Value   int    `json:"value"`
	Percent int    `json:"percent"`
	Color   string `json:"color"`
	End     int    `json:"end"`
}

type DashboardLocationBreakdown struct {
	Segments   []DashboardLocationSegment `json:"segments"`
	TotalLabel string                     `json:"totalLabel"`
}

type DashboardTrendPoint struct {
	Label string `json:"label"`
	Value int    `json:"value"`
}

type DashboardTrend struct {
	Points []DashboardTrendPoint `json:"points"`
}

type DashboardAnalytics struct {
	TopMetrics        DashboardTopMetrics        `json:"topMetrics"`
	Coverage          DashboardCoverage          `json:"coverage"`
	LocationBreakdown DashboardLocationBreakdown `json:"locationBreakdown"`
	Trend             DashboardTrend             `json:"trend"`
}

type GetProjectDashboardOutput struct {
	Body DashboardAnalytics
}

type GetSourceCompanyAnalyticsInput struct {
	Authorization string `header:"Authorization" doc:"Bearer ingestion/search JWT."`
	Source        string `path:"source"`
	CompanyName   string `path:"companyName"`
}

type SourceAnalyticsChartItem struct {
	Label      string  `json:"label"`
	Value      float64 `json:"value"`
	ValueLabel string  `json:"valueLabel,omitempty"`
	Detail     string  `json:"detail,omitempty"`
	FromCode   string  `json:"fromCode,omitempty"`
	ToCode     string  `json:"toCode,omitempty"`
	FromLabel  string  `json:"fromLabel,omitempty"`
	ToLabel    string  `json:"toLabel,omitempty"`
	FromLat    float64 `json:"fromLat,omitempty"`
	FromLng    float64 `json:"fromLng,omitempty"`
	ToLat      float64 `json:"toLat,omitempty"`
	ToLng      float64 `json:"toLng,omitempty"`
}

type SourceAnalyticsChart struct {
	ID       string                     `json:"id"`
	Title    string                     `json:"title"`
	Subtitle string                     `json:"subtitle,omitempty"`
	Kind     string                     `json:"kind"`
	Items    []SourceAnalyticsChartItem `json:"items"`
}

type SourceCompanyAnalytics struct {
	Source       string                 `json:"source"`
	Company      string                 `json:"company"`
	TotalRecords int                    `json:"totalRecords"`
	Charts       []SourceAnalyticsChart `json:"charts"`
}

type GetSourceCompanyAnalyticsOutput struct {
	Body SourceCompanyAnalytics
}

type HealthOutput struct {
	Body struct {
		Status string `json:"status"`
	}
}

func (h *Handler) GetAPIKey(ctx context.Context, input *GetAPIKeyInput) (*GetAPIKeyOutput, error) {
	accessToken, err := extractBearerToken(input.Authorization)
	if err != nil {
		return nil, huma.Error401Unauthorized(err.Error())
	}

	if err := h.accessKeyManager.Validate(accessToken); err != nil {
		return nil, huma.Error401Unauthorized("invalid API key access token")
	}

	token, expiresAt, err := h.keyManager.Token()
	if err != nil {
		return nil, huma.Error500InternalServerError("failed to issue API key")
	}
	if expiresAt == nil {
		return nil, huma.Error500InternalServerError("failed to issue expiring API key")
	}

	return &GetAPIKeyOutput{
		Body: APIKeyResponse{
			APIKey:    token,
			ExpiresAt: expiresAt,
		},
	}, nil
}

func (h *Handler) CreateSource(ctx context.Context, input *CreateSourceInput) (*CreateSourceOutput, error) {
	if err := h.authorizeAccessToken(input.Authorization); err != nil {
		return nil, err
	}

	if strings.TrimSpace(input.Body.Source) == "" || strings.TrimSpace(input.Body.Company) == "" || strings.TrimSpace(input.Body.City) == "" || strings.TrimSpace(input.Body.State) == "" || strings.TrimSpace(input.Body.Country) == "" || strings.TrimSpace(input.Body.WebsiteDomain) == "" {
		return nil, huma.Error400BadRequest("source, company, city, state, country, and websiteDomain are required")
	}

	source, err := h.store.CreateSource(
		ctx,
		strings.TrimSpace(input.Body.Source),
		strings.TrimSpace(input.Body.Company),
		strings.TrimSpace(input.Body.City),
		strings.TrimSpace(input.Body.State),
		strings.TrimSpace(input.Body.Country),
		strings.TrimSpace(input.Body.WebsiteDomain),
		toModelSchema(input.Body.TableSchema),
	)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrDuplicateSource):
			return nil, huma.Error409Conflict(formatSourceConflictMessage(input.Body.Source, input.Body.Company))
		case errors.Is(err, store.ErrInvalidSource), errors.Is(err, store.ErrSourceSchemaMismatch), errors.Is(err, store.ErrInvalidSourceOwner), errors.Is(err, store.ErrInvalidTableSchema), errors.Is(err, store.ErrInvalidLocation), errors.Is(err, store.ErrInvalidWebsiteDomain):
			return nil, huma.Error400BadRequest(err.Error())
		default:
			return nil, huma.Error500InternalServerError("failed to create source")
		}
	}

	return &CreateSourceOutput{Body: toSourceRecord(*source)}, nil
}

func (h *Handler) CreateProject(ctx context.Context, input *CreateProjectInput) (*CreateProjectOutput, error) {
	if err := h.authorizeAccessToken(input.Authorization); err != nil {
		return nil, err
	}

	projectName := strings.TrimSpace(input.Body.ProjectName)
	ingestionJWT := strings.TrimSpace(input.Body.IngestionJWT)
	if projectName == "" || ingestionJWT == "" {
		return nil, huma.Error400BadRequest("projectName and ingestionJwt are required")
	}

	if err := h.keyManager.Validate(ingestionJWT); err != nil {
		return nil, huma.Error400BadRequest("invalid ingestion JWT")
	}

	expiresAt, err := h.expiringIngestionJWTExpiry(ingestionJWT)
	if err != nil {
		return nil, huma.Error400BadRequest("invalid ingestion JWT")
	}

	project, err := h.store.CreateProject(ctx, projectName, ingestionJWT, expiresAt)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrDuplicateProject):
			return nil, huma.Error409Conflict(fmt.Sprintf("Project %q already exists.", projectName))
		case errors.Is(err, store.ErrInvalidProject):
			return nil, huma.Error400BadRequest(err.Error())
		default:
			return nil, huma.Error500InternalServerError("failed to create project")
		}
	}

	return &CreateProjectOutput{Body: toProjectRecord(*project)}, nil
}

func (h *Handler) BootstrapProject(ctx context.Context, input *struct{}) (*BootstrapProjectOutput, error) {
	accessJWT, _, err := h.accessKeyManager.Token()
	if err != nil {
		return nil, huma.Error500InternalServerError("failed to issue access JWT")
	}

	out := &BootstrapProjectOutput{}
	out.Body.AccessJWT = accessJWT

	sources, err := h.store.ListSources(ctx)
	if err != nil {
		return nil, huma.Error500InternalServerError("failed to load sources")
	}
	out.Body.Sources = toSourceRecords(sources)

	projects, err := h.store.ListProjects(ctx)
	if err != nil {
		return nil, huma.Error500InternalServerError("failed to load projects")
	}

	reusableProject := h.findReusableProjectJWT(projects)
	if reusableProject != nil {
		if err := h.repairInvalidProjects(ctx, projects, reusableProject.IngestionJWT, reusableProject.IngestionJWTExpiresAt); err != nil {
			return nil, huma.Error500InternalServerError("failed to normalize project JWTs")
		}

		projects, err = h.store.ListProjects(ctx)
		if err != nil {
			return nil, huma.Error500InternalServerError("failed to reload projects")
		}
	}

	out.Body.NeedsAccessJWT = reusableProject == nil
	out.Body.Projects, err = h.listProjectRecords(ctx, projects, reusableProject == nil)
	if err != nil {
		return nil, huma.Error500InternalServerError("failed to list project owners")
	}

	return out, nil
}

func (h *Handler) ListProjects(ctx context.Context, input *ListProjectsInput) (*ListProjectsOutput, error) {
	if err := h.authorizeAccessToken(input.Authorization); err != nil {
		return nil, err
	}

	projects, err := h.store.ListProjects(ctx)
	if err != nil {
		return nil, huma.Error500InternalServerError("failed to list projects")
	}

	out := &ListProjectsOutput{}
	out.Body.Projects, err = h.listProjectRecords(ctx, projects, false)
	if err != nil {
		return nil, huma.Error500InternalServerError("failed to list project owners")
	}

	return out, nil
}

func (h *Handler) DeleteProject(ctx context.Context, input *DeleteProjectInput) (*DeleteProjectOutput, error) {
	if err := h.authorizeAccessToken(input.Authorization); err != nil {
		return nil, err
	}

	if err := h.store.DeleteProject(ctx, strings.TrimSpace(input.ProjectName)); err != nil {
		switch {
		case errors.Is(err, store.ErrProjectNotFound):
			return nil, huma.Error404NotFound(err.Error())
		case errors.Is(err, store.ErrInvalidProject):
			return nil, huma.Error400BadRequest(err.Error())
		default:
			return nil, huma.Error500InternalServerError("failed to delete project")
		}
	}

	return &DeleteProjectOutput{}, nil
}

func (h *Handler) UpdateProjectIngestionJWT(ctx context.Context, input *UpdateProjectJWTInput) (*UpdateProjectJWTOutput, error) {
	if err := h.authorizeAccessToken(input.Authorization); err != nil {
		return nil, err
	}

	projectName := strings.TrimSpace(input.ProjectName)
	ingestionJWT := strings.TrimSpace(input.Body.IngestionJWT)
	if projectName == "" || ingestionJWT == "" {
		return nil, huma.Error400BadRequest("projectName and ingestionJwt are required")
	}

	if err := h.keyManager.Validate(ingestionJWT); err != nil {
		return nil, huma.Error400BadRequest("invalid ingestion JWT")
	}

	expiresAt, err := h.expiringIngestionJWTExpiry(ingestionJWT)
	if err != nil {
		return nil, huma.Error400BadRequest("invalid ingestion JWT")
	}

	project, err := h.store.UpdateProjectIngestionJWT(ctx, projectName, ingestionJWT, expiresAt)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrProjectNotFound):
			return nil, huma.Error404NotFound(err.Error())
		case errors.Is(err, store.ErrInvalidProject):
			return nil, huma.Error400BadRequest(err.Error())
		default:
			return nil, huma.Error500InternalServerError("failed to update project")
		}
	}

	return &UpdateProjectJWTOutput{Body: toProjectRecord(*project)}, nil
}

func (h *Handler) UpdateAllProjectsIngestionJWT(ctx context.Context, input *UpdateAllProjectsJWTInput) (*UpdateAllProjectsJWTOutput, error) {
	if err := h.authorizeAccessToken(input.Authorization); err != nil {
		return nil, err
	}

	ingestionJWT := strings.TrimSpace(input.Body.IngestionJWT)
	if ingestionJWT == "" {
		return nil, huma.Error400BadRequest("ingestionJwt is required")
	}

	if err := h.keyManager.Validate(ingestionJWT); err != nil {
		return nil, huma.Error400BadRequest("invalid ingestion JWT")
	}

	expiresAt, err := h.expiringIngestionJWTExpiry(ingestionJWT)
	if err != nil {
		return nil, huma.Error400BadRequest("invalid ingestion JWT")
	}

	projects, err := h.store.ListProjects(ctx)
	if err != nil {
		return nil, huma.Error500InternalServerError("failed to list projects")
	}

	if err := h.replaceProjectJWTs(ctx, projects, ingestionJWT, expiresAt); err != nil {
		return nil, huma.Error500InternalServerError("failed to update project JWTs")
	}

	projects, err = h.store.ListProjects(ctx)
	if err != nil {
		return nil, huma.Error500InternalServerError("failed to reload projects")
	}

	out := &UpdateAllProjectsJWTOutput{}
	out.Body.Projects, err = h.listProjectRecords(ctx, projects, false)
	if err != nil {
		return nil, huma.Error500InternalServerError("failed to list project owners")
	}

	return out, nil
}

func (h *Handler) AttachProjectOwner(ctx context.Context, input *AttachProjectOwnerInput) (*AttachProjectOwnerOutput, error) {
	if err := h.authorizeAccessToken(input.Authorization); err != nil {
		return nil, err
	}

	projectName := strings.TrimSpace(input.ProjectName)
	source := strings.TrimSpace(input.Body.Source)
	company := strings.TrimSpace(input.Body.Company)
	if projectName == "" || source == "" || company == "" {
		return nil, huma.Error400BadRequest("projectName, source, and company are required")
	}

	owner, err := h.store.AttachProjectSourceOwner(ctx, projectName, source, company)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrProjectNotFound), errors.Is(err, store.ErrSourceOwnerNotFound):
			return nil, huma.Error404NotFound(err.Error())
		case errors.Is(err, store.ErrDuplicateProjectLink):
			return nil, huma.Error409Conflict(fmt.Sprintf("Source %q for %q is already linked to project %q.", source, company, projectName))
		case errors.Is(err, store.ErrInvalidProject), errors.Is(err, store.ErrInvalidSource):
			return nil, huma.Error400BadRequest(err.Error())
		default:
			return nil, huma.Error500InternalServerError("failed to attach project owner")
		}
	}

	return &AttachProjectOwnerOutput{Body: toProjectOwnerRecord(*owner)}, nil
}

func (h *Handler) DeleteProjectOwner(ctx context.Context, input *DeleteProjectOwnerInput) (*DeleteProjectOwnerOutput, error) {
	if err := h.authorizeAccessToken(input.Authorization); err != nil {
		return nil, err
	}

	projectName := strings.TrimSpace(input.ProjectName)
	source := strings.TrimSpace(input.Source)
	company := strings.TrimSpace(input.Company)
	if projectName == "" || source == "" || company == "" {
		return nil, huma.Error400BadRequest("projectName, source, and company are required")
	}

	if err := h.store.DetachProjectSourceOwner(ctx, projectName, source, company); err != nil {
		switch {
		case errors.Is(err, store.ErrProjectNotFound), errors.Is(err, store.ErrSourceOwnerNotFound), errors.Is(err, store.ErrProjectOwnerLinkNotFound):
			return nil, huma.Error404NotFound(err.Error())
		case errors.Is(err, store.ErrInvalidProject), errors.Is(err, store.ErrInvalidSource):
			return nil, huma.Error400BadRequest(err.Error())
		default:
			return nil, huma.Error500InternalServerError("failed to detach project owner")
		}
	}

	return &DeleteProjectOwnerOutput{}, nil
}

func (h *Handler) ListProjectOwners(ctx context.Context, input *ListProjectOwnersInput) (*ListProjectOwnersOutput, error) {
	if err := h.authorizeAccessToken(input.Authorization); err != nil {
		return nil, err
	}

	owners, err := h.store.ListProjectSourceOwners(ctx, strings.TrimSpace(input.ProjectName))
	if err != nil {
		switch {
		case errors.Is(err, store.ErrProjectNotFound):
			return nil, huma.Error404NotFound(err.Error())
		case errors.Is(err, store.ErrInvalidProject):
			return nil, huma.Error400BadRequest(err.Error())
		default:
			return nil, huma.Error500InternalServerError("failed to list project owners")
		}
	}

	out := &ListProjectOwnersOutput{}
	for _, owner := range owners {
		out.Body.Owners = append(out.Body.Owners, toProjectOwnerRecord(owner))
	}

	return out, nil
}

func (h *Handler) GetProjectDashboard(ctx context.Context, input *GetProjectDashboardInput) (*GetProjectDashboardOutput, error) {
	if err := h.authorizeIngestionToken(input.Authorization); err != nil {
		return nil, err
	}

	projectName := strings.TrimSpace(input.ProjectName)
	source := strings.TrimSpace(input.Source)
	company := strings.TrimSpace(input.Company)
	if projectName == "" || source == "" || company == "" {
		return nil, huma.Error400BadRequest("projectName, source, and company are required")
	}

	owner, events, err := h.store.GetProjectOwnerEvents(ctx, projectName, source, company)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrProjectNotFound), errors.Is(err, store.ErrSourceOwnerNotFound), errors.Is(err, store.ErrProjectOwnerLinkNotFound):
			return nil, huma.Error404NotFound(err.Error())
		case errors.Is(err, store.ErrInvalidProject), errors.Is(err, store.ErrInvalidSource), errors.Is(err, store.ErrInvalidTableSchema):
			return nil, huma.Error400BadRequest(err.Error())
		default:
			return nil, huma.Error500InternalServerError("failed to load dashboard analytics")
		}
	}

	projects, err := h.store.ListProjects(ctx)
	if err != nil {
		return nil, huma.Error500InternalServerError("failed to load projects")
	}

	return &GetProjectDashboardOutput{
		Body: buildDashboardAnalytics(events, len(projects), *owner),
	}, nil
}

func (h *Handler) GetSourceCompanyAnalytics(ctx context.Context, input *GetSourceCompanyAnalyticsInput) (*GetSourceCompanyAnalyticsOutput, error) {
	if err := h.authorizeIngestionToken(input.Authorization); err != nil {
		return nil, err
	}

	source := strings.TrimSpace(input.Source)
	companyName := strings.TrimSpace(input.CompanyName)
	if source == "" || companyName == "" {
		return nil, huma.Error400BadRequest("source and companyName are required")
	}

	owner, events, err := h.store.GetSourceOwnerEvents(ctx, source, companyName)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrSourceOwnerNotFound):
			return nil, huma.Error404NotFound(err.Error())
		case errors.Is(err, store.ErrInvalidSource), errors.Is(err, store.ErrInvalidTableSchema):
			return nil, huma.Error400BadRequest(err.Error())
		default:
			return nil, huma.Error500InternalServerError("failed to load source analytics")
		}
	}

	return &GetSourceCompanyAnalyticsOutput{
		Body: buildSourceCompanyAnalytics(events, *owner),
	}, nil
}

func (h *Handler) ListSources(ctx context.Context, input *ListSourcesInput) (*ListSourcesOutput, error) {
	if err := h.authorizeAccessToken(input.Authorization); err != nil {
		return nil, err
	}

	sources, err := h.store.ListSources(ctx)
	if err != nil {
		return nil, huma.Error500InternalServerError("failed to list sources")
	}

	out := &ListSourcesOutput{}
	for _, source := range sources {
		out.Body.Sources = append(out.Body.Sources, toSourceRecord(source))
	}

	return out, nil
}

func (h *Handler) CreateEvent(ctx context.Context, input *CreateEventInput) (*CreateEventOutput, error) {
	if err := h.authorizeIngestionToken(input.Authorization); err != nil {
		return nil, err
	}

	if strings.TrimSpace(input.Body.Source) == "" || strings.TrimSpace(input.Body.Company) == "" || strings.TrimSpace(input.Body.City) == "" || strings.TrimSpace(input.Body.State) == "" || strings.TrimSpace(input.Body.Country) == "" {
		return nil, huma.Error400BadRequest("source, company, city, state, and country are required")
	}

	if len(input.Body.Payload) == 0 {
		return nil, huma.Error400BadRequest("payload is required")
	}

	event, err := h.store.CreateEvent(
		ctx,
		strings.TrimSpace(input.Body.Source),
		strings.TrimSpace(input.Body.Company),
		strings.TrimSpace(input.Body.City),
		strings.TrimSpace(input.Body.State),
		strings.TrimSpace(input.Body.Country),
		input.Body.Payload,
	)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrSourceNotFound):
			return nil, huma.Error404NotFound(err.Error())
		case errors.Is(err, store.ErrDuplicateEvent):
			return nil, huma.Error409Conflict("This event was already ingested.")
		case errors.Is(err, store.ErrInvalidSource), errors.Is(err, store.ErrInvalidPayload), errors.Is(err, store.ErrInvalidLocation):
			return nil, huma.Error400BadRequest(err.Error())
		default:
			return nil, huma.Error500InternalServerError("failed to store event")
		}
	}

	out := &CreateEventOutput{}
	out.Body.ID = event.ID
	out.Body.CreatedAt = event.CreatedAt
	return out, nil
}

func (h *Handler) SearchEvents(ctx context.Context, input *SearchEventsInput) (*SearchEventsOutput, error) {
	if err := h.authorizeIngestionToken(input.Authorization); err != nil {
		return nil, err
	}

	if strings.TrimSpace(input.Source) == "" || strings.TrimSpace(input.Company) == "" {
		return nil, huma.Error400BadRequest("source and company are required")
	}

	page := input.Page
	if page <= 0 {
		page = 1
	}

	events, total, err := h.store.SearchEvents(
		ctx,
		strings.TrimSpace(input.Source),
		strings.TrimSpace(input.Company),
		strings.TrimSpace(input.City),
		strings.TrimSpace(input.State),
		strings.TrimSpace(input.Country),
		strings.TrimSpace(input.Query),
		page,
		searchPageSize,
	)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrInvalidSource):
			return nil, huma.Error400BadRequest(err.Error())
		case errors.Is(err, store.ErrSourceOwnerNotFound):
			return nil, huma.Error404NotFound(err.Error())
		default:
			return nil, huma.Error500InternalServerError("failed to search events")
		}
	}

	out := &SearchEventsOutput{}
	out.Body.Page = page
	out.Body.PageSize = searchPageSize
	out.Body.Total = total
	for _, event := range events {
		out.Body.Results = append(out.Body.Results, SearchEventRecord(event))
	}

	return out, nil
}

func (h *Handler) Health(ctx context.Context, input *struct{}) (*HealthOutput, error) {
	if err := h.store.Ping(ctx); err != nil {
		return nil, huma.Error503ServiceUnavailable("database unavailable")
	}

	out := &HealthOutput{}
	out.Body.Status = "ok"
	return out, nil
}

func (h *Handler) authorizeAccessToken(header string) error {
	token, err := extractBearerToken(header)
	if err != nil {
		return huma.Error401Unauthorized(err.Error())
	}

	if err := h.accessKeyManager.Validate(token); err != nil {
		return huma.Error401Unauthorized("invalid API key access token")
	}

	return nil
}

func (h *Handler) authorizeIngestionToken(header string) error {
	token, err := extractBearerToken(header)
	if err != nil {
		return huma.Error401Unauthorized(err.Error())
	}

	if err := h.keyManager.Validate(token); err != nil {
		return huma.Error401Unauthorized("invalid API key")
	}

	return nil
}

func toModelSchema(columns []SourceSchemaColumn) models.TableSchema {
	schema := make(models.TableSchema, 0, len(columns))
	for _, column := range columns {
		schema = append(schema, models.TableColumn{
			Name:     column.Name,
			Type:     column.Type,
			Required: column.Required,
		})
	}

	return schema
}

func toSourceRecord(source models.Source) SourceRecord {
	record := SourceRecord{
		ID:             source.ID,
		SourceOwnerID:  source.SourceOwnerID,
		Source:         source.Source,
		Company:        source.Company,
		WebsiteDomain:  source.WebsiteDomain,
		City:           source.City,
		State:          source.State,
		Country:        source.Country,
		ChildTableName: source.ChildTableName,
		CreatedAt:      source.CreatedAt,
		UpdatedAt:      source.UpdatedAt,
	}
	for _, column := range source.TableSchema {
		record.TableSchema = append(record.TableSchema, SourceSchemaColumn{
			Name:     column.Name,
			Type:     column.Type,
			Required: column.Required,
		})
	}

	return record
}

func toSourceRecords(sources []models.Source) []SourceRecord {
	records := make([]SourceRecord, 0, len(sources))
	for _, source := range sources {
		records = append(records, toSourceRecord(source))
	}

	return records
}

func toProjectOwnerRecord(owner models.SourceOwner) ProjectOwnerRecord {
	record := ProjectOwnerRecord{
		ID:             owner.ID,
		Source:         owner.Source,
		Company:        owner.Company,
		WebsiteDomain:  owner.WebsiteDomain,
		ChildTableName: owner.ChildTableName,
	}
	for _, column := range owner.TableSchema {
		record.TableSchema = append(record.TableSchema, SourceSchemaColumn{
			Name:     column.Name,
			Type:     column.Type,
			Required: column.Required,
		})
	}

	return record
}

func toProjectRecord(project models.Project, owners ...[]models.SourceOwner) ProjectRecord {
	record := ProjectRecord{
		ID:                    project.ID,
		ProjectName:           project.ProjectName,
		IngestionJWT:          project.IngestionJWT,
		IngestionJWTExpiresAt: project.IngestionJWTExpiresAt,
		CreatedAt:             project.CreatedAt,
		UpdatedAt:             project.UpdatedAt,
	}
	if len(owners) > 0 {
		for _, owner := range owners[0] {
			record.Owners = append(record.Owners, toProjectOwnerRecord(owner))
		}
	}

	return record
}

func (h *Handler) projectJWTValid(project *models.Project) bool {
	if project == nil || strings.TrimSpace(project.IngestionJWT) == "" || project.IngestionJWTExpiresAt == nil {
		return false
	}

	if err := h.keyManager.Validate(project.IngestionJWT); err != nil {
		return false
	}

	expiresAt, err := h.keyManager.ExpiresAt(project.IngestionJWT)
	if err != nil {
		return false
	}

	if expiresAt == nil {
		return false
	}

	now := time.Now().UTC()
	return expiresAt.UTC().After(now) && project.IngestionJWTExpiresAt.UTC().After(now)
}

func (h *Handler) expiringIngestionJWTExpiry(token string) (*time.Time, error) {
	expiresAt, err := h.keyManager.ExpiresAt(token)
	if err != nil {
		return nil, err
	}
	if expiresAt == nil {
		return nil, errors.New("ingestion JWT missing expiry")
	}

	return expiresAt, nil
}

func (h *Handler) findReusableProjectJWT(projects []models.Project) *models.Project {
	for index := range projects {
		if h.projectJWTValid(&projects[index]) {
			return &projects[index]
		}
	}

	return nil
}

func (h *Handler) repairInvalidProjects(ctx context.Context, projects []models.Project, ingestionJWT string, expiresAt *time.Time) error {
	for _, project := range projects {
		if h.projectJWTValid(&project) {
			continue
		}

		if _, err := h.store.UpdateProjectIngestionJWT(ctx, project.ProjectName, ingestionJWT, expiresAt); err != nil {
			return err
		}
	}

	return nil
}

func (h *Handler) replaceProjectJWTs(ctx context.Context, projects []models.Project, ingestionJWT string, expiresAt *time.Time) error {
	for _, project := range projects {
		if _, err := h.store.UpdateProjectIngestionJWT(ctx, project.ProjectName, ingestionJWT, expiresAt); err != nil {
			return err
		}
	}

	return nil
}

func (h *Handler) listProjectRecords(ctx context.Context, projects []models.Project, maskInvalid bool) ([]ProjectRecord, error) {
	records := make([]ProjectRecord, 0, len(projects))
	for _, project := range projects {
		owners, err := h.store.ListProjectSourceOwners(ctx, project.ProjectName)
		if err != nil {
			return nil, err
		}

		record := toProjectRecord(project, owners)
		if maskInvalid && !h.projectJWTValid(&project) {
			record.IngestionJWT = ""
			record.IngestionJWTExpiresAt = nil
		}
		records = append(records, record)
	}

	return records, nil
}

func buildDashboardAnalytics(events []store.ChildEventRow, projectsCount int, owner models.SourceOwner) DashboardAnalytics {
	now := time.Now().UTC()
	uniqueLocations := map[string]int{}
	fieldStats := map[string]struct {
		total  int
		filled int
	}{}
	dayBuckets := createDayBuckets(now)
	last24Hours := 0
	last7Days := 0

	for _, event := range events {
		createdAt := event.CreatedAt.UTC()
		age := now.Sub(createdAt)
		if age <= 24*time.Hour {
			last24Hours += 1
		}
		if age <= 7*24*time.Hour {
			last7Days += 1
		}

		locationKey := fmt.Sprintf("%s, %s, %s", event.City, event.State, event.Country)
		uniqueLocations[locationKey] += 1

		bucketKey := createdAt.Format("2006-01-02")
		if _, ok := dayBuckets[bucketKey]; ok {
			dayBuckets[bucketKey] += 1
		}

		for key, value := range event.Payload {
			stat := fieldStats[key]
			stat.total += 1
			if value != nil && value != "" {
				stat.filled += 1
			}
			fieldStats[key] = stat
		}
	}

	schemaFields := owner.TableSchema
	if len(schemaFields) == 0 {
		for key := range fieldStats {
			schemaFields = append(schemaFields, models.TableColumn{Name: key})
		}
	}

	coverageFields := make([]DashboardCoverageField, 0, minInt(len(schemaFields), 4))
	for _, field := range schemaFields {
		stat := fieldStats[field.Name]
		base := maxInt(maxInt(stat.total, len(events)), 1)
		coverageFields = append(coverageFields, DashboardCoverageField{
			Name:     prettifyFieldName(field.Name),
			Percent:  roundedPercent(stat.filled, base),
			Required: field.Required,
		})
		if len(coverageFields) == 4 {
			break
		}
	}

	locationSegments := buildLocationSegments(uniqueLocations, len(events))
	trendPoints := make([]DashboardTrendPoint, 0, len(dayBuckets))
	for _, key := range orderedDayBucketKeys(dayBuckets) {
		trendPoints = append(trendPoints, DashboardTrendPoint{
			Label: formatDayLabel(key),
			Value: dayBuckets[key],
		})
	}

	requiredFields := 0
	for _, field := range owner.TableSchema {
		if field.Required {
			requiredFields += 1
		}
	}

	return DashboardAnalytics{
		TopMetrics: DashboardTopMetrics{
			ProjectsCount:  projectsCount,
			TotalRecords:   len(events),
			LocationsCount: len(uniqueLocations),
			Last24Hours:    last24Hours,
			Last7Days:      last7Days,
			ActiveDelta:    signedCount(projectsCount),
			RecordsDelta:   signedCount(last24Hours),
			LocationsDelta: signedCount(len(uniqueLocations)),
		},
		Coverage: DashboardCoverage{
			Overall: averageCoverage(coverageFields),
			Fields:  coverageFields,
			Summary: []DashboardSummaryItem{
				{Label: "Required fields", Value: requiredFields, Color: dashboardPalette[0]},
				{Label: "Optional fields", Value: maxInt(len(owner.TableSchema)-requiredFields, 0), Color: dashboardPalette[1]},
				{Label: "Records loaded", Value: len(events), Color: dashboardPalette[2]},
			},
		},
		LocationBreakdown: DashboardLocationBreakdown{
			Segments:   locationSegments,
			TotalLabel: fmt.Sprintf("%d locations", len(uniqueLocations)),
		},
		Trend: DashboardTrend{
			Points: trendPoints,
		},
	}
}

func createDayBuckets(now time.Time) map[string]int {
	buckets := make(map[string]int, 7)
	for index := 6; index >= 0; index -= 1 {
		day := now.AddDate(0, 0, -index)
		buckets[day.Format("2006-01-02")] = 0
	}

	return buckets
}

func orderedDayBucketKeys(buckets map[string]int) []string {
	keys := make([]string, 0, len(buckets))
	for key := range buckets {
		keys = append(keys, key)
	}
	// YYYY-MM-DD sorts lexically in chronological order.
	sort.Strings(keys)
	return keys
}

func buildLocationSegments(uniqueLocations map[string]int, totalRecords int) []DashboardLocationSegment {
	type locationCount struct {
		label string
		value int
	}

	locations := make([]locationCount, 0, len(uniqueLocations))
	for label, value := range uniqueLocations {
		locations = append(locations, locationCount{label: label, value: value})
	}

	sort.Slice(locations, func(left int, right int) bool {
		if locations[left].value == locations[right].value {
			return locations[left].label < locations[right].label
		}
		return locations[left].value > locations[right].value
	})
	if len(locations) > 4 {
		locations = locations[:4]
	}

	if len(locations) == 0 {
		return []DashboardLocationSegment{{
			Label:   "No records yet",
			Value:   0,
			Percent: 100,
			Color:   "#d8d9df",
			End:     100,
		}}
	}

	runningTotal := 0
	segments := make([]DashboardLocationSegment, 0, len(locations))
	for index, item := range locations {
		runningTotal += item.value
		segments = append(segments, DashboardLocationSegment{
			Label:   item.label,
			Value:   item.value,
			Percent: roundedPercent(item.value, maxInt(totalRecords, 1)),
			Color:   dashboardPalette[index%len(dashboardPalette)],
			End:     roundedPercent(runningTotal, maxInt(totalRecords, 1)),
		})
	}

	return segments
}

func buildSourceCompanyAnalytics(events []store.ChildEventRow, owner models.SourceOwner) SourceCompanyAnalytics {
	var charts []SourceAnalyticsChart

	switch owner.Source {
	case "Flights":
		charts = buildFlightsCharts(events)
	case "News":
		charts = buildNewsCharts(events)
	case "ECommerce":
		charts = buildECommerceCharts(events)
	case "Events":
		charts = buildEventsCharts(events)
	default:
		charts = []SourceAnalyticsChart{}
	}

	return SourceCompanyAnalytics{
		Source:       owner.Source,
		Company:      owner.Company,
		TotalRecords: len(events),
		Charts:       charts,
	}
}

func buildFlightsCharts(events []store.ChildEventRow) []SourceAnalyticsChart {
	statusCounts := map[string]int{}
	departureBuckets := map[time.Time]int{}
	destinationCounts := map[string]int{}
	punctualityCounts := map[string]int{}
	routeCounts := map[string]*flightRouteSummary{}

	var departureDelayTotal float64
	var departureDelayCount int
	var arrivalDelayTotal float64
	var arrivalDelayCount int

	for _, event := range events {
		addCount(statusCounts, payloadString(event.Payload, "status"))
		departureBucket, hasDepartureBucket := payloadTimeTruncated(event.Payload, "scheduled_departure_at", time.Hour)
		addTimeBucket(departureBuckets, departureBucket, hasDepartureBucket)

		origin := payloadString(event.Payload, "origin_iata")
		destination := payloadString(event.Payload, "destination_iata")
		if origin != "" && destination != "" {
			key := origin + " -> " + destination
			if routeCounts[key] == nil {
				routeCounts[key] = &flightRouteSummary{
					FromCode:  origin,
					ToCode:    destination,
					FromLabel: formatAirportLabel(origin, payloadString(event.Payload, "origin_city")),
					ToLabel:   formatAirportLabel(destination, payloadString(event.Payload, "destination_city")),
				}
			}
			routeCounts[key].Count += 1
			destinationCounts[destination] += 1
		}

		scheduledDeparture, hasScheduledDeparture := payloadTime(event.Payload, "scheduled_departure_at")
		actualDeparture, hasActualDeparture := payloadTime(event.Payload, "actual_departure_at")
		if hasScheduledDeparture && hasActualDeparture {
			delayMinutes := actualDeparture.Sub(scheduledDeparture).Minutes()
			departureDelayTotal += delayMinutes
			departureDelayCount += 1
			if delayMinutes > 15 {
				punctualityCounts["Delayed > 15 min"] += 1
			} else {
				punctualityCounts["On time <= 15 min"] += 1
			}
		}

		scheduledArrival, hasScheduledArrival := payloadTime(event.Payload, "scheduled_arrival_at")
		actualArrival, hasActualArrival := payloadTime(event.Payload, "actual_arrival_at")
		if hasScheduledArrival && hasActualArrival {
			arrivalDelayTotal += actualArrival.Sub(scheduledArrival).Minutes()
			arrivalDelayCount += 1
		}
	}

	return []SourceAnalyticsChart{
		buildCountChart("status-mix", "Status Mix", "Flights by status", "donut", statusCounts, []string{"scheduled", "boarding", "delayed", "departed", "landed"}, 0),
		buildTimeChart("departure-trend", "Scheduled Departures", "Hourly", "line", departureBuckets, 12),
		buildRouteMapChart("top-routes", "Top Routes", "Most active routes", routeCounts, 8),
		buildMetricChart("average-delays", "Average Delays", "Minutes", "bar", []SourceAnalyticsChartItem{
			makeMetricItem("Departure delay", roundedFloat(averageOrZero(departureDelayTotal, departureDelayCount), 2), "min"),
			makeMetricItem("Arrival delay", roundedFloat(averageOrZero(arrivalDelayTotal, arrivalDelayCount), 2), "min"),
		}),
		buildCountChart("departure-punctuality", "Departure Punctuality", "15 minute threshold", "donut", punctualityCounts, []string{"On time <= 15 min", "Delayed > 15 min"}, 0),
		buildCountChart("destinations", "Destination Mix", "Top destinations", "horizontal-bar", destinationCounts, nil, 8),
	}
}

func buildNewsCharts(events []store.ChildEventRow) []SourceAnalyticsChart {
	sectionCounts := map[string]int{}
	publicationBuckets := map[time.Time]int{}
	breakingCounts := map[string]int{}
	authorCounts := map[string]int{}
	headlineBuckets := map[string]int{}
	updateLagBuckets := map[string]int{}

	for _, event := range events {
		addCount(sectionCounts, payloadString(event.Payload, "section"))
		addCount(authorCounts, payloadString(event.Payload, "author_name"))
		publicationBucket, hasPublicationBucket := payloadTimeTruncated(event.Payload, "published_at", 24*time.Hour)
		addTimeBucket(publicationBuckets, publicationBucket, hasPublicationBucket)

		if isBreaking, ok := payloadBool(event.Payload, "is_breaking"); ok {
			if isBreaking {
				breakingCounts["Breaking"] += 1
			} else {
				breakingCounts["Standard"] += 1
			}
		}

		headlineLength := len(strings.TrimSpace(payloadString(event.Payload, "headline")))
		switch {
		case headlineLength == 0:
		case headlineLength < 40:
			headlineBuckets["< 40 chars"] += 1
		case headlineLength < 80:
			headlineBuckets["40-79 chars"] += 1
		case headlineLength < 120:
			headlineBuckets["80-119 chars"] += 1
		default:
			headlineBuckets["120+ chars"] += 1
		}

		publishedAt, hasPublishedAt := payloadTime(event.Payload, "published_at")
		updatedAt, hasUpdatedAt := payloadTime(event.Payload, "updated_at")
		if hasPublishedAt && hasUpdatedAt {
			lagHours := updatedAt.Sub(publishedAt).Hours()
			switch {
			case lagHours <= 1:
				updateLagBuckets["0-1h"] += 1
			case lagHours <= 6:
				updateLagBuckets["1-6h"] += 1
			case lagHours <= 24:
				updateLagBuckets["6-24h"] += 1
			default:
				updateLagBuckets["24h+"] += 1
			}
		}
	}

	return []SourceAnalyticsChart{
		buildCountChart("sections", "Section Mix", "Articles by section", "donut", sectionCounts, nil, 8),
		buildTimeChart("publication-trend", "Publication Trend", "Daily", "line", publicationBuckets, 12),
		buildCountChart("breaking-split", "Breaking Split", "Breaking vs standard", "donut", breakingCounts, []string{"Breaking", "Standard"}, 0),
		buildCountChart("authors", "Author Mix", "Top authors", "horizontal-bar", authorCounts, nil, 8),
		buildCountChart("headline-length", "Headline Length", "Character buckets", "bar", headlineBuckets, []string{"< 40 chars", "40-79 chars", "80-119 chars", "120+ chars"}, 0),
		buildCountChart("update-lag", "Update Lag", "Published to updated", "bar", updateLagBuckets, []string{"0-1h", "1-6h", "6-24h", "24h+"}, 0),
	}
}

func buildECommerceCharts(events []store.ChildEventRow) []SourceAnalyticsChart {
	paymentCounts := map[string]int{}
	fulfillmentCounts := map[string]int{}
	orderBuckets := map[time.Time]int{}
	itemBuckets := map[string]int{}
	expeditedCounts := map[string]int{}

	var subtotalTotal float64
	var shippingTotal float64
	var taxTotal float64
	var discountTotal float64
	var totalTotal float64

	for _, event := range events {
		addCount(paymentCounts, payloadString(event.Payload, "payment_status"))
		addCount(fulfillmentCounts, payloadString(event.Payload, "fulfillment_status"))
		orderBucket, hasOrderBucket := payloadTimeTruncated(event.Payload, "placed_at", 24*time.Hour)
		addTimeBucket(orderBuckets, orderBucket, hasOrderBucket)

		if itemCount, ok := payloadInt(event.Payload, "item_count"); ok {
			switch {
			case itemCount <= 2:
				itemBuckets["1-2 items"] += 1
			case itemCount <= 5:
				itemBuckets["3-5 items"] += 1
			case itemCount <= 10:
				itemBuckets["6-10 items"] += 1
			default:
				itemBuckets["11+ items"] += 1
			}
		}

		if expedited, ok := payloadBool(event.Payload, "is_expedited"); ok {
			if expedited {
				expeditedCounts["Expedited"] += 1
			} else {
				expeditedCounts["Standard"] += 1
			}
		}

		subtotalTotal += payloadFloatValue(event.Payload, "subtotal_amount")
		shippingTotal += payloadFloatValue(event.Payload, "shipping_amount")
		taxTotal += payloadFloatValue(event.Payload, "tax_amount")
		discountTotal += payloadFloatValue(event.Payload, "discount_amount")
		totalTotal += payloadFloatValue(event.Payload, "total_amount")
	}

	count := len(events)
	return []SourceAnalyticsChart{
		buildCountChart("payment-status", "Payment Status", "Orders by payment state", "donut", paymentCounts, nil, 0),
		buildCountChart("fulfillment-status", "Fulfillment Status", "Orders by fulfillment state", "donut", fulfillmentCounts, nil, 0),
		buildTimeChart("order-trend", "Orders Over Time", "Daily", "line", orderBuckets, 12),
		buildMetricChart("amount-averages", "Average Order Amounts", "Per order", "bar", []SourceAnalyticsChartItem{
			makeMetricItem("Subtotal", roundedFloat(averageOrZero(subtotalTotal, count), 2), ""),
			makeMetricItem("Shipping", roundedFloat(averageOrZero(shippingTotal, count), 2), ""),
			makeMetricItem("Tax", roundedFloat(averageOrZero(taxTotal, count), 2), ""),
			makeMetricItem("Discount", roundedFloat(averageOrZero(discountTotal, count), 2), ""),
			makeMetricItem("Total", roundedFloat(averageOrZero(totalTotal, count), 2), ""),
		}),
		buildCountChart("item-count", "Item Count", "Items per order", "bar", itemBuckets, []string{"1-2 items", "3-5 items", "6-10 items", "11+ items"}, 0),
		buildCountChart("expedited", "Shipping Speed", "Expedited vs standard", "donut", expeditedCounts, []string{"Expedited", "Standard"}, 0),
	}
}

func buildEventsCharts(events []store.ChildEventRow) []SourceAnalyticsChart {
	categoryCounts := map[string]int{}
	availabilityCounts := map[string]int{}
	startBuckets := map[time.Time]int{}
	soldOutCounts := map[string]int{}
	venueCityCounts := map[string]int{}
	priceBuckets := map[string]int{}

	for _, event := range events {
		addCount(categoryCounts, payloadString(event.Payload, "event_category"))
		addCount(availabilityCounts, payloadString(event.Payload, "availability_status"))
		addCount(venueCityCounts, payloadString(event.Payload, "venue_city"))
		startBucket, hasStartBucket := payloadTimeTruncated(event.Payload, "starts_at", 24*time.Hour)
		addTimeBucket(startBuckets, startBucket, hasStartBucket)

		if soldOut, ok := payloadBool(event.Payload, "is_sold_out"); ok {
			if soldOut {
				soldOutCounts["Sold out"] += 1
			} else {
				soldOutCounts["Available"] += 1
			}
		}

		price := payloadFloatValue(event.Payload, "base_ticket_price")
		switch {
		case price <= 0:
		case price < 50:
			priceBuckets["< 50"] += 1
		case price < 100:
			priceBuckets["50-99"] += 1
		case price < 200:
			priceBuckets["100-199"] += 1
		default:
			priceBuckets["200+"] += 1
		}
	}

	return []SourceAnalyticsChart{
		buildCountChart("categories", "Category Mix", "Events by category", "donut", categoryCounts, nil, 8),
		buildCountChart("availability", "Availability Status", "Inventory state", "donut", availabilityCounts, nil, 0),
		buildTimeChart("start-trend", "Event Start Trend", "Daily", "line", startBuckets, 12),
		buildCountChart("sold-out", "Sold Out Split", "Availability snapshot", "donut", soldOutCounts, []string{"Available", "Sold out"}, 0),
		buildCountChart("venue-cities", "Venue City Mix", "Top venues", "horizontal-bar", venueCityCounts, nil, 8),
		buildCountChart("ticket-price", "Ticket Price Buckets", "Base ticket price", "bar", priceBuckets, []string{"< 50", "50-99", "100-199", "200+"}, 0),
	}
}

type flightRouteSummary struct {
	Count     int
	FromCode  string
	ToCode    string
	FromLabel string
	ToLabel   string
}

type flightAirportLocation struct {
	Label string
	Lat   float64
	Lng   float64
}

func buildCountChart(id string, title string, subtitle string, kind string, counts map[string]int, preferredOrder []string, limit int) SourceAnalyticsChart {
	return SourceAnalyticsChart{
		ID:       id,
		Title:    title,
		Subtitle: subtitle,
		Kind:     kind,
		Items:    buildCountItems(counts, preferredOrder, limit),
	}
}

func buildMetricChart(id string, title string, subtitle string, kind string, items []SourceAnalyticsChartItem) SourceAnalyticsChart {
	return SourceAnalyticsChart{
		ID:       id,
		Title:    title,
		Subtitle: subtitle,
		Kind:     kind,
		Items:    items,
	}
}

func buildTimeChart(id string, title string, subtitle string, kind string, buckets map[time.Time]int, limit int) SourceAnalyticsChart {
	return SourceAnalyticsChart{
		ID:       id,
		Title:    title,
		Subtitle: subtitle,
		Kind:     kind,
		Items:    buildTimeItems(buckets, limit),
	}
}

func buildRouteMapChart(id string, title string, subtitle string, counts map[string]*flightRouteSummary, limit int) SourceAnalyticsChart {
	return SourceAnalyticsChart{
		ID:       id,
		Title:    title,
		Subtitle: subtitle,
		Kind:     "route-map",
		Items:    buildRouteItems(counts, limit),
	}
}

func buildCountItems(counts map[string]int, preferredOrder []string, limit int) []SourceAnalyticsChartItem {
	type labelCount struct {
		label string
		value int
	}

	items := make([]SourceAnalyticsChartItem, 0, len(counts))
	used := map[string]bool{}

	for _, label := range preferredOrder {
		if value, ok := counts[label]; ok {
			items = append(items, makeCountItem(label, value))
			used[label] = true
		}
	}

	remaining := make([]labelCount, 0, len(counts))
	for label, value := range counts {
		if used[label] {
			continue
		}
		remaining = append(remaining, labelCount{label: label, value: value})
	}

	sort.Slice(remaining, func(left int, right int) bool {
		if remaining[left].value == remaining[right].value {
			return remaining[left].label < remaining[right].label
		}
		return remaining[left].value > remaining[right].value
	})

	for _, item := range remaining {
		items = append(items, makeCountItem(item.label, item.value))
	}

	if limit > 0 && len(items) > limit {
		items = items[:limit]
	}

	return items
}

func buildRouteItems(counts map[string]*flightRouteSummary, limit int) []SourceAnalyticsChartItem {
	type routeEntry struct {
		key   string
		value *flightRouteSummary
	}

	routes := make([]routeEntry, 0, len(counts))
	for key, value := range counts {
		if value == nil {
			continue
		}
		routes = append(routes, routeEntry{key: key, value: value})
	}

	sort.Slice(routes, func(left int, right int) bool {
		if routes[left].value.Count == routes[right].value.Count {
			return routes[left].key < routes[right].key
		}
		return routes[left].value.Count > routes[right].value.Count
	})

	items := make([]SourceAnalyticsChartItem, 0, len(routes))
	for _, route := range routes {
		fromLocation, hasFrom := lookupFlightAirportLocation(route.value.FromCode)
		toLocation, hasTo := lookupFlightAirportLocation(route.value.ToCode)
		if !hasFrom || !hasTo {
			continue
		}

		fromLabel := route.value.FromLabel
		if strings.TrimSpace(fromLabel) == "" {
			fromLabel = formatAirportLabel(route.value.FromCode, fromLocation.Label)
		}
		toLabel := route.value.ToLabel
		if strings.TrimSpace(toLabel) == "" {
			toLabel = formatAirportLabel(route.value.ToCode, toLocation.Label)
		}

		items = append(items, SourceAnalyticsChartItem{
			Label:      route.key,
			Value:      float64(route.value.Count),
			ValueLabel: strconv.Itoa(route.value.Count),
			Detail:     fmt.Sprintf("%s to %s: %d flights", fromLabel, toLabel, route.value.Count),
			FromCode:   route.value.FromCode,
			ToCode:     route.value.ToCode,
			FromLabel:  fromLabel,
			ToLabel:    toLabel,
			FromLat:    fromLocation.Lat,
			FromLng:    fromLocation.Lng,
			ToLat:      toLocation.Lat,
			ToLng:      toLocation.Lng,
		})
		if limit > 0 && len(items) >= limit {
			break
		}
	}

	return items
}

func lookupFlightAirportLocation(code string) (flightAirportLocation, bool) {
	airport, err := reference.LookupAirport(code)
	if err != nil {
		return flightAirportLocation{}, false
	}

	label := strings.TrimSpace(airport.City)
	if label == "" {
		label = strings.TrimSpace(code)
	}

	return flightAirportLocation{
		Label: label,
		Lat:   airport.Lat,
		Lng:   airport.Lng,
	}, true
}

func buildTimeItems(buckets map[time.Time]int, limit int) []SourceAnalyticsChartItem {
	keys := make([]time.Time, 0, len(buckets))
	for bucket := range buckets {
		keys = append(keys, bucket)
	}
	sort.Slice(keys, func(left int, right int) bool {
		return keys[left].Before(keys[right])
	})

	if limit > 0 && len(keys) > limit {
		keys = keys[len(keys)-limit:]
	}

	items := make([]SourceAnalyticsChartItem, 0, len(keys))
	for _, bucket := range keys {
		items = append(items, SourceAnalyticsChartItem{
			Label:      bucket.Format("2006-01-02 15:00"),
			Value:      float64(buckets[bucket]),
			ValueLabel: strconv.Itoa(buckets[bucket]),
			Detail:     fmt.Sprintf("%s: %d", bucket.Format("2006-01-02 15:00"), buckets[bucket]),
		})
	}

	return items
}

func makeCountItem(label string, value int) SourceAnalyticsChartItem {
	return SourceAnalyticsChartItem{
		Label:      label,
		Value:      float64(value),
		ValueLabel: strconv.Itoa(value),
		Detail:     fmt.Sprintf("%s: %d", label, value),
	}
}

func makeMetricItem(label string, value float64, unit string) SourceAnalyticsChartItem {
	valueLabel := fmt.Sprintf("%.2f", value)
	if unit != "" {
		valueLabel = fmt.Sprintf("%s %s", valueLabel, unit)
	}

	return SourceAnalyticsChartItem{
		Label:      label,
		Value:      value,
		ValueLabel: valueLabel,
		Detail:     fmt.Sprintf("%s: %s", label, valueLabel),
	}
}

func formatAirportLabel(code string, city string) string {
	trimmedCode := strings.TrimSpace(code)
	trimmedCity := strings.TrimSpace(city)
	if trimmedCode == "" {
		return trimmedCity
	}
	if trimmedCity == "" {
		return trimmedCode
	}
	return fmt.Sprintf("%s (%s)", trimmedCity, trimmedCode)
}

func payloadString(payload map[string]any, key string) string {
	value, ok := payload[key]
	if !ok || value == nil {
		return ""
	}

	switch candidate := value.(type) {
	case string:
		return candidate
	default:
		return fmt.Sprintf("%v", candidate)
	}
}

func payloadTime(payload map[string]any, key string) (time.Time, bool) {
	value, ok := payload[key]
	if !ok || value == nil {
		return time.Time{}, false
	}

	switch candidate := value.(type) {
	case time.Time:
		return candidate, true
	case *time.Time:
		if candidate == nil {
			return time.Time{}, false
		}
		return *candidate, true
	case string:
		parsed, err := time.Parse(time.RFC3339, candidate)
		if err != nil {
			return time.Time{}, false
		}
		return parsed, true
	default:
		return time.Time{}, false
	}
}

func payloadTimeTruncated(payload map[string]any, key string, bucketSize time.Duration) (time.Time, bool) {
	value, ok := payloadTime(payload, key)
	if !ok || bucketSize <= 0 {
		return time.Time{}, false
	}

	return value.Truncate(bucketSize), true
}

func payloadBool(payload map[string]any, key string) (bool, bool) {
	value, ok := payload[key]
	if !ok || value == nil {
		return false, false
	}

	switch candidate := value.(type) {
	case bool:
		return candidate, true
	case string:
		parsed, err := strconv.ParseBool(strings.TrimSpace(candidate))
		if err != nil {
			return false, false
		}
		return parsed, true
	default:
		return false, false
	}
}

func payloadInt(payload map[string]any, key string) (int, bool) {
	value, ok := payload[key]
	if !ok || value == nil {
		return 0, false
	}

	switch candidate := value.(type) {
	case int:
		return candidate, true
	case int8:
		return int(candidate), true
	case int16:
		return int(candidate), true
	case int32:
		return int(candidate), true
	case int64:
		return int(candidate), true
	case uint:
		return int(candidate), true
	case uint8:
		return int(candidate), true
	case uint16:
		return int(candidate), true
	case uint32:
		return int(candidate), true
	case uint64:
		return int(candidate), true
	case float32:
		return int(candidate), true
	case float64:
		return int(candidate), true
	case string:
		parsed, err := strconv.Atoi(strings.TrimSpace(candidate))
		if err != nil {
			return 0, false
		}
		return parsed, true
	default:
		return 0, false
	}
}

func payloadFloatValue(payload map[string]any, key string) float64 {
	value, ok := payload[key]
	if !ok || value == nil {
		return 0
	}

	switch candidate := value.(type) {
	case float32:
		return float64(candidate)
	case float64:
		return candidate
	case int:
		return float64(candidate)
	case int8:
		return float64(candidate)
	case int16:
		return float64(candidate)
	case int32:
		return float64(candidate)
	case int64:
		return float64(candidate)
	case uint:
		return float64(candidate)
	case uint8:
		return float64(candidate)
	case uint16:
		return float64(candidate)
	case uint32:
		return float64(candidate)
	case uint64:
		return float64(candidate)
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(candidate), 64)
		if err != nil {
			return 0
		}
		return parsed
	default:
		return 0
	}
}

func addCount(counts map[string]int, rawLabel string) {
	label := strings.TrimSpace(rawLabel)
	if label == "" {
		return
	}

	counts[label] += 1
}

func addTimeBucket(buckets map[time.Time]int, bucket time.Time, ok bool) {
	if !ok {
		return
	}

	buckets[bucket] += 1
}

func averageOrZero(total float64, count int) float64 {
	if count == 0 {
		return 0
	}
	return total / float64(count)
}

func roundedFloat(value float64, places int) float64 {
	scale := math.Pow10(places)
	return math.Round(value*scale) / scale
}

func averageCoverage(fields []DashboardCoverageField) int {
	if len(fields) == 0 {
		return 0
	}

	total := 0
	for _, field := range fields {
		total += field.Percent
	}

	return int(math.Round(float64(total) / float64(len(fields))))
}

func formatDayLabel(dateString string) string {
	day, err := time.Parse("2006-01-02", dateString)
	if err != nil {
		return dateString
	}

	return day.Format("Mon")
}

func prettifyFieldName(name string) string {
	return strings.ReplaceAll(name, "_", " ")
}

func roundedPercent(value int, base int) int {
	if base <= 0 {
		return 0
	}

	return int(math.Round((float64(value) / float64(base)) * 100))
}

func signedCount(value int) string {
	if value > 0 {
		return fmt.Sprintf("+%d", value)
	}

	return fmt.Sprintf("%d", value)
}

func maxInt(left int, right int) int {
	if left > right {
		return left
	}

	return right
}

func minInt(left int, right int) int {
	if left < right {
		return left
	}

	return right
}

func extractBearerToken(header string) (string, error) {
	scheme, token, ok := strings.Cut(header, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || strings.TrimSpace(token) == "" {
		return "", huma.Error401Unauthorized("missing bearer token")
	}

	return strings.TrimSpace(token), nil
}

func formatSourceConflictMessage(source string, company string) string {
	trimmedSource := strings.TrimSpace(source)
	trimmedCompany := strings.TrimSpace(company)
	if trimmedSource == "" && trimmedCompany == "" {
		return "That source already exists."
	}
	if trimmedSource == "" {
		return fmt.Sprintf("A source for %q already exists.", trimmedCompany)
	}
	if trimmedCompany == "" {
		return fmt.Sprintf("Source %q already exists.", trimmedSource)
	}

	return fmt.Sprintf("Source %q for %q already exists.", trimmedSource, trimmedCompany)
}
