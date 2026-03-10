package api

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/macho_prawn/events-dashboard/internal/auth"
	"github.com/macho_prawn/events-dashboard/internal/models"
	"github.com/macho_prawn/events-dashboard/internal/store"
)

const searchPageSize = 50

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
	Source      string               `json:"source" minLength:"1"`
	Company     string               `json:"company" minLength:"1"`
	City        string               `json:"city" minLength:"1"`
	State       string               `json:"state" minLength:"1"`
	Country     string               `json:"country" minLength:"1"`
	TableSchema []SourceSchemaColumn `json:"tableSchema"`
}

type CreateSourceInput struct {
	Authorization string           `header:"Authorization" doc:"Bearer JWT used to access source management endpoints."`
	Body          CreateSourceBody `json:"body"`
}

type SourceRecord struct {
	ID             uint                 `json:"id"`
	Source         string               `json:"source"`
	Company        string               `json:"company"`
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

	if strings.TrimSpace(input.Body.Source) == "" || strings.TrimSpace(input.Body.Company) == "" || strings.TrimSpace(input.Body.City) == "" || strings.TrimSpace(input.Body.State) == "" || strings.TrimSpace(input.Body.Country) == "" {
		return nil, huma.Error400BadRequest("source, company, city, state, and country are required")
	}

	source, err := h.store.CreateSource(
		ctx,
		strings.TrimSpace(input.Body.Source),
		strings.TrimSpace(input.Body.Company),
		strings.TrimSpace(input.Body.City),
		strings.TrimSpace(input.Body.State),
		strings.TrimSpace(input.Body.Country),
		toModelSchema(input.Body.TableSchema),
	)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrDuplicateSource):
			return nil, huma.Error409Conflict(err.Error())
		case errors.Is(err, store.ErrInvalidSource), errors.Is(err, store.ErrSourceSchemaMismatch), errors.Is(err, store.ErrInvalidSourceOwner), errors.Is(err, store.ErrInvalidTableSchema), errors.Is(err, store.ErrInvalidLocation):
			return nil, huma.Error400BadRequest(err.Error())
		default:
			return nil, huma.Error500InternalServerError("failed to create source")
		}
	}

	return &CreateSourceOutput{Body: toSourceRecord(*source)}, nil
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
			return nil, huma.Error409Conflict(err.Error())
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
		Source:         source.Source,
		Company:        source.Company,
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

func extractBearerToken(header string) (string, error) {
	scheme, token, ok := strings.Cut(header, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || strings.TrimSpace(token) == "" {
		return "", huma.Error401Unauthorized("missing bearer token")
	}

	return strings.TrimSpace(token), nil
}
