package api

import (
	"net/http"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"
	"github.com/macho_prawn/events-dashboard/internal/auth"
	"github.com/macho_prawn/events-dashboard/internal/store"
)

type Dependencies struct {
	Store            store.EventStore
	KeyManager       *auth.Manager
	AccessKeyManager *auth.Manager
}

func NewPublicServer(deps Dependencies) (http.Handler, error) {
	handler := &Handler{
		store:            deps.Store,
		keyManager:       deps.KeyManager,
		accessKeyManager: deps.AccessKeyManager,
	}

	config := huma.DefaultConfig("Events Dashboard API", "1.0.0")
	config.Info.Description = "JWT-protected source-driven event ingestion service backed by PostgreSQL."

	mux := http.NewServeMux()
	api := humago.New(mux, config)

	huma.Register(api, huma.Operation{
		OperationID: "get-api-key",
		Method:      http.MethodGet,
		Path:        "/api-key",
		Summary:     "Get the ingestion/search API key",
		Tags:        []string{"auth"},
	}, handler.GetAPIKey)

	huma.Register(api, huma.Operation{
		OperationID: "create-source",
		Method:      http.MethodPost,
		Path:        "/source",
		Summary:     "Create a source and source-owned child table",
		Tags:        []string{"sources"},
	}, handler.CreateSource)

	huma.Register(api, huma.Operation{
		OperationID: "list-sources",
		Method:      http.MethodGet,
		Path:        "/source",
		Summary:     "List source records",
		Tags:        []string{"sources"},
	}, handler.ListSources)

	huma.Register(api, huma.Operation{
		OperationID: "create-event",
		Method:      http.MethodPost,
		Path:        "/events",
		Summary:     "Create an event for a source-specific child table",
		Tags:        []string{"events"},
	}, handler.CreateEvent)

	huma.Register(api, huma.Operation{
		OperationID: "search-events",
		Method:      http.MethodGet,
		Path:        "/search",
		Summary:     "Search events within a source/company child table",
		Tags:        []string{"events"},
	}, handler.SearchEvents)

	huma.Register(api, huma.Operation{
		OperationID: "health-check",
		Method:      http.MethodGet,
		Path:        "/healthz",
		Summary:     "Health check",
		Tags:        []string{"system"},
	}, handler.Health)

	return disableCORS(mux), nil
}

func disableCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions {
			stripCORSHeaders(w.Header())
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		next.ServeHTTP(&corsDisabledResponseWriter{ResponseWriter: w}, r)
	})
}

type corsDisabledResponseWriter struct {
	http.ResponseWriter
}

func (w *corsDisabledResponseWriter) WriteHeader(statusCode int) {
	stripCORSHeaders(w.Header())
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *corsDisabledResponseWriter) Write(b []byte) (int, error) {
	stripCORSHeaders(w.Header())
	return w.ResponseWriter.Write(b)
}

func stripCORSHeaders(headers http.Header) {
	headers.Del("Access-Control-Allow-Origin")
	headers.Del("Access-Control-Allow-Credentials")
	headers.Del("Access-Control-Allow-Headers")
	headers.Del("Access-Control-Allow-Methods")
	headers.Del("Access-Control-Expose-Headers")
	headers.Del("Access-Control-Max-Age")
}
