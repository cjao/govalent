package api

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/casey/govalent/server/common"
	"github.com/casey/govalent/server/models"
)

// Dispatch management

type RequestHandler struct {
	config      *common.Config
	dbPool      *sql.DB
	handlerFunc func(*common.Config, *sql.DB, http.ResponseWriter, *http.Request) int
}

type PaginationParams struct {
	Count int
	Page  int
}

type JSONRequest interface {
	// JSON-decode and validate
	DecodeJSON(*json.Decoder) *models.APIError
}

type JSONResponse interface {
	// Validate and JSON-encode
	EncodeJSON(*json.Encoder) *models.APIError
}

type GovalentAPIServer struct {
	Srv      *http.Server
	mux      *http.ServeMux
	patterns []string
	config   *common.Config
}

func NewGovalentAPIServer(c *common.Config, addr string) *GovalentAPIServer {
	mux := http.NewServeMux()
	return &GovalentAPIServer{
		Srv:      &http.Server{Addr: addr, Handler: mux},
		mux:      mux,
		patterns: make([]string, 0),
		config:   c,
	}
}

func (m *GovalentAPIServer) AddRoute(verb string, path string, handler RequestHandler) {
	pattern := fmt.Sprintf("%s %s%s", verb, m.config.APIPrefix, path)
	m.mux.Handle(pattern, handler)
	m.patterns = append(m.patterns, pattern)
	// TODO: add instrospection route
}

func NewPaginationParamsFromReq(r *http.Request) (PaginationParams, *models.APIError) {
	count, err := extractQueryInt(r, "count", 10)
	if err != nil {
		return PaginationParams{}, err
	}
	page, err := extractQueryInt(r, "page", 0)
	if err != nil {
		return PaginationParams{}, err
	}
	return PaginationParams{Count: count, Page: page}, nil
}

func (h RequestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	code := h.handlerFunc(h.config, h.dbPool, w, r)
	slog.Info(fmt.Sprintf("%s %s %s %d\n", r.Method, r.URL.Path, r.Proto, code))
}

// Introspection route
func (s *GovalentAPIServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	respBody := models.APIIntrospectionResponse{Routes: s.patterns}
	code := writeJSONResponse(w, &respBody)
	slog.Info(fmt.Sprintf("%s %s %s %d\n", r.Method, r.URL.Path, r.Proto, code))
}

func writeJSONResponse(w http.ResponseWriter, respBody JSONResponse) int {
	enc := json.NewEncoder(w)
	err := respBody.EncodeJSON(enc)
	if err != nil {
		slog.Info(fmt.Sprint("Error JSON serializing response:", err.Error()))
		models.WriteError(w, err)
		return http.StatusInternalServerError
	}
	return http.StatusOK
}

// Parsing and validation utility functions
func extractQueryString(r *http.Request, key string, default_value string) (string, *models.APIError) {
	val := r.FormValue(key)
	if len(val) == 0 {
		return default_value, nil
	} else {
		return val, nil
	}
}

func extractQueryInt(r *http.Request, key string, default_value int) (int, *models.APIError) {
	val := r.FormValue(key)
	if len(val) == 0 {
		return default_value, nil
	}
	i, err := strconv.Atoi(val)
	if err != nil {
		return 0, models.NewValidationError(err)
	}
	return i, nil
}

func extractPathString(r *http.Request, key string) (string, *models.APIError) {
	val := r.PathValue(key)
	if len(val) == 0 {
		e := fmt.Errorf("Expected path parameter %s", key)
		return "", &models.APIError{StatusCode: 422, Err: e}
	} else {
		return val, nil
	}
}

func extractPathInt(r *http.Request, key string) (int, *models.APIError) {
	val := r.PathValue(key)
	if len(val) == 0 {
		e := fmt.Errorf("Expected path parameter %s", key)
		return 0, &models.APIError{StatusCode: 422, Err: e}
	}
	i, err := strconv.Atoi(val)
	if err != nil {
		e := fmt.Errorf("Expected integer for path parameter %s", key)
		return 0, &models.APIError{StatusCode: 422, Err: e}
	} else {
		return i, nil
	}
}

// GET /config
func handleGetConfig(c *common.Config, d *sql.DB, w http.ResponseWriter, r *http.Request) int {
	configResponse := models.ConfigResponse{Config: c}
	writeJSONResponse(w, &configResponse)
	return http.StatusOK
}

// TODO: PUT /dispatches/{dispatch_id}/status
// TODO: PATCH /dispatches/{dispatch_id}/electrons/{node_id}

// Assets
//
// POST /assets

func (m *GovalentAPIServer) AddRoutes(c *common.Config, d *sql.DB) {
	dump_config_handler := RequestHandler{
		config:      c,
		dbPool:      d,
		handlerFunc: handleGetConfig,
	}
	create_dispatch_handler := RequestHandler{
		config:      c,
		dbPool:      d,
		handlerFunc: handleImportManifest,
	}
	bulk_get_dispatches_handler := RequestHandler{
		config:      c,
		dbPool:      d,
		handlerFunc: handleGetDispatches,
	}
	delete_dispatch_handler := RequestHandler{
		config:      c,
		dbPool:      d,
		handlerFunc: handleDeleteDispatch,
	}
	export_manifest_handler := RequestHandler{
		config:      c,
		dbPool:      d,
		handlerFunc: handleExportManifest,
	}
	get_dispatch_asset_links_handler := RequestHandler{
		config:      c,
		dbPool:      d,
		handlerFunc: handleGetDispatchAssetLinks,
	}
	create_assets_handler := RequestHandler{
		config:      c,
		dbPool:      d,
		handlerFunc: handleCreateAssets,
	}
	export_assets_handler := RequestHandler{
		config:      c,
		dbPool:      d,
		handlerFunc: handleExportAssets,
	}

	get_electron_asset_links_handler := RequestHandler{
		config:      c,
		dbPool:      d,
		handlerFunc: handleGetElectronAssetLinks,
	}

	m.AddRoute("GET", "/config", dump_config_handler)
	m.AddRoute("POST", "/dispatches", create_dispatch_handler)
	m.AddRoute("GET", "/dispatches", bulk_get_dispatches_handler)
	m.AddRoute("DELETE", "/dispatches/{dispatch_id}", delete_dispatch_handler)
	m.AddRoute("GET", "/dispatches/{dispatch_id}", export_manifest_handler)
	m.AddRoute("GET", "/dispatches/{dispatch_id}/assets", get_dispatch_asset_links_handler)

	m.AddRoute("GET", "/dispatches/{dispatch_id}/electrons/{node_id}/assets", get_electron_asset_links_handler)

	m.AddRoute("POST", "/assets", create_assets_handler)
	m.AddRoute("GET", "/assets", export_assets_handler)

	// TODO: add introspection route
	m.mux.Handle("GET /introspection", m)
}
