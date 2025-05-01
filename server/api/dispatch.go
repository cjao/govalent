package api

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/casey/govalent/server/common"
	"github.com/casey/govalent/server/crud"
	"github.com/casey/govalent/server/models"
)

// POST /dispatches
func importManifest(c *common.Config, d *sql.DB, manifest *models.DispatchSchema) (*models.DispatchSchema, *models.APIError) {

	// Set root_dispatch_id
	manifest.Metadata.RootDispatchId = manifest.Metadata.DispatchId

	t, db_err := d.Begin()
	if db_err != nil {
		return nil, models.NewGenericServerError(db_err)
	}
	err := crud.ImportManifest(c, t, manifest)

	// TODO: differentiate between 4xx and 5xx errors
	if err != nil {
		t.Rollback()
		return nil, models.NewGenericServerError(err)
	}
	t.Commit()
	return manifest, nil
}

func handleImportManifest(c *common.Config, d *sql.DB, w http.ResponseWriter, r *http.Request) int {
	// Deserialize and validate input
	// Apply middleware
	// Call business logic
	// Serialize response
	var reqBody models.DispatchSchema
	var respBody *models.DispatchSchema

	slog.Info(fmt.Sprint("Received POST /dispatches"))
	dec := json.NewDecoder(r.Body)

	err := (&reqBody).DecodeJSON(dec)
	if err != nil {
		models.WriteError(w, err)
		return err.StatusCode
	}

	respBody, err = importManifest(c, d, &reqBody)
	if err != nil {
		slog.Info(fmt.Sprint("Error importing manifest:", err.Error()))
		models.WriteError(w, err)
		return err.StatusCode
	}
	return writeJSONResponse(w, respBody)
}

// GET /dispatches/{dispatch_id}
func exportManifest(c *common.Config, d *sql.DB, dispatch_id string) (*models.DispatchSchema, *models.APIError) {
	t, db_err := d.Begin()
	if db_err != nil {
		return nil, models.NewGenericServerError(db_err)
	}
	manifest, err := crud.ExportManifest(c, t, dispatch_id)

	// TODO: distinguish between 4xx and 5xx errors
	if err != nil {
		t.Rollback()
		return nil, models.NewGenericServerError(err)
	}
	t.Rollback()

	return &manifest, nil
}

func handleExportManifest(c *common.Config, d *sql.DB, w http.ResponseWriter, r *http.Request) int {
	dispatch_id, err := extractPathString(r, "dispatch_id")
	if err != nil {
		models.WriteError(w, err)
		return err.StatusCode
	}

	respBody, err := exportManifest(c, d, dispatch_id)
	if err != nil {
		// TODO: improve error handling
		models.WriteError(w, err)
		return err.StatusCode
	}
	return writeJSONResponse(w, respBody)
}

// TODO: GET /dispatches?page=<page>&count=<count>
func handleGetDispatches(c *common.Config, d *sql.DB, w http.ResponseWriter, r *http.Request) int {
	pagination, _ := NewPaginationParamsFromReq(r)
	t, db_err := d.Begin()
	if db_err != nil {
		api_err := models.NewGenericServerError(db_err)
		models.WriteError(w, api_err)
		return api_err.StatusCode
	}
	dispatch_id, _ := extractQueryString(r, "dispatch_id", "")

	respBody, err := crud.GetDispatchSummaries(t, dispatch_id, pagination.Page, pagination.Count)
	t.Rollback()
	if err != nil {
		models.WriteError(w, err)
		return err.StatusCode
	}
	return writeJSONResponse(w, &respBody)
}

func deleteDispatch(c *common.Config, d *sql.DB, dispatch_id string) *models.APIError {
	t, db_err := d.Begin()
	if db_err != nil {
		return models.NewGenericServerError(db_err)
	}
	err := crud.DeleteDispatch(t, dispatch_id)
	if err != nil {
		t.Rollback()
		return models.NewGenericServerError(err)
	}
	t.Commit()
	return nil
}

// DELETE /dispatches/{dispatch_id}
func handleDeleteDispatch(c *common.Config, d *sql.DB, w http.ResponseWriter, r *http.Request) int {
	dispatch_id, err := extractPathString(r, "dispatch_id")
	if err != nil {
		models.WriteError(w, err)
		return err.StatusCode
	}
	err = deleteDispatch(c, d, dispatch_id)
	if err != nil {
		models.WriteError(w, err)
		return err.StatusCode
	}

	w.WriteHeader(http.StatusAccepted)
	return http.StatusNoContent
}
