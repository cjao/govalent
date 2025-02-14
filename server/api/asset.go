// Routes for asset handling

package api

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"

	"github.com/casey/govalent/server/common"
	"github.com/casey/govalent/server/crud"
	"github.com/casey/govalent/server/models"
)

func importAssets(c *common.Config, d *sql.DB, assets []models.AssetPublicSchema) ([]models.AssetPublicSchema, *models.APIError) {
	t, db_err := d.Begin()
	if db_err != nil {
		return nil, models.NewGenericServerError(db_err)
	}
	_, err := crud.CreateAssets(c, t, assets)
	if err != nil {
		t.Rollback()
		return nil, err
	}
	t.Commit()
	return assets, nil
}

func handleCreateAssets(c *common.Config, d *sql.DB, w http.ResponseWriter, r *http.Request) int {
	var reqBody models.BulkAssetPostBody
	var respBody models.BulkAssetPostResponse
	dec := json.NewDecoder(r.Body)
	err := (&reqBody).DecodeJSON(dec)
	if err != nil {
		log.Println("Error creating assets: ", err.Error())
		models.WriteError(w, err)
		return err.StatusCode
	}
	assets, err := importAssets(c, d, reqBody.Assets)
	if err != nil {
		models.WriteError(w, err)
		return err.StatusCode
	}
	respBody.Assets = assets
	return writeJSONResponse(w, &respBody)
}

func exportAssets(
	c *common.Config,
	d *sql.DB,
	prefix string,
	limit int,
	offset int,
) ([]models.AssetPublicSchema, *models.APIError) {
	tx, db_err := d.Begin()
	if db_err != nil {
		return nil, models.NewGenericServerError(db_err)
	}
	ents, api_err := crud.GetAssetEntitiesByPrefix(tx, prefix, limit, offset)
	if api_err != nil {
		tx.Rollback()
		return nil, api_err
	}
	assets := make([]models.AssetPublicSchema, len(ents))
	for i, item := range ents {
		assets[i] = *item.GetPublicEntity(c)
	}
	tx.Rollback()
	return assets, nil
}

// GET /assets?prefix=<prefix>
func handleExportAssets(c *common.Config, d *sql.DB, w http.ResponseWriter, r *http.Request) int {
	params, api_err := NewPaginationParamsFromReq(r)
	if api_err != nil {
		models.WriteError(w, api_err)
		return api_err.StatusCode
	}
	prefix, api_err := extractQueryString(r, "prefix", "")
	if api_err != nil {
		models.WriteError(w, api_err)
		return api_err.StatusCode
	}
	limit := params.Count
	offset := params.Page * limit
	assets, api_err := exportAssets(c, d, prefix, limit, offset)
	if api_err != nil {
		models.WriteError(w, api_err)
		return api_err.StatusCode
	}

	respBody := models.BulkAssetGetResponse{Assets: assets}
	return writeJSONResponse(w, &respBody)
}

func getDispatchAssetLinks(c *common.Config, d *sql.DB, dispatch_id string) ([]models.AssetLink, *models.APIError) {
	tx, db_err := d.Begin()
	if db_err != nil {
		return nil, models.NewGenericServerError(db_err)
	}
	dispatch_assets, err := crud.GetDispatchAssets(c, tx, dispatch_id)
	// TODO: handle dispatch not found
	if err != nil {
		tx.Rollback()
		return nil, models.NewGenericServerError(err)
	}
	links := make([]models.AssetLink, len(dispatch_assets))
	for i := range links {
		links[i].Asset = dispatch_assets[i].Asset.GetPublicEntity(c).AssetDetails
		links[i].Name = dispatch_assets[i].Name
	}
	tx.Rollback()
	return links, nil
}

// GET /dispatches/{dispatch_id}/assets
func handleGetDispatchAssetLinks(c *common.Config, d *sql.DB, w http.ResponseWriter, r *http.Request) int {
	dispatch_id, err := extractPathString(r, "dispatch_id")
	if err != nil {
		models.WriteError(w, err)
		return err.StatusCode
	}
	links, err := getDispatchAssetLinks(c, d, dispatch_id)
	if err != nil {
		models.WriteError(w, err)
		return err.StatusCode
	}
	respBody := models.AssetLinksResponse{Records: links}
	return writeJSONResponse(w, &respBody)
}

func getElectronAssetLinks(c *common.Config, d *sql.DB, dispatch_id string, node_id int) ([]models.AssetLink, *models.APIError) {
	tx, db_err := d.Begin()
	if db_err != nil {
		return nil, models.NewGenericServerError(db_err)
	}
	electron_assets, err := crud.GetElectronAssets(c, tx, dispatch_id, node_id)
	// TODO: handle dispatch not found
	if err != nil {
		tx.Rollback()
		return nil, models.NewGenericServerError(err)
	}
	links := make([]models.AssetLink, len(electron_assets))
	for i := range links {
		links[i].Asset = electron_assets[i].Asset.GetPublicEntity(c).AssetDetails
		links[i].Name = electron_assets[i].Name
	}
	tx.Rollback()
	return links, nil
}

// GET /dispatches/{dispatch_id}/electrons/{node_id}/assets
func handleGetElectronAssetLinks(c *common.Config, d *sql.DB, w http.ResponseWriter, r *http.Request) int {
	dispatch_id, err := extractPathString(r, "dispatch_id")
	if err != nil {
		models.WriteError(w, err)
		return err.StatusCode
	}
	node_id, err := extractPathInt(r, "node_id")
	if err != nil {
		models.WriteError(w, err)
		return err.StatusCode
	}
	links, err := getElectronAssetLinks(c, d, dispatch_id, node_id)
	if err != nil {
		models.WriteError(w, err)
		return err.StatusCode
	}
	respBody := models.AssetLinksResponse{Records: links}
	return writeJSONResponse(w, &respBody)
}
