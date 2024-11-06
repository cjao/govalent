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
		assets[i] = item.GetPublicEntity(c)
	}
	tx.Rollback()
	return assets, nil
}

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
