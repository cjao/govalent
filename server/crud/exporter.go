package crud

import (
	"database/sql"

	"github.com/casey/govalent/server/common"
	"github.com/casey/govalent/server/models"
)

func ExportManifest(c *common.Config, t *sql.Tx, dispatch_id string) (models.DispatchSchema, *models.APIError) {
	// Export Graph
	g, err := GetGraph(c, t, dispatch_id, true)
	if err != nil {
		return models.DispatchSchema{}, err
	}

	d, err := GetDispatch(c, t, dispatch_id, true)
	if err != nil {
		return models.DispatchSchema{}, err
	}
	d.Lattice.TransportGraph = g
	return d, nil
}
