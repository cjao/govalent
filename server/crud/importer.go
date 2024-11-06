package crud

import (
	"database/sql"

	"github.com/casey/govalent/server/common"
	"github.com/casey/govalent/server/models"
)

func ImportManifest(c *common.Config, t *sql.Tx, m *models.DispatchSchema) *models.APIError {
	// TODO: create assets
	err := CreateDispatchMetadata(t, &m.Metadata, &m.Lattice.Metadata)
	if err != nil {
		return err
	}
	err = createDispatchAssets(c, t, m)
	if err != nil {
		return err
	}
	return CreateGraph(c, t, m.Metadata.DispatchId, &m.Lattice.TransportGraph)
}
