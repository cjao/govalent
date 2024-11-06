package crud

import (
	"database/sql"

	"github.com/casey/govalent/server/models"
)

func ExportManifest(t *sql.Tx, dispatch_id string) (models.DispatchSchema, *models.APIError) {
	// Export Graph
	g, err := GetGraph(t, dispatch_id, true)
	if err != nil {
		return models.DispatchSchema{}, err
	}
	l, err := getLatticeMetadata(t, dispatch_id)
	if err != nil {
		return models.DispatchSchema{}, err
	}
	d, err := GetDispatchMetadata(t, dispatch_id)
	if err != nil {
		return models.DispatchSchema{}, err
	}

	// TODO: export assets
	return models.DispatchSchema{
		Metadata: d,
		Lattice:  models.LatticeSchema{Metadata: l, TransportGraph: g},
	}, nil
}
