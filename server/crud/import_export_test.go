package crud

import (
	"testing"

	"github.com/casey/govalent/server/common"
	"github.com/casey/govalent/server/models"
)

func TestImportExport(t *testing.T) {
	dispatch := newMockDispatch(nil, nil)
	config := common.NewConfigFromEnv()
	e1 := models.ElectronSchema{NodeId: 0, Metadata: newMockElectronMeta(0, "NEW_OBJECT")}
	e2 := models.ElectronSchema{NodeId: 1, Metadata: newMockElectronMeta(1, "NEW_OBJECT")}
	e3 := models.ElectronSchema{NodeId: 2, Metadata: newMockElectronMeta(2, "NEW_OBJECT")}
	electrons := [3]models.ElectronSchema{e1, e2, e3}
	edge_1 := newMockEdge(1, 0, "x", "arg", 0)
	edge_2 := newMockEdge(2, 0, "y", "arg", 1)
	edges := [2]models.Edge{edge_1, edge_2}
	graph := models.Graph{
		Nodes: electrons[:],
		Links: edges[:],
	}
	dispatch.Lattice.TransportGraph = graph
	d := newMockDB(t)
	tx, db_err := d.Begin()
	if db_err != nil {
		t.Fatalf("Error starting transaction: %v", db_err)
	}
	err := ImportManifest(&config, tx, &dispatch)
	if err != nil {
		t.Fatalf("Error importing manifest: %v", err)
	}
	export, err := ExportManifest(tx, dispatch.Metadata.DispatchId)
	if err != nil {
		t.Fatalf("Error exporting manifest: %v", err)
	}
	if export.Metadata.DispatchId != dispatch.Metadata.DispatchId {
		t.Fatalf("Wrong dispatch id: expected %v, actual %v", dispatch.Metadata.DispatchId, export.Metadata.DispatchId)
	}
	n_nodes_old := len(dispatch.Lattice.TransportGraph.Nodes)
	n_edges_old := len(dispatch.Lattice.TransportGraph.Links)
	n_nodes_new := len(export.Lattice.TransportGraph.Nodes)
	n_edges_new := len(export.Lattice.TransportGraph.Links)
	if n_nodes_old != n_nodes_new {
		t.Fatalf("Wrong number of nodes: expected %v, actual %v", n_nodes_old, n_nodes_new)
	}
	if n_edges_old != n_edges_new {
		t.Fatalf("Wrong number of edges: expected %v, actual %v", n_edges_old, n_edges_new)
	}
	tx.Rollback()
}
