package crud

import (
	"testing"

	"github.com/casey/govalent/server/common"
	"github.com/casey/govalent/server/models"
)

func TestCreateGraph(t *testing.T) {
	config := common.NewConfigFromEnv()
	dispatch := newMockDispatch(nil, nil)
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

	db := newMockDB(t)
	tx, db_err := db.Begin()
	if db_err != nil {
		t.Fatalf("Error starting transaction: %v", db_err)
	}
	err := CreateDispatchMetadata(tx, &dispatch.Metadata, &dispatch.Lattice.Metadata)
	if err != nil {
		t.Fatalf("Error creating dispatch record: %v", err)
	}

	err = CreateGraph(&config, tx, dispatch.Metadata.DispatchId, &graph)
	if err != nil {
		t.Fatalf("Error creating dispatch record: %v", err)
	}

	electrons_new, err := GetAllElectrons(&config, tx, dispatch.Metadata.DispatchId, false)
	if len(electrons_new) != 3 {
		t.Fatalf("Expected %d electrons, actual %d electrons", 3, len(electrons_new))
	}

	edges_new, err := GetAllEdges(tx, dispatch.Metadata.DispatchId)
	if len(edges_new) != 2 {
		t.Fatalf("Expected %d edges, actual %d edges", 2, len(edges_new))
	}
	if edges_new[0].Source != 1 {
		t.Fatalf("Wrong edge source: expected %d, actual %d", 1, edges_new[0].Source)
	}

	if edges_new[1].Source != 2 {
		t.Fatalf("Wrong edge source: expected %d, actual %d", 2, edges_new[1].Source)
	}

	tx.Rollback()
}

func TestTopologicalSort(t *testing.T) {
	nodes := []models.ElectronSchema{
		{NodeId: 0},
		{NodeId: 1},
		{NodeId: 2},
		{NodeId: 3},
	}
	edges := []models.Edge{
		{
			Source: 1,
			Target: 0,
		},
		{
			Source: 2,
			Target: 0,
		},
		{
			Source: 3,
			Target: 2,
		},
	}
	g := models.Graph{
		Nodes: nodes,
		Links: edges,
	}
	gv := NewGraphView(&g)
	_, err := gv.sortTopologically()
	if err != nil {
		t.Fatalf("Error in topological sort: %v", err)
	}
}
