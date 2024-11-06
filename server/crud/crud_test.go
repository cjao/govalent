package crud

import (
	"database/sql"
	"testing"
	"time"

	"github.com/casey/govalent/server/common"
	"github.com/casey/govalent/server/db"
	"github.com/casey/govalent/server/models"
	"github.com/google/uuid"
)

func newMockDB(t *testing.T) *sql.DB {
	c := common.Config{
		Dsn:  ":memory:",
		Port: common.DEFAULT_PORT,
	}
	d, err := db.GetDB(&c)
	if err != nil {
		t.Fatalf("Error establishing DB connection: %v", err)
	}
	db.EmitDDL(d)
	if err != nil {
		t.Fatalf("Error establishing DB connection: %v", err)
	}
	return d
}

func newUninitializedDB(t *testing.T) *sql.DB {
	c := common.Config{
		Dsn:  ":memory:",
		Port: common.DEFAULT_PORT,
	}
	d, err := db.GetDB(&c)
	if err != nil {
		t.Fatalf("Error establishing DB connection: %v", err)
	}
	return d
}

func newMockDispatch(electrons []models.ElectronSchema, edges []models.Edge) models.DispatchSchema {
	mock_dispatch_id := uuid.New().String()
	ts := time.Now().UTC()
	lattice := models.LatticeMeta{
		Name:         "test-workflow",
		Executor:     "local",
		ExecutorData: "{}",
	}
	dispatch := models.DispatchMeta{
		DispatchId: mock_dispatch_id,
		Status:     "NEW_OBJECT",
		StartTime:  &ts,
		CreatedAt:  ts,
	}
	graph := models.Graph{
		Nodes: electrons,
		Links: edges,
	}
	return models.DispatchSchema{Metadata: dispatch, Lattice: models.LatticeSchema{Metadata: lattice, TransportGraph: graph}}
}

func newMockElectronMeta(node_id int, status string) models.ElectronMeta {
	return models.ElectronMeta{
		TaskGroupId:  node_id,
		Status:       status,
		Name:         "mock_electron",
		Executor:     "mock_executor",
		ExecutorData: "{}",
	}
}

func newMockElectron(node_id int, metadata models.ElectronMeta, assets models.ElectronAssets) models.ElectronSchema {
	return models.ElectronSchema{
		NodeId:   node_id,
		Metadata: metadata,
		Assets:   assets,
	}
}

func newMockEdge(source int, target int, name string, param_type string, arg_index int) models.Edge {
	return models.Edge{
		Source: source,
		Target: target,
		Metadata: models.EdgeMetadata{
			Name:      name,
			ParamType: param_type,
			ArgIndex:  &arg_index,
		},
	}
}

func TestCreateDispatchMetadata(t *testing.T) {

	dispatch := newMockDispatch(nil, nil)
	d := newMockDB(t)
	tx, db_err := d.Begin()
	if db_err != nil {
		t.Fatalf("Error starting transaction: %v", db_err)
	}
	err := CreateDispatchMetadata(tx, &dispatch.Metadata, &dispatch.Lattice.Metadata)
	if err != nil {
		t.Fatalf("Error creating dispatch: %v", err)
	}

	record, err := GetDispatchMetadata(tx, dispatch.Metadata.DispatchId)
	if err != nil {
		t.Fatalf("Error retrieving dispatch: %v", err)
	}
	if record.DispatchId != dispatch.Metadata.DispatchId {
		t.Fatalf("Wrong dispatch id")
	}

	lattice_record, err := getLatticeMetadata(tx, dispatch.Metadata.DispatchId)
	if err != nil {
		t.Fatalf("Error retrieving dispatch: %v", err)
	}
	if lattice_record.Name != dispatch.Lattice.Metadata.Name {
		t.Fatalf("Wrong name")
	}
	tx.Rollback()
}

func TestBulkGetDispatches(t *testing.T) {
	dispatch_1 := newMockDispatch(nil, nil)
	dispatch_2 := newMockDispatch(nil, nil)
	d := newMockDB(t)
	tx, db_err := d.Begin()
	if db_err != nil {
		t.Fatalf("Error starting transaction: %v", db_err)
	}
	err := CreateDispatchMetadata(tx, &dispatch_1.Metadata, &dispatch_1.Lattice.Metadata)
	if err != nil {
		t.Fatalf("Error creating dispatch: %v", err)
	}
	err = CreateDispatchMetadata(tx, &dispatch_2.Metadata, &dispatch_2.Lattice.Metadata)
	if err != nil {
		t.Fatalf("Error creating dispatch: %v", err)
	}

	// Try retrieving a specific dispatch id
	dispatches, err := GetDispatches(tx, dispatch_1.Metadata.DispatchId, 0, 1)
	if err != nil {
		t.Fatalf("Error retrieving dispatches: %v", err)
	}
	if len(dispatches.Records) != 1 {
		t.Fatalf("Expected %d records, got %d records", 1, len(dispatches.Records))
	}
	expected := dispatch_1.Metadata.DispatchId
	actual := dispatches.Records[0].DispatchId
	if expected != actual {
		t.Fatalf("Wrong dispatch: expected %s, actual %s", expected, actual)
	}

	// Try retrieving both
	//
	dispatches, err = GetDispatches(tx, "", 0, 10)
	if err != nil {
		t.Fatalf("Error retrieving dispatches: %v", err)
	}
	if len(dispatches.Records) != 2 {
		t.Fatalf("Expected %d records, got %d records", 1, len(dispatches.Records))
	}
	tx.Rollback()

}

func TestUpdateDispatch(t *testing.T) {
	dispatch := newMockDispatch(nil, nil)

	d := newMockDB(t)
	tx, db_err := d.Begin()
	if db_err != nil {
		t.Fatalf("Error starting transaction: %v", db_err)
	}
	err := CreateDispatchMetadata(tx, &dispatch.Metadata, &dispatch.Lattice.Metadata)
	if err != nil {
		t.Fatalf("Error creating dispatch: %v", err)
	}

	err = UpdateDispatch(tx, dispatch.Metadata.DispatchId, "COMPLETED", "", "")
	if err != nil {
		t.Fatalf("Error updating dispatch: %v", err)
	}

	record, err := GetDispatchMetadata(tx, dispatch.Metadata.DispatchId)
	if err != nil {
		t.Fatalf("Error retrieving dispatch: %v", err)
	}
	if record.DispatchId != dispatch.Metadata.DispatchId {
		t.Fatalf("Wrong dispatch id")
	}
	if record.Status != "COMPLETED" {
		t.Fatalf("Wrong status expected %s, actual %s", "COMPLETED", "STATUS")
	}
	tx.Rollback()

}

func TestDeleteDispatch(t *testing.T) {
	dispatch := newMockDispatch(nil, nil)

	d := newMockDB(t)
	tx, db_err := d.Begin()
	if db_err != nil {
		t.Fatalf("Error starting transaction: %v", db_err)
	}
	err := CreateDispatchMetadata(tx, &dispatch.Metadata, &dispatch.Lattice.Metadata)
	if err != nil {
		tx.Rollback()
		t.Fatalf("Error creating dispatch: %v", err)
	}
	err = DeleteDispatch(tx, dispatch.Metadata.DispatchId)
	if err != nil {
		tx.Rollback()
		t.Fatal("Error deleting dispatch: ", err.Error())
	}
	tx.Rollback()
}
