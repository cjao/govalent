package crud

import (
	"testing"

	"github.com/casey/govalent/server/models"
)

func TestCreateElectron(t *testing.T) {
	dispatch := newMockDispatch(nil, nil)
	electron := newMockElectronMeta(0, "NEW_OBJECT")
	d := newMockDB(t)
	tx, db_err := d.Begin()
	if db_err != nil {
		t.Fatalf("Error starting transaction: %v", db_err)
	}
	err := CreateDispatchMetadata(tx, &dispatch.Metadata, &dispatch.Lattice.Metadata)
	if err != nil {
		t.Fatalf("Error creating dispatch: %v", err)
	}
	err = CreateElectronMetadata(tx, dispatch.Metadata.DispatchId, 0, &electron)
	if err != nil {
		t.Fatalf("Error creating electron: %v", err)
	}

	electron2, err := GetElectronMetadata(tx, dispatch.Metadata.DispatchId, 0)
	if err != nil {
		t.Fatalf("Error retrieving electron: %v", err)
	}
	if electron2.TaskGroupId != electron.TaskGroupId {
		t.Fatalf("Error retrieving electron: wrong node id")
	}
	if electron2.Status != electron.Status {
		t.Fatalf("Error retrieving electron: wrong status")
	}

	err = UpdateElectronMetadata(tx, dispatch.Metadata.DispatchId, 0, models.ElectronStatusUpdate{Status: "RUNNING"})
	electron2, err = GetElectronMetadata(tx, dispatch.Metadata.DispatchId, 0)
	if electron2.Status != "RUNNING" {
		t.Fatalf("Error updating electron: expected status %s, actual status %s", "RUNNING", electron2.Status)
	}

	tx.Rollback()
}

func TestGetAllElectrons(t *testing.T) {
	d := newMockDB(t)
	tx, db_err := d.Begin()
	if db_err != nil {
		t.Fatalf("Error starting transaction: %v", db_err)
	}
	dispatch := newMockDispatch(nil, nil)
	CreateDispatchMetadata(tx, &dispatch.Metadata, &dispatch.Lattice.Metadata)
	for i := 0; i < 5; i++ {
		electron := newMockElectronMeta(i, "NEW_OBJECT")
		err := CreateElectronMetadata(tx, dispatch.Metadata.DispatchId, i, &electron)
		if err != nil {
			t.Fatalf("Error creating electron: %v", err)
		}
	}

	results, err := GetAllElectrons(tx, dispatch.Metadata.DispatchId, false)
	if err != nil {
		t.Fatalf("Error retrieving electrons from dispatch %s: %s", dispatch.Metadata.DispatchId, err.Error())
	}
	if len(results) != 5 {
		t.Fatalf("Expected %d electrons, Actual %d electrons", 5, len(results))
	}

	for i, item := range results {
		if item.NodeId != i {
			t.Fatalf("Expected NodeId %d, Actual NodeId %d", i, item.NodeId)
		}
		if item.Metadata.Status != "NEW_OBJECT" {
			t.Fatalf("Expected status %s, Actual status %s", "NEW_OBJECT", item.Metadata.Status)
		}
	}
	tx.Rollback()

}
