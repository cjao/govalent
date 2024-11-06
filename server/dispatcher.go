package main

import (
	"database/sql"

	"github.com/casey/govalent/server/common"
	"github.com/casey/govalent/server/models"
)

// Rename this package "crud"?

func gatherElectronInputs(db *sql.DB, dispatch_id string, node_id int) ([]int, map[string]int) {
	// Query parent nodes and edges
	// Assemble args and kwargs

	panic("Not Implemented")
}

func startDispatch(db *sql.DB, dispatch_id string) *models.APIError {
	// Topologically sort tasks
	// Initialize book keeping counters
	// Submit initial tasks groups
	// Must be idempotent
	return models.NewNotImplementedError()
}

func filterSublatticeElectron(
	db *sql.DB,
	c *common.Config,
	dispatch_id string,
	node_id int,
	update *models.ElectronStatusUpdate,
) (*models.ElectronStatusUpdate, error) {
	// COMPLETED + sublattice_electron + no sub_dispatch_id -> DISPATCHING
	// Take care to ensure idempotency
	panic("Not implemented")

}

func makeSublatticeDispatch(db *sql.DB, c *common.Config, dispatch_id string, node_id int, update *models.ElectronStatusUpdate) error {
	// Query manifest from node output json
	// Import manifest; assign a dispatch id one is not already assigned
	// Link the sublattice dispatch id to the electron (dispatch_id, node_id)

	if update.Status != common.STATUS_DISPATCHING {
		return nil
	}

	panic("Not implemented")
}

func updateNodeStatus(db *sql.DB, c *common.Config, dispatch_id string, node_id int, update *models.ElectronStatusUpdate) *models.APIError {
	// Filter illegal status transitions and save to DB
	// Handle electrons representing newly built sublatttices COMPLETED -> DISPATCHING
	// Call appropriate dispatcher callback

	return nil
}

func submitTaskGroup(db *sql.DB, dispatch_id string, task_group_id int) error {
	// Enumerate the tasks in topological order
	// Gather inputs
	// Send a job using the executor API
	panic("Not Implemented")
}
