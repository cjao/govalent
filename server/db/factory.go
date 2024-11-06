package db

import (
	"database/sql"
	"fmt"
	"log"
	"log/slog"

	"github.com/casey/govalent/server/common"

	_ "github.com/mattn/go-sqlite3"
)

const dispatchesDDL = `
CREATE TABLE IF NOT EXISTS dispatches (
    dispatch_id TEXT PRIMARY KEY,
    root_dispatch_id TEXT,
    name TEXT NOT NULL,
    status TEXT NOT NULL,
    executor TEXT,
    executor_data TEXT,
    workflow_executor TEXT,
    workflow_executor_data TEXT,
    python_version TEXT,
    covalent_version TEXT,
    start_time DATETIME,
    end_time DATETIME,
    created_at DATETIME,
    updated_at DATETIME
)
`

const electronsDDL = `
CREATE TABLE IF NOT EXISTS electrons (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    parent_dispatch_id TEXT NOT NULL REFERENCES dispatches(id) ON DELETE CASCADE,
    sub_dispatch_id TEXT REFERENCES dispatches(id) ON DELETE CASCADE,
    transport_graph_node_id INTEGER NOT NULL,
    task_group_id INTEGER NOT NULL,
    name TEXT,
    status TEXT,
    executor TEXT,
    executor_data TEXT,
    created_at DATETIME,
    start_time DATETIME,
    updated_at DATETIME,
    end_time DATETIME,
    job_id TEXT,
    sort_order INTEGER
)
`

const edgesDDL = `
CREATE TABLE IF NOT EXISTS edges (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    child_electron_id INTEGER NOT NULL REFERENCES electrons(id) ON DELETE CASCADE,
    parent_electron_id INTEGER NOT NULL REFERENCES electrons(id) ON DELETE CASCADE,
    edge_name TEXT NOT NULL,
    param_type TEXT NOT NULL,
    arg_index INTEGER
)
`

const assetsDDL = `
CREATE TABLE IF NOT EXISTS assets (
	id TEXT PRIMARY KEY,
	scheme TEXT NOT NULL,
	base_path TEXT NOT NULL,
	key TEXT UNIQUE,
	size INTEGER NOT NULL,
	digest_alg TEXT,
	digest TEXT,
	remote_uri TEXT
)
`

const electronAssetsDDL = `
CREATE TABLE IF NOT EXISTS electronassets (
	id TEXT PRIMARY KEY,
	meta_id INTEGER REFERENCES electrons(id) ON DELETE CASCADE NOT NULL,
	asset_id TEXT REFERENCES assets(id) ON DELETE CASCADE NOT NULL,
	name TEXT NOT NULL
)
`

const dispatchAssetsDDL = `
CREATE TABLE IF NOT EXISTS dispatchassets (
	id TEXT PRIMARY KEY,
	meta_id TEXT REFERENCES dispatches(id) ON DELETE CASCADE NOT NULL,
	asset_id TEXT REFERENCES assets(id) ON DELETE CASCADE NOT NULL,
	name TEXT NOT NULL
)
`

func GetDB(c *common.Config) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", c.Dsn)
	if err != nil {
		log.Println("Error opening db: ", err.Error())
		return nil, err
	}
	return db, nil
}

func EmitDDL(db *sql.DB) error {
	_, err := db.Exec(dispatchesDDL)
	if err != nil {
		slog.Error(fmt.Sprintf("Error emitting DDL: %s", err.Error()))
		return err
	}
	_, err = db.Exec(electronsDDL)
	if err != nil {
		slog.Error(fmt.Sprintf("Error emitting DDL: %s", err.Error()))
		return err
	}
	_, err = db.Exec(edgesDDL)
	if err != nil {
		slog.Error(fmt.Sprintf("Error emitting DDL: %s", err.Error()))
		return err
	}
	_, err = db.Exec(assetsDDL)
	if err != nil {
		slog.Error(fmt.Sprintf("Error emitting DDL: %s", err.Error()))
		return err
	}
	_, err = db.Exec(assetsDDL)
	if err != nil {
		slog.Error(fmt.Sprintf("Error emitting DDL: %s", err.Error()))
		return err
	}
	_, err = db.Exec(electronAssetsDDL)
	if err != nil {
		log.Println("Error emitting: ", err.Error())
		return err
	}
	_, err = db.Exec(dispatchAssetsDDL)
	if err != nil {
		log.Println("Error emitting: ", err.Error())
		return err
	}
	return nil
}
