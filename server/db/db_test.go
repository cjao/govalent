package db

import "github.com/casey/govalent/server/common"
import _ "github.com/mattn/go-sqlite3"
import "database/sql"
import "testing"

var registered bool


func TestEmitDDL(t *testing.T) {
	c := common.Config{
		Dsn: "file:test.db",
		Port: common.DEFAULT_PORT,
	}
	drivers := sql.Drivers()
	t.Logf("Registered drivers: %v\n", drivers)
	db, err := GetDB(&c)
	if err != nil {
		t.Fatalf("Error establishing DB connection: %v", err)
	}
	err = EmitDDL(db)
	if err != nil {
		t.Fatalf("Error emitting DDL: %v", err)
	}
}
