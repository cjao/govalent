package crud

import (
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/casey/govalent/server/models"
)

// Generic CRUD functions
type KeyValue struct {
	Key   string
	Value any
}

type JoinCondition struct {
	LeftTable  string
	RightTable string
	LeftCol    string
	RightCol   string
}

type DBEntity interface {
	Fields() []string
	Values() []any
	Fieldrefs() []any
	Joins() []JoinCondition
}

func UpdateTable(t *sql.Tx, table string, update []KeyValue, where []KeyValue) *models.APIError {

	var update_cols, where_cols []string
	var values []any

	for _, item := range update {
		update_cols = append(update_cols, item.Key)
		values = append(values, item.Value)
	}

	for _, item := range where {
		where_cols = append(where_cols, item.Key)
		values = append(values, item.Value)
	}

	template, _ := generateUpdateSQLTemplate(table, update_cols, where_cols)

	// Improvement: cache this prepared statement
	stmt, err := t.Prepare(template)

	if err != nil {
		slog.Info(fmt.Sprintf("Error preparing statement: %s", err.Error()))
		return models.NewGenericServerError(err)
	}
	_, err = stmt.Exec(values...)

	if err != nil {
		slog.Info(fmt.Sprintf("Error executing update: %s", err.Error()))
		return models.NewGenericServerError(err)
	}
	return nil
}

func InsertEntitiesWithTemplate(t *sql.Tx, template string, entities []DBEntity) (int, *models.APIError) {
	stmt, err := t.Prepare(template)
	if err != nil {
		slog.Info(fmt.Sprintf("Error preparing statement: %s", err.Error()))
		return 0, models.NewGenericServerError(err)
	}
	l := len(entities)
	for i := 0; i < l; i++ {
		_, err = stmt.Exec(entities[i].Values()...)
		if err != nil {
			slog.Info(fmt.Sprintf("Error inserting row: %s", err.Error()))
			return i, models.NewGenericServerError(err)
		}
	}
	slog.Debug(fmt.Sprintf("Inserted %d rows", l))
	return l, nil
}

func InsertEntities(t *sql.Tx, table string, entities []DBEntity) (int, *models.APIError) {
	if len(entities) > 0 {
		template, _ := generateInsertTemplate(table, entities[0].Fields())
		return InsertEntitiesWithTemplate(t, template, entities)
	} else {
		return 0, nil
	}
}

func DeleteEntities(t *sql.Tx, table string, filters Filters) *models.APIError {
	template := generateDeleteTemplate(table, filters.RenderTemplate())
	stmt, err := t.Prepare(template)
	if err != nil {
		return models.NewGenericServerError(err)
	}
	_, err = stmt.Exec(filters.RenderValues()...)
	if err != nil {
		slog.Error(fmt.Sprint("Error deleting record: ", err.Error()))
		return models.NewGenericServerError(err)
	}
	return nil
}
