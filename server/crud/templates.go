package crud

import (
	"fmt"
	"strings"
)

// SQL templates

type Filters struct {
	Eq     []KeyValue
	Like   []KeyValue
	Limit  int
	Offset int
}

func (f *Filters) AddLike(key string, val string) {
	f.Like = append(f.Like, KeyValue{Key: key, Value: val})
}

func (f *Filters) AddEq(key string, val any) {
	f.Eq = append(f.Eq, KeyValue{Key: key, Value: val})
}

// The values must be generated in the same order as the parameters in the template
func (f *Filters) RenderValues() []any {
	vals := make([]any, len(f.Eq)+len(f.Like))
	for i, item := range f.Eq {
		vals[i] = item.Value
	}
	n := len(f.Eq)
	for i, item := range f.Like {
		vals[n+i] = item.Value
	}
	if f.Limit > 0 {
		vals = append(vals, f.Limit)
		vals = append(vals, f.Offset)
	}
	return vals
}

func (f *Filters) RenderTemplate() string {
	var where_clause_builder strings.Builder
	if len(f.Eq) > 0 || len(f.Like) > 0 {
		where_clause_builder.WriteString("WHERE ")
	}
	if len(f.Eq) > 0 {
		for i := 0; i+1 < len(f.Eq); i++ {
			where_clause_builder.WriteString(fmt.Sprintf("%s = ? AND ", f.Eq[i].Key))
		}
		where_clause_builder.WriteString(fmt.Sprintf("%s = ?", f.Eq[len(f.Eq)-1].Key))
		if len(f.Like) > 0 {
			where_clause_builder.WriteString(" AND ")
		}
	}
	if len(f.Like) > 0 {
		for i := 0; i+1 < len(f.Like); i++ {
			where_clause_builder.WriteString(fmt.Sprintf("%s LIKE ? AND ", f.Like[i].Key))
		}
		where_clause_builder.WriteString(fmt.Sprintf("%s LIKE ?", f.Like[len(f.Like)-1].Key))
	}
	return where_clause_builder.String()
}

func generateUpdateSQLTemplate(table string, update_cols []string, where_cols []string) (string, error) {
	var set_clause_builder strings.Builder

	for i := 0; i+1 < len(update_cols); i++ {
		set_clause_builder.WriteString(fmt.Sprintf("%s = ?, ", update_cols[i]))
	}
	set_clause_builder.WriteString(fmt.Sprintf("%s = ?", update_cols[len(update_cols)-1]))

	set_clause := set_clause_builder.String()
	where_clause := generateWhereString(where_cols)
	return fmt.Sprintf("UPDATE %s SET %s %s", table, set_clause, where_clause), nil
}

func generateInsertTemplate(table string, columns []string) (string, error) {
	var cols_clause strings.Builder
	var vals_clause strings.Builder
	cols_clause.WriteString("(")
	vals_clause.WriteString("(")
	for i := 0; i+1 < len(columns); i++ {
		cols_clause.WriteString(fmt.Sprintf("%s, ", columns[i]))
		vals_clause.WriteString("?, ")
	}
	cols_clause.WriteString(fmt.Sprintf("%s)", columns[len(columns)-1]))
	vals_clause.WriteString("?)")

	return fmt.Sprintf("INSERT OR IGNORE INTO %s %s VALUES %s", table, cols_clause.String(), vals_clause.String()), nil
}

func generateColumnString(columns []string) string {
	var builder strings.Builder
	for i := 0; i+1 < len(columns); i++ {
		builder.WriteString(fmt.Sprintf("%s, ", columns[i]))
	}
	builder.WriteString(columns[len(columns)-1])
	return builder.String()
}

func generateWhereString(where_cols []string) string {
	var where_clause_builder strings.Builder
	if len(where_cols) > 0 {
		where_clause_builder.WriteString("WHERE ")
		for i := 0; i+1 < len(where_cols); i++ {
			where_clause_builder.WriteString(fmt.Sprintf("%s = ? AND ", where_cols[i]))
		}
		where_clause_builder.WriteString(fmt.Sprintf("%s = ?", where_cols[len(where_cols)-1]))
	}
	return where_clause_builder.String()
}

// Basic get operation on a single table
// Columns to retrieve
// Filters
// Sort
func generateSelectTemplate(table string, columns []string, where_clause string, sort_key string, ascending bool, with_limits bool) string {
	sort_order := "ASC"
	if !ascending {
		sort_order = "DESC"
	}
	template := fmt.Sprintf("SELECT %s FROM %s %s ORDER BY %s %s", generateColumnString(columns), table, where_clause, sort_key, sort_order)
	if with_limits {
		template = strings.Join([]string{template, "LIMIT ? OFFSET ?"}, " ")
	}
	return template
}

func generateJoinString(joins []JoinCondition) string {
	var jb strings.Builder
	for _, item := range joins {
		jb.WriteString(
			fmt.Sprintf(
				"JOIN %s ON %s.%s = %s.%s ",
				item.RightTable,
				item.LeftTable,
				item.LeftCol,
				item.RightTable,
				item.RightCol,
			),
		)
	}
	return jb.String()
}
func generateSelectJoinTemplate(
	table string,
	attributes []string,
	joins []JoinCondition,
	filter_template string,
	sort_key string,
	ascending bool,
	limit int,
	offset int,
) string {

	sort_order := "ASC"
	if !ascending {
		sort_order = "DESC"
	}
	return fmt.Sprintf(
		"SELECT %s FROM %s %s %s ORDER BY %s %s LIMIT %d OFFSET %d",
		generateColumnString(attributes),
		table,
		generateJoinString(joins),
		filter_template,
		sort_key,
		sort_order,
		limit,
		offset,
	)
}

func generateDeleteTemplate(table string, filter_template string) string {
	return fmt.Sprintf("DELETE FROM %s %s", table, filter_template)
}
