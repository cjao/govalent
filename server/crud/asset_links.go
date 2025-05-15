package crud

import (
	"database/sql"
	"fmt"
	"log/slog"
	"strings"

	"github.com/casey/govalent/server/common"
	"github.com/casey/govalent/server/db"
	"github.com/casey/govalent/server/models"
)

// Mapped to a row in the electronassets table
type AssetLink struct {
	dispatch_id string
	node_id     int
	asset_id    int64
	Name        string
}

func (l *AssetLink) Fields() []string {
	return []string{
		db.ASSET_LINKS_TABLE_DISPATCH_ID,
		db.ASSET_LINKS_TABLE_NODE_ID,
		db.ASSET_LINKS_TABLE_ASSET_ID,
		db.ASSET_LINKS_TABLE_NAME,
	}
}

func (l *AssetLink) Values() []any {
	return []any{
		l.dispatch_id,
		l.node_id,
		l.asset_id,
		l.Name,
	}
}

func (l *AssetLink) Fieldrefs() []any {
	return []any{
		&l.dispatch_id,
		&l.node_id,
		&l.asset_id,
		&l.Name,
	}
}

func (l *AssetLink) Joins() []JoinCondition {
	return []JoinCondition{}
}

type NamedAssetEntity struct {
	Name  string
	Asset AssetEntity
}

func NewNamedAssetEntity() NamedAssetEntity {
	return NamedAssetEntity{
		Asset: NewAssetEntity(),
	}
}

// Use fully qualified column names
func (e *NamedAssetEntity) Fields() []string {
	fields := []string{strings.Join([]string{db.ASSET_LINKS_TABLE, db.ASSET_LINKS_TABLE_NAME}, ".")}
	for _, item := range e.Asset.Fields() {
		fields = append(fields, strings.Join([]string{db.ASSET_TABLE, item}, "."))
	}
	return fields
}

func (e *NamedAssetEntity) Fieldrefs() []any {
	return append([]any{&e.Name}, e.Asset.Fieldrefs()...)
}

func (e *NamedAssetEntity) Values() []any {
	return append([]any{e.Name}, e.Asset.Values()...)
}

func (e *NamedAssetEntity) Joins() []JoinCondition {
	return []JoinCondition{
		{
			LeftTable:  db.ASSET_LINKS_TABLE,
			RightTable: db.ASSET_TABLE,
			LeftCol:    db.ASSET_LINKS_TABLE_ASSET_ID,
			RightCol:   db.ASSET_TABLE_ID,
		},
	}
}

func GetDispatchAssets(
	c *common.Config,
	t *sql.Tx,
	dispatch_id string,
) ([]NamedAssetEntity, *models.APIError) {

	return GetElectronAssets(c, t, dispatch_id, -1)
}

func createAssetLinks(t *sql.Tx, links []AssetLink) *models.APIError {
	if len(links) == 0 {
		return nil
	}
	template, _ := generateInsertTemplate(db.ASSET_LINKS_TABLE, (&links[0]).Fields())
	slog.Debug(fmt.Sprintf("SQL template: %s\n", template))
	stmt, err := t.Prepare(template)
	if err != nil {
		slog.Error(fmt.Sprintf("Error preparing statement: %s\n", err.Error()))
		return models.NewGenericServerError(err)
	}
	for _, item := range links {
		_, err = stmt.Exec((&item).Values()...)
		if err != nil {
			slog.Error(fmt.Sprintf("Error inserting row: %s\n", err.Error()))
			return models.NewGenericServerError(err)
		}
	}
	slog.Debug(fmt.Sprintf("Created %d electron asset links\n", len(links)))
	return nil
}

func getAssetLinks(t *sql.Tx, dispatch_id string, node_id int) ([]AssetLink, *models.APIError) {
	results := make([]AssetLink, 0)
	count := 0
	f := Filters{}
	(&f).AddEq(db.ASSET_LINKS_TABLE_DISPATCH_ID, dispatch_id)
	(&f).AddEq(db.ASSET_LINKS_TABLE_NODE_ID, node_id)

	keys := (&AssetLink{}).Fields()
	template := generateSelectJoinTemplate(
		db.ASSET_LINKS_TABLE,
		keys,
		(&AssetLink{}).Joins(),
		(&f).RenderTemplate(),
		db.ASSET_LINKS_TABLE_NAME,
		true,
		100,
		0,
	)
	slog.Debug(fmt.Sprintf("SQL template: %s\n", template))
	stmt, err := t.Prepare(template)
	if err != nil {
		slog.Info(fmt.Sprintf("Error preparing statement: %s\n", err.Error()))
		return nil, models.NewGenericServerError(err)
	}

	rows, err := stmt.Query((&f).RenderValues()...)
	if err != nil {
		slog.Error(fmt.Sprintf("Error executing query: %s\n", err.Error()))
		return nil, models.NewGenericServerError(err)
	}

	for rows.Next() {
		link := AssetLink{}
		err = rows.Scan((&link).Fieldrefs()...)
		if err != nil {
			slog.Error(fmt.Sprintf("Error querying row: %s\n", err.Error()))
			return nil, models.NewGenericServerError(err)
		}
		results = append(results, link)
		count += 1
	}
	slog.Debug(fmt.Sprintf("Returning %d electron-asset links\n", count))
	return results, nil
}

// Output: map name -> Asset Record
func GetElectronAssets(
	c *common.Config,
	t *sql.Tx,
	dispatch_id string,
	node_id int,
) ([]NamedAssetEntity, *models.APIError) {

	ents := make([]NamedAssetEntity, 0)
	count := 0
	// Generate template
	ent := NamedAssetEntity{}
	table := db.ASSET_LINKS_TABLE
	filters := Filters{}
	(&filters).AddEq(strings.Join([]string{db.ASSET_LINKS_TABLE, db.ASSET_LINKS_TABLE_DISPATCH_ID}, "."), dispatch_id)
	(&filters).AddEq(strings.Join([]string{db.ASSET_LINKS_TABLE, db.ASSET_LINKS_TABLE_NODE_ID}, "."), node_id)
	sort_key := strings.Join([]string{db.ASSET_LINKS_TABLE, db.ASSET_LINKS_TABLE_NAME}, ".")
	limit := 100
	offset := 0
	template := generateSelectJoinTemplate(
		table,
		(&ent).Fields(),
		(&ent).Joins(),
		(&filters).RenderTemplate(),
		sort_key,
		true,
		limit,
		offset,
	)

	// Prepare statement and exec query
	stmt, err := t.Prepare(template)
	if err != nil {
		slog.Info(fmt.Sprintf("Error preparing statement: %s\n", err.Error()))
		return nil, models.NewGenericServerError(err)
	}

	rows, err := stmt.Query((&filters).RenderValues()...)
	if err != nil {
		slog.Error(fmt.Sprintf("Error executing query: %s\n", err.Error()))
		return nil, models.NewGenericServerError(err)
	}
	for rows.Next() {
		ent = NewNamedAssetEntity()
		err = rows.Scan((&ent).Fieldrefs()...)
		if err != nil {
			slog.Error(fmt.Sprintf("Error querying row: %s\n", err.Error()))
			return nil, models.NewGenericServerError(err)
		}
		ents = append(ents, ent)
		count += 1
	}
	slog.Debug(fmt.Sprintf("Returning %d electron-asset links\n", count))

	return ents, nil
}
