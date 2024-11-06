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

type ElectronAssetLink struct {
	id          string
	electron_id int
	asset_id    string
	name        string
}

func NewElectronAsset(electron_id int, asset_key string, name string) ElectronAssetLink {
	return ElectronAssetLink{
		id:          fmt.Sprintf("%d-%s", electron_id, name),
		electron_id: electron_id,
		asset_id:    ComputeAssetId(asset_key),
		name:        name,
	}
}

func (l *ElectronAssetLink) init(electron_id int, asset_key string, name string) {
	l.id = fmt.Sprintf("%d-%s", electron_id, name)
	l.electron_id = electron_id
	l.asset_id = ComputeAssetId(asset_key)
	l.name = name
}

func (l *ElectronAssetLink) Fields() []string {
	return []string{
		db.ELECTRON_ASSET_TABLE_ID,
		db.ELECTRON_ASSET_TABLE_META_ID,
		db.ELECTRON_ASSET_TABLE_ASSET_ID,
		db.ELECTRON_ASSET_TABLE_NAME,
	}
}

func (l *ElectronAssetLink) Values() []any {
	return []any{
		l.id,
		l.electron_id,
		l.asset_id,
		l.name,
	}
}

func (l *ElectronAssetLink) Fieldrefs() []any {
	return []any{
		&l.id,
		&l.electron_id,
		&l.asset_id,
		&l.name,
	}
}

func (l *ElectronAssetLink) Joins() []JoinCondition {
	return []JoinCondition{
		JoinCondition{
			LeftTable:  db.ELECTRON_ASSET_TABLE,
			RightTable: db.ELECTRON_TABLE,
			LeftCol:    db.ELECTRON_ASSET_TABLE_META_ID,
			RightCol:   db.ELECTRON_TABLE_ID,
		},
	}
}

type DispatchAssetLink struct {
	id          string
	dispatch_id string
	asset_id    string
	name        string
}

func NewDispatchAsset(dispatch_id, asset_key, name string) DispatchAssetLink {
	return DispatchAssetLink{
		id:          fmt.Sprintf("%s-%s", dispatch_id, name),
		dispatch_id: dispatch_id,
		asset_id:    ComputeAssetId(asset_key),
		name:        name,
	}
}

func (l *DispatchAssetLink) init(dispatch_id, asset_key, name string) {
	l.id = fmt.Sprintf("%s-%s", dispatch_id, name)
	l.dispatch_id = dispatch_id
	l.asset_id = ComputeAssetId(asset_key)
	l.name = name
}

func (l *DispatchAssetLink) Fields() []string {
	return []string{
		db.DISPATCH_ASSET_TABLE_ID,
		db.DISPATCH_ASSET_TABLE_META_ID,
		db.DISPATCH_ASSET_TABLE_ASSET_ID,
		db.DISPATCH_ASSET_TABLE_NAME,
	}
}

func (l *DispatchAssetLink) Values() []any {
	return []any{
		l.id,
		l.dispatch_id,
		l.asset_id,
		l.name,
	}
}

func (l *DispatchAssetLink) Fieldrefs() []any {
	return []any{
		&l.id,
		&l.dispatch_id,
		&l.asset_id,
		&l.name,
	}
}

func CreateDispatchAssetLinks(t *sql.Tx, links []DispatchAssetLink) *models.APIError {
	if len(links) == 0 {
		return nil
	}
	template, _ := generateInsertTemplate(db.DISPATCH_ASSET_TABLE, (&links[0]).Fields())
	stmt, err := t.Prepare(template)
	if err != nil {
		slog.Info(fmt.Sprintf("Error preparing statement: %s\n", err.Error()))
		return models.NewGenericServerError(err)
	}
	for _, item := range links {
		_, err = stmt.Exec((&item).Values()...)
		if err != nil {
			slog.Info(fmt.Sprintf("Error inserting row: %s\n", err.Error()))
			return models.NewGenericServerError(err)
		}
	}
	slog.Info(fmt.Sprintf("Created %d dispatch asset links\n", len(links)))
	return nil
}

func GetDispatchAssetLinks(t *sql.Tx, dispatch_id string) ([]DispatchAssetLink, *models.APIError) {
	results := make([]DispatchAssetLink, 0)
	count := 0
	f := Filters{}
	(&f).AddEq(db.DISPATCH_ASSET_TABLE_META_ID, dispatch_id)

	template := generateSelectTemplate(
		db.DISPATCH_ASSET_TABLE,
		(&DispatchAssetLink{}).Fields(),
		(&f).RenderTemplate(),
		db.DISPATCH_ASSET_TABLE_NAME,
		true,
		false,
	)
	stmt, err := t.Prepare(template)
	if err != nil {
		slog.Info(fmt.Sprintf("Error preparing statement: %s\n", err.Error()))
		return nil, models.NewGenericServerError(err)
	}
	rows, err := stmt.Query((&f).RenderValues()...)
	if err != nil {
		slog.Info(fmt.Sprintf("Error executing query: %s\n", err.Error()))
		return nil, models.NewGenericServerError(err)
	}
	for rows.Next() {
		results = append(results, DispatchAssetLink{})
		err = rows.Scan((&results[count]).Fieldrefs()...)
		if err != nil {
			slog.Info(fmt.Sprintf("Error executing query: %s\n", err.Error()))
			return nil, models.NewGenericServerError(err)
		}
		count += 1
	}
	return results, nil
}

func CreateDispatchAsset(
	c *common.Config,
	t *sql.Tx,
	dispatch_id string,
	name string,
	asset *models.AssetDetails,
) *models.APIError {
	// Create an asset with a given name
	//
	// TODO: look this up
	if asset == nil {
		return nil
	}
	key := fmt.Sprintf("%s/%s", dispatch_id, name)
	reqBody := []models.AssetPublicSchema{
		{Key: key, AssetDetails: *asset},
	}
	_, api_err := CreateAssets(c, t, reqBody)
	if api_err != nil {
		return api_err
	}

	link := NewDispatchAsset(dispatch_id, key, name)
	api_err = CreateDispatchAssetLinks(t, []DispatchAssetLink{link})
	if api_err != nil {
		return api_err
	}

	return nil
}

func GetDispatchAsset(
	c *common.Config,
	t *sql.Tx,
	dispatch_id string,
	name string,
) (AssetEntity, *models.APIError) {
	ent := NewAssetEntity()
	asset_link_join := JoinCondition{
		LeftTable:  db.ASSET_TABLE,
		LeftCol:    db.ASSET_TABLE_ID,
		RightTable: db.DISPATCH_ASSET_TABLE,
		RightCol:   db.DISPATCH_ASSET_TABLE_ASSET_ID,
	}
	link_dispatch_join := JoinCondition{
		LeftTable:  db.DISPATCH_ASSET_TABLE,
		LeftCol:    db.DISPATCH_ASSET_TABLE_META_ID,
		RightTable: db.DISPATCH_TABLE,
		RightCol:   db.DISPATCH_TABLE_ID,
	}
	sort_key := db.ASSET_TABLE_KEY
	ascending := true
	limit := 1
	offset := 0
	f := Filters{}
	(&f).AddEq(strings.Join([]string{db.DISPATCH_ASSET_TABLE, db.DISPATCH_ASSET_TABLE_NAME}, "."), name)
	(&f).AddEq(strings.Join([]string{db.DISPATCH_TABLE, db.DISPATCH_TABLE_ID}, "."), dispatch_id)

	fields := (&ent).Fields()
	for i := 0; i < len(fields); i++ {
		fields[i] = strings.Join([]string{db.ASSET_TABLE, fields[i]}, ".")
	}

	template := generateSelectJoinTemplate(
		db.ASSET_TABLE,
		fields,
		[]JoinCondition{asset_link_join, link_dispatch_join},
		(&f).RenderTemplate(),
		sort_key,
		ascending,
		limit,
		offset,
	)
	slog.Debug(fmt.Sprintf("SQL template: %s", template))

	stmt, db_err := t.Prepare(template)
	if db_err != nil {
		slog.Error(fmt.Sprintf("Error preparing statement: %s\n", db_err.Error()))
		return ent, models.NewGenericServerError(db_err)
	}

	row := stmt.QueryRow((&f).RenderValues()...)

	db_err = row.Scan((&ent).Fieldrefs()...)
	if db_err == sql.ErrNoRows {
		slog.Info(fmt.Sprintf("ERROR: Asset with name %s not found for dispatch %s\n", name, dispatch_id))
		return ent, models.NewNotFoundError(db_err)
	}
	if db_err != nil {
		slog.Error(fmt.Sprintf("Error querying row: %s\n", db_err.Error()))
		return ent, models.NewGenericServerError(db_err)
	}

	return ent, nil
}

func CreateElectronAssetLinks(t *sql.Tx, links []ElectronAssetLink) *models.APIError {
	if len(links) == 0 {
		return nil
	}
	template, _ := generateInsertTemplate(db.ELECTRON_ASSET_TABLE, (&links[0]).Fields())
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

func getElectronAssetLinks(t *sql.Tx, dispatch_id string, node_id int) ([]ElectronAssetLink, *models.APIError) {
	results := make([]ElectronAssetLink, 0)
	count := 0
	f := Filters{}
	(&f).AddEq(fmt.Sprintf("%s.%s", db.ELECTRON_TABLE, db.ELECTRON_TABLE_DISPATCH_ID), dispatch_id)
	(&f).AddEq(fmt.Sprintf("%s.%s", db.ELECTRON_TABLE, db.ELECTRON_TABLE_NODE_ID), node_id)

	keys := (&ElectronAssetLink{}).Fields()
	for i := 0; i < len(keys); i++ {
		keys[i] = strings.Join([]string{db.ELECTRON_ASSET_TABLE, keys[i]}, ".")
	}
	template := generateSelectJoinTemplate(
		db.ELECTRON_ASSET_TABLE,
		keys,
		(&ElectronAssetLink{}).Joins(),
		(&f).RenderTemplate(),
		strings.Join([]string{db.ELECTRON_ASSET_TABLE, db.ELECTRON_ASSET_TABLE_NAME}, "."),
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
		link := ElectronAssetLink{}
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

func CreateElectronAsset(
	c *common.Config,
	t *sql.Tx,
	dispatch_id string,
	node_id int,
	name string,
	asset *models.AssetDetails,
) *models.APIError {
	return models.NewNotImplementedError()
}

func GetElectronAsset(
	c *common.Config,
	t *sql.Tx,
	dispatch_id string,
	node_id int,
	name string,
) (models.AssetDetails, *models.APIError) {
	return models.AssetDetails{}, models.NewNotImplementedError()
}
