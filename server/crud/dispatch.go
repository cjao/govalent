package crud

import (
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/casey/govalent/server/common"
	"github.com/casey/govalent/server/db"
	"github.com/casey/govalent/server/models"
)

const GetDispatchMetadataSQL = `
SELECT name, status, start_time, end_time
FROM dispatches
WHERE dispatch_id = ?
`

var DISPATCH_ENTITY_KEYS = []string{
	db.DISPATCH_TABLE_ID,
	db.DISPATCH_TABLE_ROOT_ID,
	db.DISPATCH_TABLE_NAME,
	db.DISPATCH_TABLE_STATUS,
	db.DISPATCH_TABLE_EXECUTOR,
	db.DISPATCH_TABLE_EXECUTOR_DATA,
	db.DISPATCH_TABLE_WORKFLOW_EXECUTOR,
	db.DISPATCH_TABLE_WORKFLOW_EXECUTOR_DATA,
	db.DISPATCH_TABLE_PYTHON_VERSION,
	db.DISPATCH_TABLE_COVALENT_VERSION,
	db.DISPATCH_TABLE_START_TIME,
	db.DISPATCH_TABLE_END_TIME,
	db.DISPATCH_TABLE_CREATED_AT,
	db.DISPATCH_TABLE_UPDATED_AT,
}

type DispatchEntity struct {
	d *models.DispatchMeta
	l *models.LatticeMeta
}

func (m *DispatchEntity) Fields() []string {
	return DISPATCH_ENTITY_KEYS
}

func (m *DispatchEntity) Values() []any {
	return []any{
		m.d.DispatchId,
		m.d.RootDispatchId,
		m.l.Name,
		m.d.Status,
		m.l.Executor,
		m.l.ExecutorData,
		m.l.WorkflowExecutor,
		m.l.WorkflowExecutorData,
		m.l.PythonVersion,
		m.l.CovalentVersion,
		m.d.StartTime,
		m.d.EndTime,
		m.d.CreatedAt,
		m.d.UpdatedAt,
	}
}

func (m *DispatchEntity) Fieldrefs() []any {
	return []any{
		&(m.d.DispatchId),
		&(m.d.RootDispatchId),
		&(m.l.Name),
		&(m.d.Status),
		&(m.l.Executor),
		&(m.l.ExecutorData),
		&(m.l.WorkflowExecutor),
		&(m.l.WorkflowExecutorData),
		&(m.l.PythonVersion),
		&(m.l.CovalentVersion),
		&(m.d.StartTime),
		&(m.d.EndTime),
		&(m.d.CreatedAt),
		&(m.d.UpdatedAt),
	}
}

func (m *DispatchEntity) Joins() []JoinCondition {
	return []JoinCondition{}
}

func GetDispatchEntities(t *sql.Tx, f Filters, sort_key string, ascending bool) ([]DispatchEntity, *models.APIError) {
	template := generateSelectTemplate(
		db.DISPATCH_TABLE,
		DISPATCH_ENTITY_KEYS,
		(&f).RenderTemplate(),
		sort_key,
		ascending,
		f.Limit > 0,
	)
	stmt, err := t.Prepare(template)
	if err != nil {
		slog.Error(fmt.Sprintf("Error preparing statement: %s\n", err.Error()))
		return nil, models.NewGenericServerError(err)
	}
	rows, err := stmt.Query((&f).RenderValues()...)
	if err != nil {
		slog.Error(fmt.Sprintf("Error executing query: %s\n", err.Error()))
		return nil, models.NewGenericServerError(err)
	}
	res := make([]DispatchEntity, 0)
	for rows.Next() {
		e := DispatchEntity{d: &models.DispatchMeta{}, l: &models.LatticeMeta{}}
		err := rows.Scan((&e).Fieldrefs()...)
		if err != nil {
			slog.Error(fmt.Sprintf("Error querying row: %s\n", err.Error()))
			return nil, models.NewGenericServerError(err)
		}
		res = append(res, e)
	}
	return res, nil
}

// TODO: strategy for updates

// Register assets with workflow scope
// This will write to the assets and dispatch_assets tables
func createDispatchAssets(c *common.Config, t *sql.Tx, m *models.DispatchSchema) *models.APIError {

	// Dynamic assets (result, error)

	// Insert assets
	attrs := m.Assets.AttrsByName()
	asset_schemas := make([]models.AssetPublicSchema, len(attrs))
	dispatch_links := make([]AssetLink, len(attrs))
	dispatch_id := m.Metadata.DispatchId
	count := 0
	key_count_map := make(map[string]int)

	attrs_by_key := make(map[string]*models.AssetDetails)
	for name, details := range attrs {
		// TODO: add file extensions
		key := fmt.Sprintf("%s/%s", dispatch_id, name)
		key_count_map[key] = count
		attrs_by_key[key] = attrs[name]
		asset_schemas[count].Key = key
		asset_schemas[count].AssetDetails = *details
		dispatch_links[count].dispatch_id = dispatch_id
		dispatch_links[count].Name = name
		// Assets linked to a workflow and not a particular electron
		// have node_id -1
		dispatch_links[count].node_id = -1
		count += 1
	}

	ents, api_err := CreateAssets(c, t, asset_schemas)
	if api_err != nil {
		return api_err
	}

	for _, ent := range ents {
		attrs_by_key[ent.public.Key].RemoteUri = ent.public.RemoteUri
		c := key_count_map[ent.public.Key]

		// Auto-incremented primary key
		dispatch_links[c].asset_id = ent.id
	}

	// Create links
	api_err = createAssetLinks(t, dispatch_links)
	if api_err != nil {
		return api_err
	}

	// Static assets (workflow_function, inputs, etc)

	count = 0
	attrs = m.Lattice.Assets.AttrsByName()
	asset_schemas = make([]models.AssetPublicSchema, len(attrs))
	dispatch_links = make([]AssetLink, len(attrs))
	key_count_map = make(map[string]int)

	for name, details := range attrs {
		key := fmt.Sprintf("%s/%s", dispatch_id, name)
		key_count_map[key] = count
		attrs_by_key[key] = attrs[name]
		asset_schemas[count].Key = key
		asset_schemas[count].AssetDetails = *details
		dispatch_links[count].dispatch_id = dispatch_id
		dispatch_links[count].Name = name
		dispatch_links[count].node_id = -1
		count += 1
	}
	ents, api_err = CreateAssets(c, t, asset_schemas)
	if api_err != nil {
		return api_err
	}

	for _, ent := range ents {
		attrs_by_key[ent.public.Key].RemoteUri = ent.public.RemoteUri
		c := key_count_map[ent.public.Key]

		// Auto-incremented primary key
		dispatch_links[c].asset_id = ent.id
	}

	api_err = createAssetLinks(t, dispatch_links)
	if api_err != nil {
		return api_err
	}

	return nil
}

func CreateDispatchMetadata(t *sql.Tx, d *models.DispatchMeta, l *models.LatticeMeta) *models.APIError {
	if d != nil && l != nil {
		entity := DispatchEntity{d: d, l: l}
		_, err := InsertEntities(t, "dispatches", []DBEntity{&entity})
		return err
	}
	return nil
}

func GetDispatchSummaries(t *sql.Tx, dispatch_id string, page int, count int) (models.GetBulkDispatchesResponse, *models.APIError) {
	filters := Filters{}
	// TODO: don't retrieve the whole record

	if len(dispatch_id) > 0 {
		(&filters).AddEq(db.DISPATCH_TABLE_ID, dispatch_id)
	}
	filters.Limit = count
	filters.Offset = page * count
	ents, err := GetDispatchEntities(t, filters, db.DISPATCH_TABLE_CREATED_AT, false)
	if err != nil {
		return models.GetBulkDispatchesResponse{}, err
	}
	d_meta := make([]models.DispatchMeta, len(ents))
	for i := range len(ents) {
		d_meta[i] = *ents[i].d
	}

	return models.GetBulkDispatchesResponse{Records: d_meta}, nil
}

func getDispatchEntity(t *sql.Tx, dispatch_id string) (DispatchEntity, *models.APIError) {
	filters := Filters{}
	(&filters).AddEq(db.DISPATCH_TABLE_ID, dispatch_id)
	ents, err := GetDispatchEntities(t, filters, db.DISPATCH_TABLE_CREATED_AT, false)
	if err != nil {
		return DispatchEntity{}, err
	}
	if len(ents) == 0 {
		return DispatchEntity{}, models.NewGenericClientError(fmt.Sprintf("Dispatch %s not found\n", dispatch_id))
	}
	return ents[0], nil
}

func GetDispatch(c *common.Config, t *sql.Tx, dispatch_id string, load_assets bool) (models.DispatchSchema, *models.APIError) {
	d := models.DispatchSchema{}
	ent, err := getDispatchEntity(t, dispatch_id)
	if err != nil {
		return models.DispatchSchema{}, err
	}
	asset_links, err := GetDispatchAssets(c, t, dispatch_id)
	if err != nil {
		return models.DispatchSchema{}, err
	}
	assets_details_by_name := make(map[string]*models.AssetPublicSchema)
	for _, row := range asset_links {
		assets_details_by_name[row.Name] = (&row.Asset).GetPublicEntity(c)
	}

	// Runtime generated assets
	asset_refs_by_name := (&d.Assets).AttrsByName()
	for name, details_ref := range asset_refs_by_name {
		if a, ok := assets_details_by_name[name]; ok {
			details_ref.Copy(&a.AssetDetails)
		}
	}
	// Static assets
	asset_refs_by_name = (&d.Lattice.Assets).AttrsByName()
	for name, details_ref := range asset_refs_by_name {
		if a, ok := assets_details_by_name[name]; ok {
			details_ref.Copy(&a.AssetDetails)
		}
	}

	d.Metadata = *ent.d
	d.Lattice.Metadata = *ent.l

	return d, nil
}

func UpdateDispatch(t *sql.Tx, dispatch_id string, status string, start_time string, end_time string) *models.APIError {
	where := []KeyValue{{Key: db.DISPATCH_TABLE_ID, Value: dispatch_id}}
	update := []KeyValue{{Key: db.DISPATCH_TABLE_STATUS, Value: status}}
	if len(start_time) > 0 {
		update = append(update, KeyValue{Key: "start_time", Value: start_time})
	}
	if len(end_time) > 0 {
		update = append(update, KeyValue{Key: "end_time", Value: end_time})

	}
	return UpdateTable(t, db.DISPATCH_TABLE, update, where)
}

func DeleteDispatch(t *sql.Tx, dispatch_id string) *models.APIError {
	f := Filters{}
	(&f).AddEq(db.DISPATCH_TABLE_ID, dispatch_id)
	(&f).AddEq(db.DISPATCH_TABLE_ROOT_ID, dispatch_id)
	return DeleteEntities(t, db.DISPATCH_TABLE, f)
}
