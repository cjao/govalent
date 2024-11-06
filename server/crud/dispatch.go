package crud

import (
	"database/sql"
	"fmt"

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

// TODO: strategy for updates

// Register assets with workflow scope
// This will write to the assets and dispatch_assets tables
func createDispatchAssets(c *common.Config, t *sql.Tx, m *models.DispatchSchema) *models.APIError {

	// Dynamic assets (result, error)

	// Insert assets
	attrs := m.Assets.AttrsByName()
	asset_schemas := make([]models.AssetPublicSchema, len(attrs))
	dispatch_links := make([]DispatchAssetLink, len(attrs))
	dispatch_id := m.Metadata.DispatchId
	count := 0

	attrs_by_key := make(map[string]*models.AssetDetails)
	for name, details := range attrs {
		// TODO: add file extensions
		key := fmt.Sprintf("%s/%s", dispatch_id, name)
		attrs_by_key[key] = attrs[name]
		asset_schemas[count].Key = key
		asset_schemas[count].AssetDetails = *details
		dispatch_links[count].init(dispatch_id, asset_schemas[count].Key, name)
		count += 1
	}

	ents, api_err := CreateAssets(c, t, asset_schemas)
	if api_err != nil {
		return api_err
	}

	for _, ent := range ents {
		attrs_by_key[ent.public.Key].RemoteUri = ent.public.RemoteUri
	}

	// Create links
	api_err = CreateDispatchAssetLinks(t, dispatch_links)
	if api_err != nil {
		return api_err
	}

	// Static assets (workflow_function, inputs, etc)

	count = 0
	attrs = m.Lattice.Assets.AttrsByName()
	asset_schemas = make([]models.AssetPublicSchema, len(attrs))
	dispatch_links = make([]DispatchAssetLink, len(attrs))

	for name, details := range attrs {
		key := fmt.Sprintf("%s/%s", dispatch_id, name)
		attrs_by_key[key] = attrs[name]
		asset_schemas[count].Key = key
		asset_schemas[count].AssetDetails = *details
		dispatch_links[count].init(dispatch_id, asset_schemas[count].Key, name)
		count += 1
	}
	ents, api_err = CreateAssets(c, t, asset_schemas)
	if api_err != nil {
		return api_err
	}

	for _, ent := range ents {
		attrs_by_key[ent.public.Key].RemoteUri = ent.public.RemoteUri
	}

	api_err = CreateDispatchAssetLinks(t, dispatch_links)
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

func GetDispatches(t *sql.Tx, dispatch_id string, page int, count int) (models.GetBulkDispatchesResponse, *models.APIError) {
	filters := Filters{}
	d_meta := make([]models.DispatchMeta, count)
	l_meta := make([]models.LatticeMeta, count)
	// TODO: don't retrieve the whole record
	ents := make([]DBEntity, count)
	for i := range ents {
		ents[i] = &DispatchEntity{d: &d_meta[i], l: &l_meta[i]}
	}

	if len(dispatch_id) > 0 {
		(&filters).AddEq(db.DISPATCH_TABLE_ID, dispatch_id)
	}
	n, err := GetEntities(
		t,
		db.DISPATCH_TABLE,
		ents,
		filters,
		count,
		page*count,
		db.DISPATCH_TABLE_CREATED_AT,
		false,
	)
	if err != nil {
		return models.GetBulkDispatchesResponse{}, models.NewGenericServerError(err)
	}

	return models.GetBulkDispatchesResponse{Records: d_meta[:n]}, nil
}

func getDispatchEntity(t *sql.Tx, dispatch_id string) (DispatchEntity, *models.APIError) {
	filters := Filters{}
	(&filters).AddEq(db.DISPATCH_TABLE_ID, dispatch_id)
	ents := make([]DBEntity, 1)
	d := models.DispatchMeta{}
	l := models.LatticeMeta{}
	ent := DispatchEntity{&d, &l}
	ents[0] = &ent
	n, err := GetEntities(t, db.DISPATCH_TABLE, ents, filters, 1, 0, db.DISPATCH_TABLE_ID, true)
	if err != nil {
		return DispatchEntity{}, err
	}
	if n == 0 {
		return DispatchEntity{}, models.NewGenericClientError(fmt.Sprintf("Dispatch %s not found\n", dispatch_id))
	}
	return ent, nil
}

func GetDispatchMetadata(t *sql.Tx, dispatch_id string) (models.DispatchMeta, *models.APIError) {
	ent, err := getDispatchEntity(t, dispatch_id)
	if err != nil {
		return models.DispatchMeta{}, err
	}
	return *ent.d, err
}

func getLatticeMetadata(t *sql.Tx, dispatch_id string) (models.LatticeMeta, *models.APIError) {
	ent, err := getDispatchEntity(t, dispatch_id)
	if err != nil {
		return models.LatticeMeta{}, err
	}
	return *ent.l, nil

}

func UpdateDispatch(t *sql.Tx, dispatch_id string, status string, start_time string, end_time string) *models.APIError {
	where := []KeyValue{{Key: "dispatch_id", Value: dispatch_id}}
	update := []KeyValue{{Key: "status", Value: status}}
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
