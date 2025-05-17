package crud

import (
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/casey/govalent/server/common"
	"github.com/casey/govalent/server/db"
	"github.com/casey/govalent/server/models"
)

var ELECTRON_ENTITY_KEYS = []string{
	db.ELECTRON_TABLE_DISPATCH_ID,
	db.ELECTRON_TABLE_SUBDISPATCH_ID,
	db.ELECTRON_TABLE_NODE_ID,
	db.ELECTRON_TABLE_GID,
	db.ELECTRON_TABLE_NAME,
	db.ELECTRON_TABLE_STATUS,
	db.ELECTRON_TABLE_EXECUTOR,
	db.ELECTRON_TABLE_EXECUTOR_DATA,
	db.ELECTRON_TABLE_START_TIME,
	db.ELECTRON_TABLE_END_TIME,
}

type ElectronEntity struct {
	parent_dispatch_id string
	node_id            int
	meta               *models.ElectronMeta
}

func newElectronEntity(parent_dispatch_id string, node_id int, metadata *models.ElectronMeta) ElectronEntity {
	return ElectronEntity{
		parent_dispatch_id: parent_dispatch_id,
		node_id:            node_id,
		meta:               metadata,
	}
}

func (e *ElectronEntity) Fields() []string {
	return ELECTRON_ENTITY_KEYS
}

func (e *ElectronEntity) Values() []any {
	return []any{
		e.parent_dispatch_id,
		e.meta.SubdispatchId,
		e.node_id,
		e.meta.TaskGroupId,
		e.meta.Name,
		e.meta.Status,
		e.meta.Executor,
		e.meta.ExecutorData,
		e.meta.StartTime,
		e.meta.EndTime,
	}
}

func (e *ElectronEntity) Fieldrefs() []any {
	return []any{
		&(e.parent_dispatch_id),
		&(e.meta.SubdispatchId),
		&(e.node_id),
		&(e.meta.TaskGroupId),
		&(e.meta.Name),
		&(e.meta.Status),
		&(e.meta.Executor),
		&(e.meta.ExecutorData),
		&(e.meta.StartTime),
		&(e.meta.EndTime),
	}
}

func (e *ElectronEntity) Joins() []JoinCondition {
	return []JoinCondition{}
}

func GetElectronEntities(t *sql.Tx, f Filters, sort_key string, ascending bool) ([]ElectronEntity, *models.APIError) {
	template := generateSelectTemplate(
		db.ELECTRON_TABLE,
		ELECTRON_ENTITY_KEYS,
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
	res := make([]ElectronEntity, 0)
	for rows.Next() {
		e := ElectronEntity{meta: &models.ElectronMeta{}}
		err := rows.Scan((&e).Fieldrefs()...)
		if err != nil {
			slog.Error(fmt.Sprintf("Error querying row: %s\n", err.Error()))
			return nil, models.NewGenericServerError(err)
		}
		res = append(res, e)
	}
	return res, nil
}

type NodeIdEntity struct {
	dispatch_id string
	node_id     int
}

func (n *NodeIdEntity) Fields() []string {
	return []string{
		db.ELECTRON_TABLE_DISPATCH_ID,
		db.ELECTRON_TABLE_NODE_ID,
	}
}

func (n *NodeIdEntity) Values() []any {
	return []any{
		n.dispatch_id,
		n.node_id,
	}
}

func (n *NodeIdEntity) Fieldrefs() []any {
	return []any{
		&n.dispatch_id,
		&n.node_id,
	}
}

func (n *NodeIdEntity) Joins() []JoinCondition {
	return []JoinCondition{}
}

func CreateElectronMetadata(t *sql.Tx, dispatch_id string, node_id int, metadata *models.ElectronMeta) *models.APIError {
	if metadata != nil {
		ent := newElectronEntity(dispatch_id, node_id, metadata)
		_, err := InsertEntities(t, db.ELECTRON_TABLE, []DBEntity{&ent})
		return err
	}
	return nil
}

func createElectronAssets(
	c *common.Config,
	t *sql.Tx,
	dispatch_id string,
	e *models.ElectronSchema,
) *models.APIError {
	attrs := e.Assets.AttrsByName()
	asset_schemas := make([]models.AssetPublicSchema, len(attrs))
	asset_links := make([]AssetLink, len(attrs))
	count := 0
	key_count_map := make(map[string]int)
	attrs_by_key := make(map[string]*models.AssetDetails)
	for name, details := range attrs {
		key := fmt.Sprintf("%s/node_%d/%s", dispatch_id, e.NodeId, name)
		key_count_map[key] = count
		attrs_by_key[key] = attrs[name]
		asset_schemas[count].Key = key
		asset_schemas[count].AssetDetails = *details
		asset_links[count].dispatch_id = dispatch_id
		asset_links[count].node_id = e.NodeId
		asset_links[count].Name = name
		count += 1
	}

	// asset_schemas are inputs to the asset creation endpoint
	// ents have remote_uri populated
	ents, api_err := CreateAssets(c, t, asset_schemas)
	if api_err != nil {
		return api_err
	}

	for _, ent := range ents {
		attrs_by_key[ent.public.Key].RemoteUri = ent.public.RemoteUri
		c := key_count_map[ent.public.Key]

		// Auto-incremented primary key
		asset_links[c].asset_id = ent.id
	}

	api_err = createAssetLinks(t, asset_links)
	if api_err != nil {
		return api_err
	}
	return nil
}

func GetElectronMetadata(t *sql.Tx, dispatch_id string, node_id int) (models.ElectronMeta, *models.APIError) {
	f := Filters{}
	(&f).AddEq(db.ELECTRON_TABLE_DISPATCH_ID, dispatch_id)
	(&f).AddEq(db.ELECTRON_TABLE_NODE_ID, node_id)
	ents, err := GetElectronEntities(t, f, db.ELECTRON_TABLE_NODE_ID, true)
	if err != nil {
		return models.ElectronMeta{}, err
	}
	if len(ents) == 0 {
		return models.ElectronMeta{}, models.NewNotFoundError(db.ERR_NOT_FOUND)
	}
	return *ents[0].meta, nil
}

func getAllElectronMeta(t *sql.Tx, dispatch_id string) ([]models.ElectronMeta, *models.APIError) {

	filters := Filters{}
	(&filters).AddEq(db.ELECTRON_TABLE_DISPATCH_ID, dispatch_id)
	ents, err := GetElectronEntities(t, filters, db.ELECTRON_TABLE_NODE_ID, true)
	if err != nil {
		return nil, err
	}
	results := make([]models.ElectronMeta, len(ents))
	for i := range results {
		results[i] = *ents[i].meta
	}

	return results, nil
}

func GetAllElectrons(c *common.Config, t *sql.Tx, dispatch_id string, load_assets bool) ([]models.ElectronSchema, *models.APIError) {
	// TODO: load assets
	//
	// These will be sorted by node_id
	meta_list, err := getAllElectronMeta(t, dispatch_id)
	if err != nil {
		return []models.ElectronSchema{}, err
	}
	electrons := make([]models.ElectronSchema, len(meta_list))
	for i, item := range meta_list {
		electrons[i].NodeId = i
		electrons[i].Metadata = item

		// TODO: condition this on load_assets
		asset_entities, err := GetElectronAssets(c, t, dispatch_id, i)
		if err != nil {
			return nil, err
		}
		asset_refs_by_name := (&electrons[i].Assets).AttrsByName()
		asset_details_by_name := make(map[string]*models.AssetPublicSchema)
		// convert [{name, AssetEntity}] into map {name -> AssetEntity}
		for _, row := range asset_entities {
			asset_details_by_name[row.Name] = (&row.Asset).GetPublicEntity(c)
		}

		// Populate ElectronAssets
		for name, details_ref := range asset_refs_by_name {
			a, ok := asset_details_by_name[name]
			if ok {
				details_ref.Copy(&a.AssetDetails)
			}
		}
	}
	// retrieve assets
	return electrons, nil
}

func CanUpdateElectronStatus(db *sql.DB, dispatch_id string, node_id int, update *models.ElectronStatusUpdate) bool {
	// Determine whether the state transition is legal
	// NEW_OBJ -> STARTING -> SUBMITTED -> RUNNING|FAILED|CANCELLED
	// NEW_OBJ -> PENDING_REUSE -> CANCELLED|COMPLETED
	// NEW_OBJ -> RUNNING -> FAILED|CANCELLED|COMPLETED
	// Use an exclusive row lock to ensure atomic RMW
	panic("Not implemented")
}

func UpdateElectronMetadata(t *sql.Tx, dispatch_id string, node_id int, update models.ElectronStatusUpdate) *models.APIError {
	// Persist an electron status update to the database
	// Filter illegal updates
	// Increment resolved_electrons counter
	// Return list of task groups ready to be submitted
	//
	// If no additional tasks can be submitted and resolved_electrons = submitted_electrons,
	// finalize the dispatch
	updates := []KeyValue{
		{Key: db.ELECTRON_TABLE_STATUS, Value: update.Status},
	}
	where := []KeyValue{
		{Key: db.ELECTRON_TABLE_DISPATCH_ID, Value: dispatch_id},
		{Key: db.ELECTRON_TABLE_NODE_ID, Value: node_id},
	}
	if update.StartTime != nil {
		updates = append(updates, KeyValue{Key: db.ELECTRON_TABLE_START_TIME, Value: update.StartTime})
	}
	if update.EndTime != nil {
		updates = append(updates, KeyValue{Key: db.ELECTRON_TABLE_END_TIME, Value: update.EndTime})
	}
	return UpdateTable(t, db.ELECTRON_TABLE, updates, where)
}
