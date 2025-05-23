package crud

import (
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/casey/govalent/server/common"
	"github.com/casey/govalent/server/db"
	"github.com/casey/govalent/server/models"
)

var EDGE_ENTITY_KEYS = []string{
	db.EDGES_TABLE_DISPATCH,
	db.EDGES_TABLE_CHILD,
	db.EDGES_TABLE_PARENT,
	db.EDGES_TABLE_TYPE,
	db.EDGES_TABLE_NAME,
	db.EDGES_TABLE_ARG_INDEX,
}

type EdgeEntity struct {
	dispatch_id string
	e           *models.Edge
}

func (e *EdgeEntity) Fields() []string {
	return EDGE_ENTITY_KEYS
}

func (e *EdgeEntity) Values() []any {
	return []any{
		e.dispatch_id,
		e.e.Target,
		e.e.Source,
		e.e.Metadata.ParamType,
		e.e.Metadata.Name,
		e.e.Metadata.ArgIndex,
	}
}

func (e *EdgeEntity) Fieldrefs() []any {
	return []any{
		&e.dispatch_id,
		&e.e.Target,
		&e.e.Source,
		&e.e.Metadata.ParamType,
		&e.e.Metadata.Name,
		&e.e.Metadata.ArgIndex,
	}
}

func (e *EdgeEntity) Joins() []JoinCondition {
	return []JoinCondition{}
}

func GetEdgeEntities(t *sql.Tx, f Filters, sort_key string, ascending bool) ([]EdgeEntity, *models.APIError) {
	template := generateSelectTemplate(
		db.EDGES_TABLE,
		EDGE_ENTITY_KEYS,
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
	res := make([]EdgeEntity, 0)
	for rows.Next() {
		e := EdgeEntity{e: &models.Edge{}}
		err := rows.Scan((&e).Fieldrefs()...)
		if err != nil {
			slog.Error(fmt.Sprintf("Error querying row: %s\n", err.Error()))
			return nil, models.NewGenericServerError(err)
		}
		res = append(res, e)
	}
	return res, nil
}

type GraphView struct {
	// Nodes assumed to be sorted by node id
	g   *models.Graph
	adj map[int][]int
}

func NewGraphView(g *models.Graph) GraphView {
	adj := make(map[int][]int)
	for i := 0; i < len(g.Nodes); i++ {
		adj[i] = make([]int, 0)
	}
	for _, edge := range g.Links {
		adj[edge.Source] = append(adj[edge.Source], edge.Target)
	}
	return GraphView{g, adj}
}

func (gv *GraphView) Nodes() []int {
	n := make([]int, len(gv.g.Nodes))
	for i, item := range gv.g.Nodes {
		n[i] = item.NodeId
	}
	return n
}

func (gv *GraphView) GetNode(node_id int) *models.ElectronSchema {
	return &gv.g.Nodes[node_id]
}

func (gv *GraphView) GetAdj(source int) []int {
	return gv.adj[source]
}

func (gv *GraphView) sortTopologically() ([]int, *models.APIError) {
	in_deg := make(map[int]int)
	adj := make(map[int][]int)
	ready_nodes := make([]int, 0)
	sorted_nodes := make([]int, 0)
	nodes_visited := 0

	// Populate indegrees and adjacency maps
	// Use Kahn's method. Maintain a queue R of
	// nodes with zero indegree. These represent nodes whose parent tasks
	// have been completed and are ready to be submitted.
	//
	//
	// sorted_nodes = []
	//
	// while R is not empty:
	//   node := R.dequeue()
	//   sorted_nodes.append(node)
	//   for n in node.children():
	//     indegree[n] -= 1
	// 	   if indegree[n] == 0;
	//         R.enqueue(n)
	//

	for _, n := range gv.Nodes() {
		adj[n] = []int{}
		in_deg[n] = 0
	}
	for _, edge := range gv.g.Links {
		adj[edge.Source] = append(adj[edge.Source], edge.Target)
		in_deg[edge.Target] += 1
	}

	for n, deg := range in_deg {
		if deg == 0 {
			ready_nodes = append(ready_nodes, n)
			slog.Info(fmt.Sprintf("Found parentless node %d\n", n))
		}
	}
	for len(ready_nodes) > 0 {
		next_ready_nodes := make([]int, 0)
		for _, i := range ready_nodes {
			slog.Info(fmt.Sprintf("Topological sort: visiting node %d\n", i))
			sorted_nodes = append(sorted_nodes, i)
			nodes_visited += 1
			for _, j := range adj[i] {
				in_deg[j] -= 1
				if in_deg[j] == 0 {
					next_ready_nodes = append(next_ready_nodes, j)
				}
			}
		}
		ready_nodes = next_ready_nodes
	}
	if nodes_visited != len(gv.g.Nodes) {
		slog.Warn(fmt.Sprintf("Graph contains cycles. Visited %d out of %d nodes\n", nodes_visited, len(gv.g.Nodes)))
		return sorted_nodes, models.NewGenericClientError("Invalid transport graph")
	}
	return sorted_nodes, nil
}

func createEdges(t *sql.Tx, dispatch_id string, edges []models.Edge) (int, *models.APIError) {
	if len(edges) == 0 {
		return 0, nil
	}
	ents := make([]EdgeEntity, len(edges))
	for i := 0; i < len(edges); i++ {
		ents[i].dispatch_id = dispatch_id
		ents[i].e = &edges[i]
	}
	template, _ := generateInsertTemplate(db.EDGES_TABLE, ents[0].Fields())
	slog.Debug(fmt.Sprintf("Insert template: %s\n", template))
	stmt, err := t.Prepare(template)
	if err != nil {
		slog.Error(fmt.Sprintf("Error preparing statement: %s", err.Error()))
		return 0, models.NewGenericServerError(err)
	}
	for i := 0; i < len(ents); i++ {
		_, err := stmt.Exec((&ents[i]).Values()...)
		if err != nil {
			slog.Error(fmt.Sprintf("Error inserting row: %s", err.Error()))
			return i, models.NewGenericServerError(err)
		}
	}
	slog.Debug(fmt.Sprintf("Inserted %d rows", len(ents)))
	return len(ents), nil
}

// TODO: check if passing tg by values allows mutating node and link slices
func CreateGraph(c *common.Config, t *sql.Tx, dispatch_id string, tg *models.Graph) *models.APIError {

	for _, item := range tg.Nodes {
		err := CreateElectronMetadata(t, dispatch_id, item.NodeId, &item.Metadata)
		if err != nil {
			slog.Error(fmt.Sprintf("Error creating electron: %s", err.Error()))
			return err
		}
	}
	for i := range tg.Nodes {
		err := createElectronAssets(c, t, dispatch_id, &tg.Nodes[i])
		if err != nil {
			slog.Error(fmt.Sprintf("Error creating electron assets: %s", err.Error()))
			return err
		}
	}

	_, err := createEdges(t, dispatch_id, tg.Links)
	if err != nil {
		slog.Info(fmt.Sprintf("Error creating edges: %s", err.Error()))
		return err
	}
	return err
}

func GetGraph(c *common.Config, t *sql.Tx, dispatch_id string, load_assets bool) (models.Graph, *models.APIError) {
	edges, err := getAllEdges(t, dispatch_id)
	if err != nil {
		return models.Graph{}, err
	}
	electrons, err := GetAllElectrons(c, t, dispatch_id, load_assets)
	if err != nil {
		return models.Graph{}, err
	}
	return models.Graph{Nodes: electrons, Links: edges}, nil
}

func getAllEdges(t *sql.Tx, dispatch_id string) ([]models.Edge, *models.APIError) {

	f := Filters{}
	(&f).AddEq(db.EDGES_TABLE_DISPATCH, dispatch_id)
	ents, err := GetEdgeEntities(t, f, db.EDGES_TABLE_CHILD, true)
	if err != nil {
		return nil, err
	}
	edges := make([]models.Edge, len(ents))
	for i := range edges {
		edges[i] = *ents[i].e
	}
	return edges, nil
}

func GetOutgoingEdges(t *sql.Tx, dispatch_id string, node_id int) ([]EdgeEntity, *models.APIError) {
	f := Filters{}
	(&f).AddEq(db.EDGES_TABLE_DISPATCH, dispatch_id)
	(&f).AddEq(db.EDGES_TABLE_PARENT, node_id)
	ents, err := GetEdgeEntities(t, f, db.EDGES_TABLE_ID, true)
	if err != nil {
		return nil, err
	}
	return ents, nil
}

// For computing electron inputs
func GetIncomingEdges(t *sql.Tx, dispatch_id string, node_id int) ([]EdgeEntity, *models.APIError) {
	f := Filters{}
	(&f).AddEq(db.EDGES_TABLE_DISPATCH, dispatch_id)
	(&f).AddEq(db.EDGES_TABLE_CHILD, node_id)
	ents, err := GetEdgeEntities(t, f, db.EDGES_TABLE_ID, true)
	if err != nil {
		return nil, err
	}
	return ents, nil
}
