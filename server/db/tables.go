package db

import "fmt"

// Constants
//

const (
	DISPATCH_TABLE                        = "dispatches"
	ELECTRON_TABLE                        = "electrons"
	EDGES_TABLE                           = "edges"
	ASSET_TABLE                           = "assets"
	ASSET_LINKS_TABLE                     = "assetlinks"
	ASSET_TABLE_ID                        = "id"
	ASSET_TABLE_SCHEME                    = "scheme"
	ASSET_TABLE_BASE                      = "base_path"
	ASSET_TABLE_KEY                       = "key"
	ASSET_TABLE_SIZE                      = "size"
	ASSET_TABLE_DIGEST_ALG                = "digest_alg"
	ASSET_TABLE_DIGEST                    = "digest"
	ASSET_TABLE_REMOTE_URI                = "remote_uri"
	ASSET_LINKS_TABLE_DISPATCH_ID         = "dispatch_id"
	ASSET_LINKS_TABLE_NODE_ID             = "transport_graph_node_id"
	ASSET_LINKS_TABLE_ASSET_ID            = "asset_id"
	ASSET_LINKS_TABLE_NAME                = "name"
	DISPATCH_TABLE_CREATED_AT             = "created_at"
	DISPATCH_TABLE_UPDATED_AT             = "updated_at"
	DISPATCH_TABLE_NAME                   = "name"
	DISPATCH_TABLE_STATUS                 = "status"
	DISPATCH_TABLE_EXECUTOR               = "executor"
	DISPATCH_TABLE_EXECUTOR_DATA          = "executor_data"
	DISPATCH_TABLE_WORKFLOW_EXECUTOR      = "workflow_executor"
	DISPATCH_TABLE_WORKFLOW_EXECUTOR_DATA = "workflow_executor_data"
	DISPATCH_TABLE_PYTHON_VERSION         = "python_version"
	DISPATCH_TABLE_COVALENT_VERSION       = "covalent_version"
	DISPATCH_TABLE_START_TIME             = "start_time"
	DISPATCH_TABLE_END_TIME               = "end_time"
	DISPATCH_TABLE_ID                     = "id"
	DISPATCH_TABLE_ROOT_ID                = "root_dispatch_id"
	ELECTRON_TABLE_ID                     = "id"
	ELECTRON_TABLE_NODE_ID                = "transport_graph_node_id"
	ELECTRON_TABLE_GID                    = "task_group_id"
	ELECTRON_TABLE_DISPATCH_ID            = "parent_dispatch_id"
	ELECTRON_TABLE_SUBDISPATCH_ID         = "sub_dispatch_id"
	ELECTRON_TABLE_NAME                   = "name"
	ELECTRON_TABLE_STATUS                 = "status"
	ELECTRON_TABLE_EXECUTOR               = "executor"
	ELECTRON_TABLE_EXECUTOR_DATA          = "executor_data"
	ELECTRON_TABLE_START_TIME             = "start_time"
	ELECTRON_TABLE_END_TIME               = "end_time"
	ELECTRON_TABLE_SORT_ORDER             = "sort_order"
	EDGES_TABLE_ID                        = "id"
	EDGES_TABLE_CHILD                     = "child_node_id"
	EDGES_TABLE_PARENT                    = "parent_node_id"
	EDGES_TABLE_DISPATCH                  = "dispatch_id"
	EDGES_TABLE_NAME                      = "edge_name"
	EDGES_TABLE_TYPE                      = "param_type"
	EDGES_TABLE_ARG_INDEX                 = "arg_index"
)

var ERR_NOT_FOUND = fmt.Errorf("Record not found")
