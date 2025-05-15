CREATE TABLE IF NOT EXISTS dispatches (
    id TEXT PRIMARY KEY,
    root_dispatch_id TEXT,
    name TEXT NOT NULL,
    status TEXT NOT NULL,
    executor TEXT,
    executor_data TEXT,
    workflow_executor TEXT,
    workflow_executor_data TEXT,
    python_version TEXT,
    covalent_version TEXT,
    start_time DATETIME,
    end_time DATETIME,
    created_at DATETIME,
    updated_at DATETIME
);

CREATE TABLE IF NOT EXISTS electrons (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    parent_dispatch_id TEXT NOT NULL REFERENCES dispatches(id) ON DELETE CASCADE,
    sub_dispatch_id TEXT REFERENCES dispatches(id) ON DELETE CASCADE,
    transport_graph_node_id INTEGER NOT NULL,
    task_group_id INTEGER NOT NULL,
    name TEXT,
    status TEXT,
    executor TEXT,
    executor_data TEXT,
    created_at DATETIME,
    start_time DATETIME,
    updated_at DATETIME,
    end_time DATETIME,
    job_id TEXT,
);


CREATE TABLE IF NOT EXISTS edges (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dispatch_id TEXT NOT NULL REFERENCES dispatches(id) ON DELETE CASCADE,
    child_node_id INTEGER NOT NULL,
    parent_node_id INTEGER NOT NULL,
    edge_name TEXT NOT NULL,
    param_type TEXT NOT NULL,
    arg_index INTEGER
);


CREATE TABLE IF NOT EXISTS assets (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	scheme TEXT NOT NULL,
	base_path TEXT NOT NULL,
	key TEXT UNIQUE NOT NULL,
	size INTEGER NOT NULL,
	digest_alg TEXT,
	digest TEXT,
	remote_uri TEXT
);

CREATE TABLE IF NOT EXISTS assetlinks (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	dispatch_id TEXT REFERENCES dispatches(id) ON DELETE CASCADE NOT NULL,
	transport_graph_node_id TEXT NOT NULL,
	asset_id INTEGER REFERENCES assets(id) ON DELETE CASCADE NOT NULL,
	name TEXT NOT NULL
);
