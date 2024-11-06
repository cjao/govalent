CREATE TABLE IF NOT EXISTS dispatches (
    dispatch_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    status TEXT NOT NULL,
    executor TEXT,
    executor_data TEXT,
    start_time TEXT,
    end_time TEXT
);

CREATE TABLE IF NOT EXISTS electrons (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    parent_dispatch_id TEXT NOT NULL REFERENCES dispatches(id),
    sub_dispatch_id TEXT REFERENCES dispatches(id),
    transport_graph_node_id INTEGER NOT NULL,
    task_group_id INTEGER NOT NULL,
    name TEXT,
    status TEXT,
    executor TEXT,
    executor_data TEXT,
    start_time TEXT,
    end_time TEXT,
    job_id TEXT,
    sort_order INTEGER
);


CREATE TABLE IF NOT EXISTS edges (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    child_electron_id INTEGER NOT NULL REFERENCES electrons(id),
    parent_electron_id INTEGER NOT NULL REFERENCES electrons(id),
    edge_name TEXT NOT NULL,
    param_type TEXT NOT NULL,
    arg_index INTEGER
);

CREATE TABLE IF NOT EXISTS taskgroups (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dispatch_id TEXT REFERENCES electrons(parent_dispatch_id),
    task_group_id INTEGER,
    pending_parents INTEGER
);


CREATE TABLE IF NOT EXISTS assets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    key TEXT UNIQUE NOT NULL,
    scheme TEXT NOT NULL,
    prefix TEXT,
    remote_uri TEXT,
    size INTEGER,
    digest_alg TEXT,
    digest TEXT
);

CREATE TABLE IF NOT EXISTS electronassets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    electron_id INTEGER NOT NULL REFERENCES electrons(id),
    asset_id INTEGER NOT NULL REFERENCES assets(id),
    name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dispatchassets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dispatch_id TEXT NOT NULL REFERENCES dispatches(id),
    asset_id INTEGER NOT NULL REFERENCES assets(id),
    name TEXT NOT NULL
);
