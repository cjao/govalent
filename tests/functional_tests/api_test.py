import httpx
import json
import uuid
import pprint
import pytest
import tempfile
import uuid


import covalent as ct
from covalent._dispatcher_plugins.local import LocalDispatcher, strip_local_uris, ResultSchema

DISPATCHER_ADDR="http://localhost:48008"


@pytest.fixture
def mock_manifest():
    @ct.electron
    def task(x):
        return x

    @ct.lattice
    def workflow(x):
        return task(x)

    workflow.build_graph(1)
    with tempfile.TemporaryDirectory() as tmp_dir:
        manifest = LocalDispatcher.prepare_manifest(workflow, tmp_dir)
        manifest.metadata.dispatch_id = str(uuid.uuid4())
    return manifest


def test_submit_manifest(mock_manifest):

    stripped = strip_local_uris(mock_manifest)
    pprint.pp(json.loads(stripped.model_dump_json()))
    print("")
    resp = httpx.post(f"{DISPATCHER_ADDR}/dispatches", data=stripped.model_dump_json())
    body = resp.json()
    pprint.pp(body)
    resp.raise_for_status()

    # Check that the response body has right structure
    returned_manifest = ResultSchema.model_validate(body)

    # TODO: Check manifest attributes including asset upload uris (size = 0 and size > 0)
    # TODO: check handling of nullable attributes

    assert len(returned_manifest.metadata.dispatch_id) > 0

    assert returned_manifest.metadata.start_time is None
    assert returned_manifest.metadata.end_time is None

    assert returned_manifest.lattice.metadata.name == mock_manifest.lattice.metadata.name
    assert returned_manifest.lattice.metadata.executor == mock_manifest.lattice.metadata.executor
    assert returned_manifest.lattice.metadata.executor_data == mock_manifest.lattice.metadata.executor_data

    assert returned_manifest.lattice.metadata.workflow_executor == mock_manifest.lattice.metadata.workflow_executor
    assert returned_manifest.lattice.metadata.workflow_executor_data == mock_manifest.lattice.metadata.workflow_executor_data

    # Workflow assets
    returned_assets = returned_manifest.assets
    submitted_assets = mock_manifest.assets
    assert returned_assets.model_dump().keys() == submitted_assets.model_dump().keys()

    # Check that remote_uri is populated for all non-null assets
    for _, asset in returned_assets:
        if asset.size > 0:
            assert len(asset.remote_uri) > 0
        if asset.size == 0:
            assert len(asset.remote_uri) == 0

    # Transport graph
    returned_tg = returned_manifest.lattice.transport_graph
    submitted_tg = mock_manifest.lattice.transport_graph

    # check nodes
    assert len(returned_tg.nodes) == len(submitted_tg.nodes)

    for i, returned_node in enumerate(returned_tg.nodes):
        submitted_node = submitted_tg.nodes[i]
        assert returned_node.id == submitted_node.id
        assert returned_node.metadata == submitted_node.metadata

    # Check node assets
    for i, returned_node in enumerate(returned_tg.nodes):
        submitted_node = submitted_tg.nodes[i]
        assert returned_node.assets.model_dump().keys() == submitted_node.assets.model_dump().keys()
        for name, asset in returned_node.assets:
            # Some assets are optional
            if not asset:
                continue
            if asset.size > 0:
                assert len(asset.remote_uri) > 0
            if asset.size == 0:
                assert len(asset.remote_uri) == 0


    # check edges
    assert len(returned_tg.links) == len(submitted_tg.links)
    assert returned_tg.links == submitted_tg.links



def test_export_manifest(mock_manifest):

    stripped = strip_local_uris(mock_manifest)
    pprint.pp(json.loads(stripped.model_dump_json()))
    print("")
    resp = httpx.post(f"{DISPATCHER_ADDR}/dispatches", data=stripped.model_dump_json())
    resp.raise_for_status()
    body = resp.json()
    returned_manifest = ResultSchema.model_validate(body)

    dispatch_id = returned_manifest.metadata.dispatch_id
    resp = httpx.get(f"{DISPATCHER_ADDR}/dispatches/{dispatch_id}")
    body = resp.json()
    pprint.pp(body)
    resp.raise_for_status()
    exported_manifest = ResultSchema.model_validate(body)

    assert exported_manifest.metadata.dispatch_id == dispatch_id

    assert exported_manifest.metadata.start_time == mock_manifest.metadata.start_time
    assert exported_manifest.metadata.end_time == mock_manifest.metadata.end_time

    assert exported_manifest.lattice.metadata.name == mock_manifest.lattice.metadata.name
    assert exported_manifest.lattice.metadata.executor == mock_manifest.lattice.metadata.executor
    assert exported_manifest.lattice.metadata.executor_data == mock_manifest.lattice.metadata.executor_data

    assert exported_manifest.lattice.metadata.workflow_executor == mock_manifest.lattice.metadata.workflow_executor
    assert exported_manifest.lattice.metadata.workflow_executor_data == mock_manifest.lattice.metadata.workflow_executor_data

    # Workflow assets
    exported_assets = exported_manifest.assets
    submitted_assets = mock_manifest.assets
    assert exported_assets.model_dump().keys() == submitted_assets.model_dump().keys()

    # Check that remote_uri is populated iff asset size > 0
    for _, asset in exported_assets:
        if asset.size > 0:
            assert len(asset.remote_uri) > 0
        if asset.size == 0:
            assert len(asset.remote_uri) == 0

    exported_tg = exported_manifest.lattice.transport_graph
    submitted_tg = mock_manifest.lattice.transport_graph

    # Check nodes
    assert len(exported_tg.nodes) == len(submitted_tg.nodes)
    for i, exported_node in enumerate(exported_tg.nodes):
        submitted_node = submitted_tg.nodes[i]
        assert exported_node.id == submitted_node.id
        assert exported_node.metadata == submitted_node.metadata

    # Check node assets
    for i, exported_node in enumerate(exported_tg.nodes):
        submitted_node = submitted_tg.nodes[i]
        assert exported_node.assets.model_dump().keys() == submitted_node.assets.model_dump().keys()
        for name, asset in exported_node.assets:
            # Some assets are optional
            if not asset:
                continue
            if asset.size > 0:
                assert len(asset.remote_uri) > 0
            if asset.size == 0:
                assert len(asset.remote_uri) == 0

    assert len(exported_tg.links) == len(submitted_tg.links)
    exported_tg.links.sort(key=lambda x: x.source)
    assert exported_tg.links == submitted_tg.links


def test_bulk_get_dispatches(mock_manifest):
    stripped = strip_local_uris(mock_manifest)
    pprint.pp(json.loads(stripped.model_dump_json()))
    print("")
    resp = httpx.post(f"{DISPATCHER_ADDR}/dispatches", data=stripped.model_dump_json())
    resp.raise_for_status()
    body = resp.json()
    returned_manifest = ResultSchema.model_validate(body)

    dispatch_id = returned_manifest.metadata.dispatch_id
    resp = httpx.get(f"{DISPATCHER_ADDR}/dispatches", params={"dispatch_id": dispatch_id})
    body = resp.json()
    assert len(body["records"]) == 1
    assert body["records"][0]["dispatch_id"] == dispatch_id


def test_delete_dispatch(mock_manifest):
    stripped = strip_local_uris(mock_manifest)
    pprint.pp(json.loads(stripped.model_dump_json()))
    print("")
    resp = httpx.post(f"{DISPATCHER_ADDR}/dispatches", data=stripped.model_dump_json())
    resp.raise_for_status()
    body = resp.json()
    returned_manifest = ResultSchema.model_validate(body)
    dispatch_id = returned_manifest.metadata.dispatch_id

    httpx.delete(f"{DISPATCHER_ADDR}/dispatches/{dispatch_id}").raise_for_status()

    resp = httpx.get(f"{DISPATCHER_ADDR}/dispatches", params={"dispatch_id": dispatch_id})
    body = resp.json()
    assert len(body["records"]) == 0



def test_create_get_assets():
    asset_details_1 = {"key": "dispatch-1", "size": 5, "uri": "file://local-uri"}
    asset_details_2 = {"key": "dispatch-2", "size": 2}
    reqBody = {"assets": [asset_details_1, asset_details_2]}
    resp = httpx.post(f"{DISPATCHER_ADDR}/assets", json=reqBody)
    resp.raise_for_status()
    body = resp.json()
    assert len(body["assets"]) == 2
    assert body["assets"][0]["key"] == reqBody["assets"][0]["key"]
    assert body["assets"][0]["size"] == reqBody["assets"][0]["size"]
    assert len(body["assets"][0]["remote_uri"]) == 0
    assert body["assets"][1]["key"] == reqBody["assets"][1]["key"]
    assert body["assets"][1]["size"] == reqBody["assets"][1]["size"]
    assert len(body["assets"][1]["remote_uri"]) > 0

    resp = httpx.get(f"{DISPATCHER_ADDR}/assets", params={"prefix": "dispatch"})
    resp.raise_for_status()
    body = resp.json()
    assert len(body["assets"]) == 2
    assert body["assets"][0]["key"] == reqBody["assets"][0]["key"]
    assert body["assets"][0]["size"] == reqBody["assets"][0]["size"]
    assert len(body["assets"][0]["remote_uri"]) > 0
    assert body["assets"][1]["key"] == reqBody["assets"][1]["key"]
    assert body["assets"][1]["size"] == reqBody["assets"][1]["size"]
    assert len(body["assets"][1]["remote_uri"]) > 0
