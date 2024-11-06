import covalent as ct
from covalent._dispatcher_plugins.local import LocalDispatcher
import tempfile

@ct.electron
def task(x):
    return x

@ct.lattice
def workflow(x):
    return task(x)


workflow.build_graph(1)

with tempfile.TemporaryDirectory() as tmp_dir:
    manifest = LocalDispatcher.prepare_manifest(workflow, tmp_dir)
    with open("basic-sample-manifest.json", "w") as f:
        print(manifest.model_dump_json(), file=f)
        
