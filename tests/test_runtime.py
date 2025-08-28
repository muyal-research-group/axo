import pytest 
import time as T
from axo.runtime.local import LocalRuntime
from axo.runtime import set_runtime
from axo.storage.services import InMemoryStorageService
from axo import Axo,axo_method
from axo.errors import AxoErrorType
# from axo.storage.services im
from option import Result
import hashlib as H

class Hasher(Axo):
    def __init__(self,x:int=10, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.x = x
        
    @axo_method
    def sha256(self,x:bytes,**kwargs):
        import hashlib as H
        h = H.sha256()
        h.update(x)
        return h.digest()
@pytest.fixture
def bucket_id() -> str:
    # choose any bucket string; InMemoryStorageService just namespaces by it
    return "test-bucket"

@pytest.fixture
def key() -> str:
    return "hasher_0"

@pytest.fixture
def runtime() -> LocalRuntime:
    """
    LocalRuntime with the InMemoryStorageService.
    """
    lr = LocalRuntime(
        storage_service=InMemoryStorageService(storage_service_id="local_mem"),
        runtime_id="rt-local",
        maxsize=32,
        q_tick_s=0,  # keep the loop snappy
    )
    set_runtime(lr)
    return lr


 
# --------------------------------- Tests ----------------------------------

@pytest.mark.asyncio
async def test_local_runtime_persist_and_load(runtime: LocalRuntime, bucket_id: str, key: str):
    # Create a real Axo instance, targeting the endpoint LocalRuntime creates by default.
    x = Hasher(axo_key=key, axo_endpoint_id="axo-endpoint-0")

    # Persist into the chosen bucket
    res_put = await runtime.persistify(instance=x, bucket_id=bucket_id, key=key)
    assert res_put.is_ok, f"persistify failed: {res_put.unwrap_err() if res_put.is_err else ''}"
    assert res_put.unwrap() == key

    # Load it back as a live object (Loader under the hood)
    res_get = await runtime.get_active_object(bucket_id=bucket_id, key=key)
    assert res_get.is_ok, f"get_active_object failed: {res_get.unwrap_err() if res_get.is_err else ''}"

    obj = res_get.unwrap()
    # Sanity: method round-trip works and returns the exact SHA-256 digest
    payload = b"nacho"
    v:Result[bytes, Exception] = obj.sha256(payload)
    assert v.is_ok
    assert v.unwrap() == H.sha256(payload).digest()

    # Tidy up the runtime thread
    runtime.stop()
    assert runtime.is_running is False


@pytest.mark.asyncio
async def test_local_runtime_name_and_endpoint(runtime: LocalRuntime):
    # Name is what we set
    assert runtime.name == "rt-local"

    # Endpoint manager exposes the default endpoint
    ep = runtime.endpoint_manager.get_endpoint(endpoint_id="axo-endpoint-0")
    assert ep is not None, "default local endpoint should exist"
    # If your Endpoint class exposes `endpoint_id`, verify it matches
    assert getattr(ep, "endpoint_id", "axo-endpoint-0") == "axo-endpoint-0"

    runtime.stop()

@pytest.mark.skip("It is not relveant now..")
@pytest.mark.asyncio
async def test_local_runtime_missing_endpoint_fails(bucket_id: str):
    # Build a runtime as usual
    rt = LocalRuntime(
        storage_service=InMemoryStorageService(storage_service_id="local_mem"),
        runtime_id="rt-missing-endpoint",
        q_tick_s=0,
    )

    # Point the Axo instance to a non-existent endpoint id
    bad = Hasher(axo_key="bad_0", axo_endpoint_id="no-such-endpoint")

    res = await rt.persistify(instance=bad, bucket_id=bucket_id, key="bad_0")
    assert res.is_err, "persistify should fail if endpoint does not exist"

    # Depending on your refactor, we expect NOT_FOUND; if you're still returning a raw Exception,
    # this still asserts the failure without over-constraining the exact type.
    err = res.unwrap_err()
    assert getattr(err, "error_type", AxoErrorType.NOT_FOUND) in (AxoErrorType.NOT_FOUND, AxoErrorType.INTERNAL_ERROR)

    rt.stop()