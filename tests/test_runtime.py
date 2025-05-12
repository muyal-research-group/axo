import pytest 
from axo.runtime.local import LocalRuntime
from axo.storage.data import LocalStorageService

@pytest.mark.asyncio
async def test_runtime():
    runtime_id = "local_rt"
    lr = LocalRuntime(
        runtime_id=runtime_id,
        storage_service=LocalStorageService(
            storage_service_id="local_ss"
        )
    )
    assert lr.getName() == runtime_id