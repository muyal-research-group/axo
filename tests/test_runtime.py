import pytest 
from axo.runtime.local import LocalRuntime
from axo.storage.data import LocalStorageService

@pytest.mark.asyncio
async def test_runtime():
    lr = LocalRuntime(
        runtime_id="local_rt",
        storage_service=LocalStorageService(
            storage_service_id="local_ss"
        )
    )