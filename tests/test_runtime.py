import pytest 
import time as T
from axo.runtime.local import LocalRuntime
from axo.storage.data import LocalStorageService
from axo import Axo,axo_method
import hashlib as H

class Hasher(Axo):
    @axo_method
    def sha256(self,x:bytes,**kwargs):
        h = H.sha256()
        h.update(x)
        return h.digest()
    

@pytest.mark.asyncio
async def test_runtime():
    runtime_id = "local_rt"
    lr = LocalRuntime(
        runtime_id=runtime_id,
        storage_service=LocalStorageService(
            storage_service_id="local_ss"
        )
    )
    x = Hasher(axo_key = "hasher_0")
    res= await lr.persistify(instance=x)
    print("RES",res)
    assert res.is_ok
    # T.sleep(5)
    assert lr.name == runtime_id