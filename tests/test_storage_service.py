import pytest
from axo.storage.data import LocalStorageService

local_ss = LocalStorageService(
    storage_service_id="local",
    sink_path="/sink"
)
bucket_id = "axo"
key = "k1"

@pytest.mark.asyncio
async def test_local_put():
    res = await local_ss.put(bucket_id=bucket_id,key=key,data=b"HOLA")
    assert res.is_ok
# @pytest.mark.skip("")
@pytest.mark.asyncio
async def test_local_get():
    res = await local_ss.get(bucket_id=bucket_id,key=key)
    print(res)
    assert res.is_ok

@pytest.mark.asyncio
async def test_get_bucket_metadata():
    res=  await local_ss.get_bucket_metadata(bucket_id)
    print(res)
    assert res.is_ok
