import pytest 
from axo.endpoint.endpoint import LocalEndpoint,DistributedEndpoint
from axo.core.models import MetadataX


from axo import Axo
class Calc(Axo):
    def sum(self,x:int,y:int,**kwargs):
        return x+y




# @pytest.mark.skip("")
@pytest.mark.asyncio
async def test_local_endpoint():
    le = LocalEndpoint(endpoint_id="e1")
    calc = Calc()
    res = le.method_execution(key="sum",fname="sum",ao = calc,fargs=[1,1],fkwargs={})
    assert res.is_ok
    fname = "_NOT_FOUNDsum"
    res = le.method_execution(key="sum",fname=fname,ao = calc, fargs=[1,1],fkwargs={})
    assert res.is_err
    # print(res)

@pytest.mark.asyncio
async def test_local_put_get_endpoint():
    le = LocalEndpoint(endpoint_id="e1")
    m = MetadataX.model_validate({
        "axo_key":"ao",
        "axo_module":"module",
        "axo_name":"AO",
        "axo_class_name":"Calc",
        "axo_version":"v0",
        "axo_bucket_id":"b1",
        "axo_source_bucket_id":"b1_source",
        "axo_sink_bucket_id":"b1_sink",
        "axo_endpoint_id":"axo_endpoint_id",
        "axo_dependencies":["numpy"]
    })
    key = "TESTING"
    res = le.put(key=key,value=m)
    assert res.is_ok
    res = le.get(key)
    assert res.is_ok

@pytest.mark.skip("")
@pytest.mark.asyncio
async def test_distributed_endpoint():
    le = DistributedEndpoint(endpoint_id="activex-endpoint-0")
    calc = Calc(
        sink_bucket_id = "TEST"
    )
    # print("RESULT",res)
    # await asyncio.sleep(1000)
