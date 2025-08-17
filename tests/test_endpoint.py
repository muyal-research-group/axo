import pytest 
from axo.endpoint.endpoint import LocalEndpoint,DistributedEndpoint
from axo.core.models import MetadataX


from axo import Axo
class Calc(Axo):
    def __init__(self,x:int, **kwargs):
        super().__init__(**kwargs)
        self.x =x
    def sum(self,x:int,y:int,**kwargs):
        return x+y




# @pytest.mark.skip("")
@pytest.mark.asyncio
async def test_local_endpoint():
    le = LocalEndpoint(endpoint_id="e1")
    calc = Calc(100)
    res = le.method_execution(key="sum",fname="sum",ao = calc,fargs=[1,1],fkwargs={})
    assert res.is_ok
    fname = "_NOT_FOUNDsum"
    res = le.method_execution(key="sum",fname=fname,ao = calc, fargs=[1,1],fkwargs={})
    assert res.is_err
    fname = "x"
    res = le.method_execution(key="sum",fname=fname,ao = calc)
    assert res.is_err


@pytest.mark.asyncio
async def test_add_code_class_def():
    le = LocalEndpoint(endpoint_id="e1")
    calc = Calc(100)
    res = le.add_code(calc)
    assert res.is_ok
    res = le.add_class_definition(calc)
    assert res.is_ok
    res = le.task_execution(lambda x:x)
    assert res.is_err
    res = le.elasticity(3)
    assert res.is_err
    


@pytest.mark.asyncio
async def test_local_put_get_endpoint():
    le = LocalEndpoint(endpoint_id="e1")
    m = MetadataX.model_validate({
        "axo_key":"ao",
        "axo_module":"module",
        "axo_name":"AO",
        "axo_class_name":"Calc",
        "axo_version":0,
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

# @pytest.mark.skip("")
@pytest.mark.asyncio
async def test_distributed_endpoint_ping():
    le = DistributedEndpoint(endpoint_id="activex-endpoint-0")
    res = le.ping()
    assert res.is_ok
@pytest.mark.asyncio
async def test_distributed_endpoint_put_metadata():
    le = DistributedEndpoint(endpoint_id="activex-endpoint-0")
    value =  MetadataX.model_validate({
        "axo_key":"ao",
        "axo_module":"module",
        "axo_name":"AO",
        "axo_class_name":"Calc",
        "axo_version":0,
        "axo_bucket_id":"b1",
        "axo_source_bucket_id":"b1_source",
        "axo_sink_bucket_id":"b1_sink",
        "axo_endpoint_id":"axo_endpoint_id",
        "axo_dependencies":["numpy"]
    })
    res = le.put(
        key="ao1",
        value=value
    )
    assert res.is_ok

# @pytest.mark.skip("")
@pytest.mark.asyncio
async def test_distributed_endpoint_put_metadata():
    le = DistributedEndpoint(endpoint_id="axo-endpoint-0")
    value =MetadataX.model_validate({
        "axo_key":"ao",
        "axo_module":"module",
        "axo_name":"AO",
        "axo_class_name":"Calc",
        "axo_version":0,
        "axo_bucket_id":"b1",
        "axo_source_bucket_id":"b1_source",
        "axo_sink_bucket_id":"b1_sink",
        "axo_endpoint_id":le.endpoint_id,
        "axo_dependencies":["numpy"],
        "axo_alias":"AOALIAS"
    }) 
    res = le.put(key="HOLA", value=value)
    print("RES",res)
    assert res.is_ok
@pytest.mark.skip("")
@pytest.mark.asyncio
async def test_distributed_endpoint_method_execution():
    le = DistributedEndpoint(endpoint_id="activex-endpoint-0")
    ao = Calc(1)
    res = le.method_execution(key="ao",ao=ao,fname="sum",fargs=[1,2])
    print("RES",res)
    assert res.is_ok

@pytest.mark.skip("")
@pytest.mark.asyncio
async def test_distributed_endpoint():
    le = DistributedEndpoint(endpoint_id="activex-endpoint-0")
    res = le._cleanup()
    res = le._ensure_connection()
    assert not res
    res = le.ping()
    assert res.is_err



    # print("RESULT",res)
    # await asyncio.sleep(1000)
