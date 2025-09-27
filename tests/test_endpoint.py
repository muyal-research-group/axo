import pytest 
import pytest_asyncio
from axo.endpoint.endpoint import LocalEndpoint,DistributedEndpoint
from axo.endpoint.manager import DistributedEndpointManager
from axo.contextmanager import AxoContextManager
from axo.core.models import MetadataX
from axo.storage.services import MictlanXStorageService
from axo.storage.types import StorageService

@pytest_asyncio.fixture(scope="session", autouse=True)
# @pytest.mark.asyncio
async def before_all_tests():
    ss = MictlanXStorageService(
        bucket_id   = "b1",
    )
    bids = ["axo","b1","bao"]
    for bid in bids:
        res = await ss.client.delete_bucket(bid)
        print(f"BUCKET [{bid}] was clean")
    yield

from axo import Axo
class Calc(Axo):
    def __init__(self,x:int, **kwargs):
        super().__init__(**kwargs)
        self.x =x

    def sum(self,x:int,y:int,**kwargs):
        return x+y


@pytest.fixture
def storage_service() -> StorageService:
    return MictlanXStorageService()
# 

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
    # res = le.task_execution(lambda x:x)
    # assert res.is_err
    res = le.elasticity(3)
    assert res.is_err
    


@pytest.mark.skip("")
@pytest.mark.asyncio
async def test_local_put_get_endpoint():
    le = LocalEndpoint(endpoint_id="e1")
    m = MetadataX.model_validate({
        "axo_key":"ao",
        "axo_module":"module",
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

@pytest.mark.skip("")
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

@pytest.mark.asyncio
async def test_distributed_endpoint_method_execution(storage_service:StorageService):
    endpoint_id = "axo-endpoint-0"
    le          = DistributedEndpoint(endpoint_id=endpoint_id)
    with AxoContextManager.distributed(endpoint_manager=DistributedEndpointManager(endpoints={endpoint_id: le} ), storage_service=storage_service) as dr: 
        ao          = Calc(1,
                        axo_endpoint_id =endpoint_id ,
                        axo_alias = "ALIAS",
                        axo_bucket_id= "bao",
                        axo_key = "aotest"
        )
        res = await ao.persistify()
        assert res.is_ok
        
    value = ao._acx_metadata
    
    res = le.put(key=value.axo_key, value=value)
    assert res.is_ok
    
    res         = le.method_execution(
        key   = value.axo_key,
        ao    = ao,
        fname = "sum",
        fargs = [1,2]
    )

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
