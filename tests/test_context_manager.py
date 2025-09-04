import pytest 
import time as T
from axo.contextmanager import AxoContextManager
from axo.runtime import get_runtime,set_runtime
from axo.runtime.local import LocalRuntime
from axo.endpoint.manager import DistributedEndpointManager
from axo.storage.types import StorageService
from axo.storage.services import MictlanXStorageService
from axo import Axo,axo_method

class Calc(Axo):
    
    def __init__(self,id:str,**kwargs):
        super().__init__(**kwargs)
        self.id=id
        self.path = "/source/01.pdf"

    @axo_method
    def sum(self,x:int,y:int,**kwargs):
        return x+y
    
    @axo_method
    def substract(self,x:int,y:int,**kwargs):
        return x-y
    @axo_method
    def multiply(self,x:int,y:int,**kwargs):
        return x*y
    @axo_method
    def divide(self,x:int,y:int,**kwargs):
        return x/y if y >0 else 0
    

    @axo_method 
    def test2(self):
        self.get_info()

    @axo_method
    def test(self,x:str):
        with open(x,"rb") as f:
            print(len(f.read()))

    @axo_method
    def get_info(self):
        print(f"Hola soy una Calculadora mi nombre es {self.id}")

@pytest.fixture
def storage_service() -> StorageService:
    return MictlanXStorageService(protocol="http")
# 

@pytest.fixture()
def dem():
    x = DistributedEndpointManager()
    x.add_endpoint(
        endpoint_id  = "axo-endpoint-0",
        hostname     = "localhost",
        protocol     = "tcp",
        pubsub_port  = 16666,
        req_res_port = 16667,
    )
    return x

@pytest.mark.asyncio
async def test_local_cm():
    lcm = AxoContextManager.local()
    assert not lcm.runtime.is_distributed
    lcm.stop()

@pytest.mark.asyncio
async def test_local_cm_method_execution():
    with AxoContextManager.local() as lrt:
        c:Calc = Calc(id = "CALC",axo_endpoint_id = "axo-endpoint-0")
        await c.persistify()
        res = c.sum(1,2)
        assert res.is_ok
        res = c.substract(1,2)
        assert res.is_ok
        res = c.multiply(1,2)
        assert res.is_ok
        res = c.divide(1,2)
        assert res.is_ok

@pytest.mark.asyncio
async def test_distributed_cm_method_execution(dem:DistributedEndpointManager,storage_service:StorageService):
    with AxoContextManager.distributed(endpoint_manager=dem,storage_service=storage_service) as drt:
        c:Calc = Calc(id = "CALC",axo_endpoint_id = "axo-endpoint-0")
        res = await c.persistify()
        assert res.is_ok
        res = c.sum(1,2)
        assert res.is_ok
        res = c.substract(1,2)
        assert res.is_ok
        res = c.multiply(1,2)
        assert res.is_ok
        res = c.divide(1,2)
        assert res.is_ok

    

    # assert not lcm.runtime.is_distributed
    
@pytest.mark.asyncio
async def test_local_cm_none_runtime():
    lcm = AxoContextManager(runtime= None)
    assert not lcm.runtime 

@pytest.mark.asyncio
async def test_local_cm_local_runtime():
    with AxoContextManager.local() as lrt:
        assert not lrt.is_distributed
        current_rt = get_runtime()
        assert isinstance(current_rt,LocalRuntime)
@pytest.mark.asyncio
async def test_init_cm_none():
    with AxoContextManager(runtime=None) as drt:
        assert not drt.is_distributed

@pytest.mark.asyncio
async def test_stop_cm():
    with AxoContextManager(runtime=None) as drt:
        drt.stop()
        assert not drt.is_running
@pytest.mark.asyncio
async def test_stop_a_running_cm():
    with AxoContextManager.local() as drt:
        drt.stop()
        assert not drt.is_running
        # assert not drt.is_distributed
@pytest.mark.asyncio
async def test_distributed_cm(storage_service:StorageService):
    with AxoContextManager.distributed(endpoint_manager=DistributedEndpointManager(),storage_service=storage_service) as drt:
        assert drt.is_distributed

# @pytest.mark.asyncio
# async def test_local_cm():
    # lcm = AxoContextManager.local()
    # assert not lcm.runtime.storage_service



