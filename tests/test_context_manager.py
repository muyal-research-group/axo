import pytest 
import time as T
from axo.contextmanager import AxoContextManager
from axo.runtime import get_runtime,set_runtime
from axo.runtime.local import LocalRuntime
from axo.runtime.distributed import DistributedRuntime
from axo.endpoint.manager import DistributedEndpointManager
# from axo.contextmanager
from axo import Axo,axo_method

class Calc(Axo):
    
    def __init__(self,id:str,**kwargs):
        super().__init__(**kwargs)
        self.id=id
        self.path = "/source/01.pdf"

    @axo_method
    def sum(x:int,y:int):
        return x+y
    
    @axo_method
    def substract(x:int,y:int):
        return x-y
    @axo_method
    def multiply(x:int,y:int):
        return x*y
    @axo_method
    def divide(x:int,y:int):
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


@pytest.mark.asyncio
async def test_local_cm():
    lcm = AxoContextManager.local()
    assert not lcm.runtime.is_distributed
    
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
async def test_distributed_cm():
    with AxoContextManager.distributed(endpoint_manager=DistributedEndpointManager()) as drt:
        assert drt.is_distributed

# @pytest.mark.asyncio
# async def test_local_cm():
    # lcm = AxoContextManager.local()
    # assert not lcm.runtime.storage_service



