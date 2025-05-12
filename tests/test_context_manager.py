import pytest 
from axo.contextmanager import ActiveXContextManager
from axo.endpoint.manager import DistributedEndpointManager
import asyncio
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

@pytest.mark.skip("")
@pytest.mark.asyncio
async def test_context_manager_distributed():
    dem = DistributedEndpointManager()
    dem.add_endpoint(
        endpoint_id="activex-endpoint-0",
        hostname="localhost",
        protocol="tcp",
        req_res_port=16667,
        pubsub_port=16666
    )
    res = ActiveXContextManager.distributed(
        endpoint_manager=dem
    )
    calc = Calc(
        id = "CALCULADORA_ID",
        axo_bucket_id = "cubeta",
        axo_key = "calculadora",
        axo_endpoint_id = "activex-endpoint-0"
    )
    res = await calc.persistify()
    print(res)
    # print(res)


# @pytest.mark.skip("")
@pytest.mark.asyncio
async def test_context_manager_distributed_get_and_execute():
    dem = DistributedEndpointManager()
    dem.add_endpoint(
        endpoint_id="activex-endpoint-0",
        hostname="localhost",
        protocol="tcp",
        req_res_port=16667,
        pubsub_port=16666
    )
    acm = ActiveXContextManager.distributed(
        endpoint_manager=dem
    )
    res = await Axo.get_by_key(
        bucket_id="cubeta",
        key="calculadora"
    )

    print(res)
    assert res.is_ok
    calc= res.unwrap()
    # print("CALC_DICT",calc.__dict__)
    # print("CALC",calc)
    result = calc.sum(1,1)
    assert result.is_ok
    result = calc.substract(1,1)
    assert result.is_ok
    print(result)
    # assert result == 2

@pytest.mark.skip("")
@pytest.mark.asyncio
async def test_context_manager_distributed_get_parts():
    dem = DistributedEndpointManager()
    dem.add_endpoint(
        endpoint_id="activex-endpoint-0",
        hostname="localhost",
        protocol="tcp",
        req_res_port=16667,
        pubsub_port=16666
    )
    acm = ActiveXContextManager.distributed(
        endpoint_manager=dem
    )
    res = await acm.runtime.get_active_object(
        bucket_id="b_source",
        key="calculadora"
    )
    assert res.is_ok
    calc= res.unwrap()
    bytess = calc.to_bytes()
    # print(bytess)
    parts_result = Axo.get_parts(raw_obj=bytess)
    # print(parts_result)
    assert parts_result.is_ok
    attrs, methods, class_def, class_source = parts_result.unwrap()
    assert class_def == Calc
    # print(methods)
    # print(attrs)

    # result = calc.sum(1,1)
    # assert result == 2
    # print(res)
    


