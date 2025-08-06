import pytest 
import time as T
from axo.contextmanager import AxoContextManager
from axo.endpoint.manager import DistributedEndpointManager
from axo.storage.data import MictlanXStorageService
from axo import Axo,axo_method
from option import Some
from .objects.scenario1 import *
import numpy as np

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
    res = AxoContextManager.distributed(
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


@pytest.fixture()
def dem():
    dem = DistributedEndpointManager()
    dem.add_endpoint(
        endpoint_id="axo-endpoint-0",
        hostname="localhost",
        protocol="tcp",
        req_res_port=16667,
        pubsub_port=16666
    )
    return dem


async def benchmark_ao(ObjectClass, input_data, name, N=31, endpoint="axo-endpoint-0"):
    print(f"\n🔍 Benchmarking {name} with N={N}...")
    rts = []
    t_global = T.time()

    # Instantiate and deploy AO
    ao = ObjectClass(axo_endpoint_id=endpoint)
    await ao.persistify()

    for i in range(N):
        t_local = T.time()
        res = ao.run(input_data)
        rt = T.time() - t_local
        rts.append(rt)

    t_total = T.time() - t_global
    rts = np.array(rts)

    # Results
    print(f"{name} = Iterations={N}, param={input_data if isinstance(input_data, int) else '...'}")
    print(f"  AVG.RT   = {np.mean(rts):.6f} sec")
    print(f"  MEDIAN   = {np.median(rts):.6f} sec")
    print(f"  STD      = {np.std(rts):.6f} sec")
    print(f"  RT TOTAL = {t_total:.6f} sec\n")


class Microservice1(Axo):
    
    def encrypt(self,data):
        return data

class Microservicio2(Axo):
    
    def decrypt(self,encrypted_data):
        return encrypted_data



@pytest.mark.asyncio
async def test_scenario1_full(dem: DistributedEndpointManager):
    
    acm = AxoContextManager.distributed(
        endpoint_manager = dem,
        storage_service  = Some(
            MictlanXStorageService(protocol="http")
        )
    )

    

    # await benchmark_ao(Object1, 42, "Object 1")
    # await benchmark_ao(Object2, {"numbers": list(range(1000)), "target": 999}, "Object 2")
    # await benchmark_ao(Object3, 1000, "Object 3")
    # await benchmark_ao(Object4, {"numbers": list(reversed(range(1000)))}, "Object 4")
    # await benchmark_ao(Object5, {"points": list(range(100))}, "Object 5")
    # await benchmark_ao(Object6, {
    #     "A": [[1]*10 for _ in range(10)],
    #     "B": [[1]*10 for _ in range(10)]
    # }, "Object 6")
    # await benchmark_ao(Object7, list(range(100)), "Object 7")
    await benchmark_ao(Object8, {"cities": list(range(1000))}, "Object 8")  # Warning: factorial growth

@pytest.mark.skip("")
@pytest.mark.asyncio
async def test_scenario1(dem: DistributedEndpointManager):
    acm = AxoContextManager.distributed(
        endpoint_manager = dem,
        storage_service  = Some(
            MictlanXStorageService(
                protocol="http"
            )
        )
    )
    N = 31 
    n = 1000
    t1_global = T.time()
    rts = []
    obj1 = Object4(axo_endpoint_id = "axo-endpoint-0")
    res = await obj1.persistify()
    for i in range(N):
        t1_local = T.time()
        res = obj1.run(n)
        print("RES",res)
        rt_local = T.time() -t1_local
        rts.append(rt_local)
    rt_global = T.time() - t1_global
    rts = np.array(rts)
    print(f"OBJECT 4 = Iterations={N}, param={n} AVG.RT {np.mean(rts)} MEDIAN = {np.median(rts)} STD={np.std(rts)} RT={rt_global}")
        


        # print("")
    
    # obj2 = Object2(axo_endpoint_id = "axo-endpoint-0")
    # res = await obj1.persistify()
    # print("PERSIST", res)
    # res = obj1.run(10)
    # print(res)

    # 0.18493127822875977
    # RT=0.17685985565185547
    # RT=0.17412471771240234
    # print("HERE")


@pytest.mark.skip("")
@pytest.mark.asyncio
async def test_context_manager_distributed_get_and_execute(dem: DistributedEndpointManager):
    acm = AxoContextManager.distributed(
        endpoint_manager=dem
    )
    res = await Axo.get_by_key(
        bucket_id="cubeta",
        key="calculadora"
    )

    print(res)
    assert res.is_ok
    calc= res.unwrap()
    result = calc.sum(1,1)
    assert result.is_ok
    result = calc.substract(1,1)
    assert result.is_ok
    print(result)

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
    acm = AxoContextManager.distributed(
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
    


