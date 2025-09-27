import asyncio
import time as T
from axo.contextmanager import AxoContextManager
from axo.endpoint.manager import DistributedEndpointManager
from axo.storage.services import MictlanXStorageService
from axo import Axo,axo_method
from option import Some
from .objects.scenario1 import *
import numpy as np


dem = DistributedEndpointManager()
dem.add_endpoint(
    endpoint_id="axo-endpoint-0",
    hostname="localhost",
    protocol="tcp",
    req_res_port=16667,
    pubsub_port=16666
)


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



async def test_scenario1_full(dem: DistributedEndpointManager):
    
    acm = AxoContextManager.distributed(
        endpoint_manager = dem,
        storage_service  = Some(
            MictlanXStorageService()
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
    await benchmark_ao(Object8, {"cities": list(range(10))}, "Object 8",N=1)  # Warning: factorial growth



if __name__ == "__main__":
    el = asyncio.get_event_loop()
    el.run_until_complete(test_scenario1_full(dem=dem) )