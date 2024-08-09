# Inject by the platform ......
import sys
sys.path.append("./common")
from common import Dog
# ___________________________________
import time as T
import json as J
import os
import scipy.stats as S
from concurrent.futures import ThreadPoolExecutor,as_completed
import unittest as UT
# Axo 
from activex import Axo
from activex.contextmanager.contextmanager import ActiveXContextManager
from activex.endpoint import XoloEndpointManager

AXO_ENDPOINT_ID           = os.environ.get("AXO_ENDPOINT_ID","activex-endpoint-0")
AXO_ENDPOINT_PROTOCOL     = os.environ.get("AXO_ENDPOINT_PROTOCOL","tcp")
AXO_ENDPOINT_HOSTNAME     = os.environ.get("AXO_ENDPOINT_HOSTNAME","localhost")
AXO_ENDPOINT_PUBSUB_PORT  = int(os.environ.get("AXO_ENDPOINT_PUBSUB_PORT","16000"))
AXO_ENDPOINT_REQ_RES_PORT = int(os.environ.get("AXO_ENDPOINT_REQ_RES_PORT","16667"))


    
class AxoBasics(UT.TestCase):
    
    @classmethod
    def setUpClass(cls):
        # 
        cls.AVG_INTERVAL_TIME = 1/10000000
        cls.exponential_dist  = S.expon(loc = cls.AVG_INTERVAL_TIME)
        cls.MAX_DOWNLOADS     = 100
        cls.MAX_CONCURRENCY   = 4
        cls.MAX_NUM_AOS       = 1000

        # Create an endpoint manager instance
        cls.endpoint_manager = XoloEndpointManager()

        # Add a new endpoint
        cls.endpoint_manager.add_endpoint(
            endpoint_id  = AXO_ENDPOINT_ID,
            protocol     = AXO_ENDPOINT_PROTOCOL,
            hostname     = AXO_ENDPOINT_HOSTNAME,
            req_res_port = AXO_ENDPOINT_REQ_RES_PORT,
            pubsub_port  = AXO_ENDPOINT_PUBSUB_PORT
        )

        # 
        cls.thread_pool = ThreadPoolExecutor(max_workers=cls.MAX_CONCURRENCY,thread_name_prefix="axo.thread")

    @classmethod
    def tearDownClass(cls):
        cls.thread_pool.shutdown(
            wait=False,
            cancel_futures=True
        )

    @UT.skip("")
    def test_create_ao(self):
        
        # Init the distributed context manager
        ax  = ActiveXContextManager.distributed(
            endpoint_manager=AxoBasics.endpoint_manager
        )

        # Create an instance of a Dog (Active Object)
        rex = Dog(name="Rex")

        #  
        res = rex.persistify()

        return self.assertTrue(res.is_ok)

    @UT.skip("")
    def test_create_aos(self):
        # Init the distributed context manager
        ax              = ActiveXContextManager.distributed(
            endpoint_manager=AxoBasics.endpoint_manager
        )
        # Create an instance of a Dog (Active Object)
        def __create_ao(i:int):
            rex = Dog(name="Rex-{}".format(i))
            res = rex.persistify()
            return res
        
        with AxoBasics.thread_pool as executor:
            futures = []
            for i in range(AxoBasics.MAX_NUM_AOS):
                fut = executor.submit(__create_ao,i)
                futures.append(fut)
                T.sleep(AxoBasics.exponential_dist.rvs())
            for fut in as_completed(futures):
                res = fut.result()
                print(res)

    @UT.skip("")
    def test_N_gets(self):
        # Init the distributed context manager
        ax              = ActiveXContextManager.distributed(
            endpoint_manager = AxoBasics.endpoint_manager
        )

        with open("./out/out.json","rb") as f:
            data = J.load(f)
            events = list(filter(lambda x:x["event"]=="PUT.CHUNKED" , data))
            with AxoBasics.thread_pool as executor:
                for e in events:
                    bucket_id:str = e["bucket_id"]
                    key:str       = e["key"]
                    if not key.endswith("_class_def"):
                        executor.submit(self.get_ao,bucket_id=bucket_id, key=key)
        

    def get_ao(self,bucket_id:str,key:str):
        max_downloads = int(S.uniform.rvs(loc = 1, scale= AxoBasics.MAX_DOWNLOADS))
        for i in range(max_downloads):
            res = Axo.get_by_key(bucket_id=bucket_id,key=key)
            T.sleep(AxoBasics.exponential_dist.rvs())
        print("GET {} {} {}".format(bucket_id,key,max_downloads))


if __name__ == "__main__":
    UT.main()