from typing import Dict
import unittest
from activex import ActiveX,activex_method
from activex.handler.handler import ActiveXHandler 
import cloudpickle as CP
from mictlanx.v4.client import Client
from mictlanx.utils.index import Utils
from mictlanx.v4.interfaces.responses import PutResponse
from concurrent.futures import ThreadPoolExecutor
from option import Result,Ok,Err
import numpy as np
import numpy.typing as npt
import time as T
import os
import logging
from .common import Dog,Calculator
from dotenv import load_dotenv

ENV_FILE_PATH = os.environ.get("ENV_FILE_PATH",-1)
if not ENV_FILE_PATH == -1:
    load_dotenv(ENV_FILE_PATH)






logger          = logging.getLogger(__name__)
formatter       = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.setLevel(logging.DEBUG)


ACTIVEX_DISTRIBUTED_PROTOCOL = os.environ.get("ACTIVEX_DISTRIBUTED_PROTOCOL","tcp")
ACTIVEX_DISTRIBUTED_HOSTNAME = os.environ.get("ACTIVEX_DISTRIBUTED_HOSTNAME","localhost")
ACTIVEX_DISTRIBUTED_PORT = int(os.environ.get("ACTIVEX_DISTRIBUTED_PORT","16000"))
ACTIVEX_DISTRIBUTED_REQ_RES_PORT = int(os.environ.get("ACTIVEX_DISTRIBUTED_REQ_RES_PORT","16667"))

NODE_ID = os.environ.get("NODE_ID","activex")
BUCKET_ID       = os.environ.get("MICTLANX_BUCKET_ID",NODE_ID)
routers_str = os.environ.get("MICTLANX_ROUTERS","mictlanx-router-0:localhost:60666")
OBSERVATORY_INDEX  = int(os.environ.get("OBSERVATORY_INDEX",0))
MICTLANX_PROTOCOL  = os.environ.get("MICTLANX_PROTOCOL","http")
OUTPUT_PATH:str    = os.environ.get("OUTPUT_PATH","/test/risk_calculator/sink")
# Parse the peers_str to a Peer object

routers     = list(Utils.routers_from_str(routers_str,protocol=MICTLANX_PROTOCOL))
c = Client(
    # Unique identifier of the client
    client_id   = os.environ.get("MICTLANX_CLIENT_ID","client-0"),
    # Storage peers
    routers     = routers,
    # Number of threads to perform I/O operations
    max_workers = int(os.environ.get("MICTLANX_MAX_WORKERS","2")),
    # This parameters are optionals only set to True if you want to see some basic metrics ( this options increase little bit the overhead please take into account).
    debug       = True,
    log_output_path= os.environ.get("MICTLANX_LOG_OUTPUT_PATH","./log"),
    bucket_id=BUCKET_ID
)







class ActiveXTest(unittest.TestCase):
    

    @staticmethod
    def create_dog()->Result[str,Exception]:
        dog = Dog()
        res = dog.persistify()
        dog.bark()
        return res
    @staticmethod
    def get_dog(key:str)->Result[Dog,Exception]:
        dog_res = Dog.get_by_key(key)
        logger.debug("GET_DOG_RESPONSE {}".format(dog_res))
        if dog_res.is_ok:
            dog = dog_res.unwrap()
            me_res = dog.bark()
            logger.debug("METHOD_EXECUTION {}".format(me_res))
            return Ok(dog)
        else:
            return Err(Exception("{} not found".format(key)))

    @unittest.skip("")
    def test_persist(self):
        ax = ActiveXHandler.distributed(
            protocol = "tcp",
            hostname = "localhost",
            port     = 16668,
            req_res_port= 16667
        )
        dog = Dog()
        dog.persistify()
        
    @unittest.skip("")
    def test_get_object(self):
        ax = ActiveXHandler.distributed(
            protocol = "tcp",
            hostname = "localhost",
            port     = 16668,
            req_res_port= 16667
        )
        result = ActiveX.get_by_key("kvs74ah19gvr3r4d")
        if result.is_ok:
            obj = result.unwrap()
            fn = getattr(obj, "bark")
            fn_res = fn("UPDATED_ARG")
            logger.debug({
                "event":"TASK.RESPONSE",
                "res":fn_res
            })
            fn = getattr(obj, "get_name")
            fn_res = fn("UPDATED_NAME_FROM_CLIENT")
            logger.debug({
                "event":"TASK.RESPONSE",
                "res":fn_res
            })
            fn = getattr(obj, "get_df")
            fn_res = fn()
            logger.debug({
                "event":"TASK.RESPONSE",
                "res":fn_res
            })
        

    @unittest.skip("")
    def test_method_execution(self):
        ax = ActiveXHandler.distributed(
            protocol = "tcp",
            hostname = "localhost",
            port     = 16668,
            req_res_port= 16667
        )
        d = Dog()
        res = d.persistify()
        print("RESPONSE",res)
        T.sleep(10)
        d.bark()
        print("METHOD EXECUTION", d)

    @unittest.skip("")
    def  test_middleware(self):
        ax = ActiveXHandler.distributed(
            protocol = "tcp",
            hostname = "127.0.0.1",
            port     = 16666
        )
        
        with ThreadPoolExecutor(max_workers=4) as tp:
            idle_time = 10
            while True:
                put_iat = 2
                create_res = ActiveXTest.create_dog()
                if create_res.is_ok:
                    key = create_res.unwrap()
                else:
                    print("PUT_ERROR")
                    continue
                T.sleep(put_iat)
                logger.debug("CREATE_RES {}".format(create_res))
                gets_counter = np.random.randint(low=1, high=10)
                logger.debug("GETS {} {}".format(key,gets_counter))
                # print("GE")
                for i in range(gets_counter):
                    get_iat = 1 
                    get_res = ActiveXTest.get_dog(key=key)
                    logger.debug("GET_RES {}".format(get_res))
                    T.sleep(get_iat)

                T.sleep(idle_time)
        # print("Here")
        # while True:
        #     print("BERting")
        #     ax.runtime.metadata_service.put(key="A", metadata=dog._acx_metadata)
        #     T.sleep(2)



    
    @unittest.skip("")
    def test_rpc(self):
        dog_obj = Dog()
        fx = getattr(dog_obj, "bark")
        fx_bytes = CP.dumps(fx)
        print("FUNCTION",fx)
        print("FUNCTION_BYTES",fx_bytes)
        fy = CP.loads(fx_bytes)
        print("RESULTS",fx(),fy())

    

    @unittest.skip("")
    def test_calculator(self):
        _       = ActiveXHandler.distributed()
        calc = Calculator()
        x,y = 1,5
        add_result = calc.add(1,2)
        logger.debug("Calculator add {} + {} = {}".format(x,y,add_result))
        calc.persistify(key="mycalculatorobject")
    @unittest.skip("")
    def test_get_calculator(self):
        _       = ActiveXHandler.distributed()
        calc:Calculator = Calculator.get_by_key("mycalculatorobject").unwrap()
        print("CALCUILATOR",calc)
        x,y = 1,5
        add_result = calc.add(1,2)
        xs,ys = np.array([1,2]),np.array([2,4])
        vec_result = calc.add_vectors(xs,ys)
        logger.debug("Calculator add {} + {} = {}".format(x,y,add_result))
        logger.debug("Calculator vector add {} + {} = {}".format(xs,ys,vec_result))

    @unittest.skip("")
    def test_get_obj(self):
        handler  = ActiveXHandler.distributed()
        obj:Dog = Dog.get_by_key(key="86h6tx5dkbkp579l")
        obj.bark()
        return self.assertIsInstance(obj, ActiveX)

    # @unittest.skip("")
    def test_persistify_obj(self):
        _       = ActiveXHandler.distributed(
            hostname=ACTIVEX_DISTRIBUTED_HOSTNAME,
            protocol=ACTIVEX_DISTRIBUTED_PROTOCOL,
            port=ACTIVEX_DISTRIBUTED_PORT,
            req_res_port=ACTIVEX_DISTRIBUTED_REQ_RES_PORT
        )
        dog_obj = Dog()
        dog_obj.bark()
        dog_obj.persistify()

        logger.debug("Saved %s successfully", dog_obj._acx_metadata.id)
        return self.assertTrue(dog_obj._acx_remote)


    """
    Create a dog instance and execute the bark method
    """
    @unittest.skip("")
    def test_serialize(self):
        ahx = ActiveXHandler.local()
        dog_obj = Dog()
        dog_obj.bark()
        dog_obj_bytes = dog_obj.to_bytes()
        future = c.put(
            key= dog_obj._acx_metadata.id , # if empty it uses the sha256(obj_bytes)
            value=dog_obj_bytes,
            tags={
                "more":"metadata",
                "about":"the object"
            }
        )
        response:Result[PutResponse,Exception]= future.result()
        if response.is_ok:
            logger.debug({
                "msg":"ActiveX object saved successfully", 
                "key":dog_obj._acx_metadata.id
            })
        else:
            logger.debug("ActiveX object was not save..")
        return self.assertTrue(response.is_ok)

        # pf = PerroFeo() 


if __name__ =="__main__":
    unittest.main()