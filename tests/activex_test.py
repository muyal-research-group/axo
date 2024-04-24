from typing import Dict
import unittest
from activex import ActiveX,activex
from activex.handler.handler import ActiveXHandler 
import cloudpickle as CP
from mictlanx.v4.client import Client
from mictlanx.utils.index import Utils
from mictlanx.v4.interfaces.responses import PutResponse
from option import Result
import numpy as np
import numpy.typing as npt
import os
import logging

logger          = logging.getLogger(__name__)
formatter       = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.setLevel(logging.DEBUG)


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




class Dog(ActiveX):
    def __init__(self):
        self.dog_name = "PERRITO"
    @activex
    def bark(self):
        print("WOOF")

class Calculator(ActiveX):
    def __init__(self):
        self.example_id = "02"
    @activex
    def add(self,x:float,y:float):
        return x + y
    @activex
    def substract(self,x:float,y:float):
        return x - y

    @activex
    def multiply(self,x:float,y:float):
        return x * y
    @activex
    def divide(self,x:float,y:float):
        if y == 0:
            raise ZeroDivisionError()
        return x / y
    @activex
    def add_vectors(self,x:npt.NDArray,y:npt.NDArray):
        res = x+y
        return res
    


class ActiveXTest(unittest.TestCase):
    
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
    # @unittest.skip("")
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
    def test_persistify_obj(self):
        _       = ActiveXHandler.distributed()
        dog_obj = Dog()
        dog_obj.bark()
        dog_obj.persistify()

        logger.debug("Saved %s successfully", dog_obj._acx_metadata.id)
        return self.assertTrue(dog_obj._acx_remote)

    @unittest.skip("")
    def test_get_obj(self):
        handler  = ActiveXHandler.distributed()
        obj:Dog = Dog.get_by_key(key="sq6eqpn3gn4lidxr8mayj")
        obj.bark()
        return self.assertIsInstance(obj, ActiveX)


    @unittest.skip("")
    def test_serialize(self):
        dog_obj = Dog()
        dog_obj.bark()
        dog_obj_bytes = dog_obj.to_bytes()
        future = c.put(
            key= dog_obj.object_id, # if empty it uses the sha256(obj_bytes)
            value=dog_obj_bytes,
            tags={
                "more":"metadata",
                "about":"the object"
            }
        )
        response:Result[PutResponse,Exception]= future.result()
        if response.is_ok:
            print("ActiveX object was save successfully...")
        else:
            print("ActiveX object was not save..")

        # pf = PerroFeo() 


if __name__ =="__main__":
    unittest.main()