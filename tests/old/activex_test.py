from typing import Dict
import unittest
from axo import Axo
from axo.contextmanager.contextmanager import AxoContextManager
from axo.endpoint import EndpointManagerX,EndpointX
from axo.runtime.local import LocalRuntime
from axo.storage.data import MictlanXStorageService
from mictlanx.logger.tezcanalyticx.tezcanalyticx import TezcanalyticXParams

from option import Result,Ok,Err
import numpy as np
import time as T
import os
import logging
from .common import Dog,Calculator,Cipher
from dotenv import load_dotenv
from option import Some

ENV_FILE_PATH = os.environ.get("ENV_FILE_PATH",-1)
if not ENV_FILE_PATH == -1:
    load_dotenv(ENV_FILE_PATH)






logger          = logging.getLogger(__name__)
formatter       = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.setLevel(logging.DEBUG)
#    key = get_random_bytes(16)  # 16 bytes key for AES-128
    # aes_cipher = AESCipher(key)

AXO_ENDPOINT_ID           = os.environ.get("AXO_ENDPOINT_ID","activex-endpoint-0")
AXO_ENDPOINT_PROTOCOL     = os.environ.get("AXO_ENDPOINT_PROTOCOL","tcp")
AXO_ENDPOINT_HOSTNAME     = os.environ.get("AXO_ENDPOINT_HOSTNAME","localhost")
AXO_ENDPOINT_PUBSUB_PORT  = int(os.environ.get("AXO_ENDPOINT_PUBSUB_PORT","16000"))
AXO_ENDPOINT_REQ_RES_PORT = int(os.environ.get("AXO_ENDPOINT_REQ_RES_PORT","16667"))

# NODE_ID = os.environ.get("NODE_ID","activex")
# BUCKET_ID       = os.environ.get("MICTLANX_BUCKET_ID",NODE_ID)
# routers_str = os.environ.get("MICTLANX_ROUTERS","mictlanx-router-0:localhost:60666")
# OBSERVATORY_INDEX  = int(os.environ.get("OBSERVATORY_INDEX",0))
# MICTLANX_PROTOCOL  = os.environ.get("MICTLANX_PROTOCOL","http")
# OUTPUT_PATH:str    = os.environ.get("OUTPUT_PATH","/test/risk_calculator/sink")
# # Parse the peers_str to a Peer object









class ActiveXTest(unittest.TestCase):
    
    @unittest.skip("")
    def test_hybrid_context(self):
        axcm = AxoContextManager(
            runtime= LocalRuntime(
                storage_service= Some(MictlanXStorageService(
                    log_path="/log",
                ))
            )
        )
        axcm.runtime.storage_service.put_bytes(
            bucket_id ="test",
            key="test_test",
            data=b"HGOLLLAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA!!"
        )
        print(axcm)
    @unittest.skip("")
    def test_local_context_layer_cipher(self):
        axcm    = AxoContextManager.local()
        cipher  = Cipher(security_level=128)
        sk      = cipher.key_gen()
        
        res =  cipher.encrypt(
            plaintext=b"Secret data",
            key= sk
        )

        logger.debug({
            "method":"encrypt",
            "result":res
        })

        res = cipher.decrypt(
            key= sk,
            ciphertext=res.unwrap()
        )
        logger.debug({
            "method":"decrypt",
            "result":res
        })
        cipher.persistify()

    @unittest.skip("")
    def test_distributed_context_layer_cipher(self):
        endpoint_manager = EndpointManagerX()
        endpoint_manager.add_endpoint(
            endpoint_id=AXO_ENDPOINT_ID,
            protocol=AXO_ENDPOINT_PROTOCOL,
            hostname=AXO_ENDPOINT_HOSTNAME,
            req_res_port=AXO_ENDPOINT_REQ_RES_PORT,
            pubsub_port=AXO_ENDPOINT_PUBSUB_PORT
        )

        ax     = AxoContextManager.distributed(
            endpoint_manager=endpoint_manager,
            tezcanalyticx_params= Some(
                TezcanalyticXParams(
                    port= 45000,
                    hostname="localhost",
                    protocol="http",
                )
            )
        )
        while True:
            cipher  = Cipher(security_level=128)
            sk = cipher.key_gen()
            cipher.persistify()
            f = open("/source/01.pdf","rb")
            plaintext = f.read()
            f.close()
            res =  cipher.encrypt(
                plaintext=plaintext,
                key= sk,
                sink_bucket_id = cipher.get_sink_bucket_id()
            )
            del plaintext

            logger.debug({
                "method":"encrypt",
                # "result":res
            })
            res = cipher.decrypt(
                key= sk,
                ciphertext=res.unwrap(),
                sink_bucket_id = cipher.get_sink_bucket_id()
            )
            for i in range(np.random.randint(0,100)):
                x = Axo.get_by_key(bucket_id=cipher.get_sink_bucket_id(), key= cipher.get_sink_key())
                print(x)
            logger.debug({
                "method":"decrypt",
                # "result":res
            })
            T.sleep(2)



    @unittest.skip("")
    def test_distributed_context_layer_get_cipher(self):
        endpoint_manager = EndpointManagerX()
        endpoint_manager.add_endpoint(
            endpoint_id=AXO_ENDPOINT_ID,
            protocol=AXO_ENDPOINT_PROTOCOL,
            hostname=AXO_ENDPOINT_HOSTNAME,
            req_res_port=AXO_ENDPOINT_REQ_RES_PORT,
            pubsub_port=AXO_ENDPOINT_PUBSUB_PORT
        )
        axcm           = AxoContextManager.distributed(endpoint_manager=endpoint_manager)
        bucket_id      = "39xoc05wyy0nunof"
        key            = "x3fxeow4f0wje3k5"
        
        get_obj_result = Axo.get_by_key(bucket_id=bucket_id, key=key)
        
        if get_obj_result.is_ok:
            obj:Cipher = get_obj_result.unwrap()

            logger.debug({
                "event":"GET.BY.KEY",
                "bucket_id":bucket_id,
                "key":key,
                "obj":str(obj)
            })
            res        = obj.encrypt(plaintext=b"SECRETMETSSAGE", key=obj.key_gen(),sink_bucket_id = obj.get_sink_bucket_id())
            logger.debug({
                "method":"encryp",
                "result":res
            })
        else:
            logger.error({
                "msg":str(get_obj_result.unwrap_err())
            })


    # 
    @unittest.skip("")
    def test_local_context_layer(self):
        ahx = AxoContextManager.local()
        dog_obj = Dog(name="Rex")
        res = dog_obj.bark(name = "Rory")

        logger.debug({
            "result":res
        })
        dog_obj.persistify()


    @unittest.skip("")
    def test_distributed_context_layer(self):

        endpoint_manager = EndpointManagerX()
        endpoint_manager.add_endpoint(
            endpoint_id=AXO_ENDPOINT_ID,
            protocol=AXO_ENDPOINT_PROTOCOL,
            hostname=AXO_ENDPOINT_HOSTNAME,
            req_res_port=AXO_ENDPOINT_REQ_RES_PORT,
            pubsub_port=AXO_ENDPOINT_PUBSUB_PORT
        )

        ax     = AxoContextManager.distributed(endpoint_manager=endpoint_manager)
        dog    = Dog(name="Rex-0")
        result = dog.persistify()
        res = dog.bark(
            name           = "Jesus",
            dependencies   = ["numpy"],
            sink_bucket_id = dog.get_sink_bucket_id(),
        )
        logger.debug({
            "result":res,
        })

        # print("BNARK_RES",res)
        print("_"*50)
    @unittest.skip("")
    def test_distributed_context_layer_get(self):

        endpoint_manager = EndpointManagerX()
        endpoint_manager.add_endpoint(
            endpoint_id=AXO_ENDPOINT_ID,
            protocol=AXO_ENDPOINT_PROTOCOL,
            hostname=AXO_ENDPOINT_HOSTNAME,
            req_res_port=AXO_ENDPOINT_REQ_RES_PORT,
            pubsub_port=AXO_ENDPOINT_PUBSUB_PORT
        )

        ax     = AxoContextManager.distributed(endpoint_manager=endpoint_manager)
        bucket_id = "hb2qlc4e1mrmxem3"
        key = "zlqp1xcjivjmfj83"
        get_obj_result = Axo.get_by_key(bucket_id=bucket_id,key=key)
        if get_obj_result.is_ok:
            obj:Dog = get_obj_result.unwrap()
            res = obj.bark(name="Ignacio",sink_bucket_id = bucket_id)
            logger.debug({
                "result":res
            })
        else:
            logger.error({"msg":str(get_obj_result.unwrap_err())})




    @unittest.skip("")
    def test_distributed_context_layer(self):

        endpoint_manager = EndpointManagerX()
        endpoint_manager.add_endpoint(
            endpoint_id=AXO_ENDPOINT_ID,
            protocol=AXO_ENDPOINT_PROTOCOL,
            hostname=AXO_ENDPOINT_HOSTNAME,
            req_res_port=AXO_ENDPOINT_REQ_RES_PORT,
            pubsub_port=AXO_ENDPOINT_PUBSUB_PORT
        )

        ax     = AxoContextManager.distributed(endpoint_manager=endpoint_manager)
        dog    = Dog(name="Rex-0")
        result = dog.persistify()
        res = dog.bark(
            name           = "Jesus",
            dependencies   = ["numpy"],
            sink_bucket_id = dog.get_sink_bucket_id(),
        )
        logger.debug({
            "result":res,
        })

        # print("BNARK_RES",res)
        print("_"*50)
        




    @unittest.skip("")
    def test_distributed_context_layer2(self):
        endpoint_manager = EndpointManagerX()

        endpoint_manager.add_endpoint(
            endpoint_id="activex-endpoint-1",
            protocol=AXO_ENDPOINT_PROTOCOL,
            hostname=AXO_ENDPOINT_HOSTNAME,
            req_res_port=61771,
            pubsub_port=AXO_ENDPOINT_PUBSUB_PORT
        )

        ax               = AxoContextManager.distributed(endpoint_manager=endpoint_manager)
        i = 0 
        dog = Dog(name="Rex-{}".format(i))
        res = dog.persistify()
        print(res)
    @unittest.skip("")
    def test_get_object(self):
       
        endpoint_manager = EndpointManagerX(endpoints={
            AXO_ENDPOINT_ID:EndpointX(
                endpoint_id=AXO_ENDPOINT_ID,
                protocol=AXO_ENDPOINT_PROTOCOL,
                hostname=AXO_ENDPOINT_HOSTNAME,
                req_res_port=AXO_ENDPOINT_REQ_RES_PORT,
                pubsub_port=AXO_ENDPOINT_PUBSUB_PORT
            )
        })
        _ = AxoContextManager.distributed(endpoint_manager=endpoint_manager)
        result = Axo.get_by_key("kvs74ah19gvr3r4d")
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
    def test_calculator(self):
        
        endpoint_manager = EndpointManagerX(endpoints={
            AXO_ENDPOINT_ID:EndpointX(
                endpoint_id=AXO_ENDPOINT_ID,
                protocol=AXO_ENDPOINT_PROTOCOL,
                hostname=AXO_ENDPOINT_HOSTNAME,
                req_res_port=AXO_ENDPOINT_REQ_RES_PORT,
                pubsub_port=AXO_ENDPOINT_PUBSUB_PORT
            )
        })
        _ = AxoContextManager.distributed(endpoint_manager=endpoint_manager)
        calc = Calculator()
        x,y = 1,5
        add_result = calc.add(1,2)
        logger.debug("Calculator add {} + {} = {}".format(x,y,add_result))
        calc.persistify(key="mycalculatorobject")
    @unittest.skip("")
    def test_get_calculator(self):
        endpoint_manager = EndpointManagerX(endpoints={
            [AXO_ENDPOINT_ID]:EndpointX(
                endpoint_id=AXO_ENDPOINT_ID,
                protocol=AXO_ENDPOINT_PROTOCOL,
                hostname=AXO_ENDPOINT_HOSTNAME,
                req_res_port=AXO_ENDPOINT_REQ_RES_PORT,
                pubsub_port=AXO_ENDPOINT_PUBSUB_PORT
            )
        })
        _ = AxoContextManager.distributed(endpoint_manager=endpoint_manager)
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

        endpoint_manager = EndpointManagerX(endpoints={
            AXO_ENDPOINT_ID:EndpointX(
                endpoint_id=AXO_ENDPOINT_ID,
                protocol=AXO_ENDPOINT_PROTOCOL,
                hostname=AXO_ENDPOINT_HOSTNAME,
                req_res_port=AXO_ENDPOINT_REQ_RES_PORT,
                pubsub_port=AXO_ENDPOINT_PUBSUB_PORT
            )
        })
        _ = AxoContextManager.distributed(endpoint_manager=endpoint_manager)
        # handler  = ActiveXContextManager.distributed()
        obj:Dog = Dog.get_by_key(key="86h6tx5dkbkp579l")
        obj.bark()
        return self.assertIsInstance(obj, Axo)

    @unittest.skip("")
    def test_distributed_context_cipher(self):

        endpoint_manager = EndpointManagerX(endpoints={
            AXO_ENDPOINT_ID:EndpointX(
                endpoint_id=AXO_ENDPOINT_ID,
                protocol=AXO_ENDPOINT_PROTOCOL,
                hostname=AXO_ENDPOINT_HOSTNAME,
                req_res_port=AXO_ENDPOINT_REQ_RES_PORT,
                pubsub_port=AXO_ENDPOINT_PUBSUB_PORT
            )
        })
      

        # get_random_bytes(16)  # 16 bytes key for AES-128

        cipher_instance = Cipher()
        key = cipher_instance.key_gen()

        cipher_instance.persistify()
        
        res = cipher_instance.encrypt(
            key        = key,
            plaintext  = b"Hola DANTE y ARMANDO",
            # args y kwargs
            sink_bucket_id  = cipher_instance.bucket_id,
            sink_key = "myresult1000"
        )

        print("CIPHER_ENCRYPT_RESPONSE",res)

        logger.debug("Saved %s successfully", cipher_instance._acx_metadata.id)
        return self.assertTrue(cipher_instance._acx_remote)
    @unittest.skip("")
    def test_distributed_context(self):

        endpoint_manager = EndpointManagerX(endpoints={
            AXO_ENDPOINT_ID:EndpointX(
                endpoint_id=AXO_ENDPOINT_ID,
                protocol=AXO_ENDPOINT_PROTOCOL,
                hostname=AXO_ENDPOINT_HOSTNAME,
                req_res_port=AXO_ENDPOINT_REQ_RES_PORT,
                pubsub_port=AXO_ENDPOINT_PUBSUB_PORT
            )
        })
        _ = AxoContextManager.distributed(endpoint_manager=endpoint_manager)

        dog_obj = Dog()
        # First make persistent
        dog_obj.persistify()
        res = dog_obj.bark(name="WOOF WOOF EN EL PARQUE")
        print("DOG_RESPONSE",res)

        logger.debug("Saved %s successfully", dog_obj._acx_metadata.id)
        return self.assertTrue(dog_obj._acx_remote)




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
   

if __name__ =="__main__":
    unittest.main()