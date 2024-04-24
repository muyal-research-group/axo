from abc import ABC,abstractmethod
import logging
from activex import ActiveX
from mictlanx.v4.client import Client
from mictlanx.utils.index import Utils
from mictlanx.v4.interfaces.responses import PutResponse,GetBytesResponse
from option import Result,Err,Ok
from nanoid import generate as nanoid
import time as T
import string

import os
logger = logging.getLogger(__name__)
console_handler = logging.StreamHandler()
logger.addHandler(console_handler)
logger.setLevel(logging.DEBUG)

class StorageService(ABC):
    def __init__(self,storage_service_id:str):
        self.storage_service_id = storage_service_id
    
    @abstractmethod
    def put(self,obj:ActiveX,key:str="")->Result[str,Exception]:
        pass
    @abstractmethod
    def get(self,key:str)->Result[ActiveX, Exception]:
        pass

class LocalStorageService(StorageService):
    def __init__(self, storage_service_id: str):
        super().__init__(storage_service_id)
    def put(self, obj: ActiveX, key: str = "") -> Result[str, Exception]:
        start_time = T.time()
        key = nanoid(alphabet=string.digits+string.ascii_lowercase) if not key else key
        os.makedirs(obj._acx_metadata.path,exist_ok=True)
        path = "{}/{}".format(obj._acx_metadata.path,key)
        size = 0
        with open(path,"wb") as f:
            data = obj.to_bytes()
            size = len(data)
            f.write(data)
        response_time = T.time() - start_time
        logger.debug("PUT.DATA %s %s %s %s",path,key,size,response_time)
        return Err(Exception("Not implemented yet."))
    
    def get(self, key: str)->Result[ActiveX, Exception]:
        base_path = os.environ.get("ACTIVEX_LOCAL_PATH","/activex")
        path = "{}/{}".format(base_path,key)
        logger.debug("GET %s %s", key, path)
        with open(path,"rb") as f :
            data = f.read()
            return ActiveX.from_bytes(raw_obj=data)
        # return Err(Exception("Not implemented yet."))

class MictlanXStorageService(StorageService):
    def __init__(self):
        super().__init__(storage_service_id="MictlanX")
        NODE_ID = os.environ.get("NODE_ID","activex")
        BUCKET_ID       = os.environ.get("MICTLANX_BUCKET_ID",NODE_ID)
        routers_str = os.environ.get("MICTLANX_ROUTERS","mictlanx-router-0:localhost:60666")
        MICTLANX_PROTOCOL  = os.environ.get("MICTLANX_PROTOCOL","http")
        # Parse the routers_str to a Router object
        routers     = list(Utils.routers_from_str(routers_str,protocol=MICTLANX_PROTOCOL))

        self.client =  Client(
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
    def put(self,obj:ActiveX,key:str="")->Result[str,Exception]:
        tags = {
                **obj._acx_metadata.to_json_with_string_values()
        }
        tags.pop("pivot_storage_node")
        tags.pop("replica_nodes")
        future= self.client.put(
            value= obj.to_bytes(),
            key=key,
            tags=tags

        )
        result:Result[PutResponse,Exception]  = future.result()
        if result.is_ok:
            response = result.unwrap()
            return Ok(response.key)
        else :
            return Err(result.unwrap_err())
        

    def get(self,key:str)->Result[ActiveX,Exception]:
        result:Result[GetBytesResponse,Exception]= self.client.get(key=key).result()
        if result.is_err:
            return Err(Exception("{} not found".format(key)))
        response:GetBytesResponse = result.unwrap()
        return Ok(ActiveX.from_bytes(response.value))
