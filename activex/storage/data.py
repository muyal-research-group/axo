from abc import ABC,abstractmethod
import logging
from activex import ActiveX
from mictlanx.v4.client import Client
from mictlanx.utils.index import Utils
from mictlanx.v4.interfaces.responses import PutResponse,GetBytesResponse
from option import Result,Err,Ok
from nanoid import generate as nanoid
from typing import Dict
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
    def put(self,obj:ActiveX,bucket_id:str="",key:str="")->Result[str,Exception]:
        pass
    @abstractmethod
    def get(self,key:str,bucket_id:str="")->Result[ActiveX, Exception]:
        pass
    
    @abstractmethod
    def get_data_to_file(self,key:str,bucket_id:str="",filename:str="",output_path:str="/activex/data")->Result[str, Exception]:
        pass
    @abstractmethod
    def put_data_from_file(self,key:str,source_path:str,bucket_id:str="",tags:Dict[str,str]={},chunk_size:str="1MB")->Result[bool, Exception]:
        pass


class LocalStorageService(StorageService):
    def __init__(self, storage_service_id: str):
        super().__init__(storage_service_id)
    def put(self, obj: ActiveX, key: str = "",bucket_id:str="") -> Result[str, Exception]:
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
    
    def get(self, key: str,bucket_id:str="")->Result[ActiveX, Exception]:
        base_path = os.environ.get("ACTIVEX_LOCAL_PATH","/activex")
        path = "{}/{}".format(base_path,key)
        logger.debug("GET %s %s", key, path)
        with open(path,"rb") as f :
            data = f.read()
            return ActiveX.from_bytes(raw_obj=data)
    
    def get_data_to_file(self,key:str,bucket_id:str="",filename:str="",output_path:str="/activex/data")->Result[str, Exception]:
        return Err(Exception("get_data_to_file not implemented"))
    def put_data_from_file(self,key:str,source_path:str,bucket_id:str="",tags:Dict[str,str]={},chunk_size:str="1MB")->Result[bool, Exception]:
        return Err(Exception("put_data_from_file not implemented"))
        # return Err(Exception("Not implemented yet."))

class AWSS3(StorageService):
    pass
class MictlanXStorageService(StorageService):
    def __init__(self):
        super().__init__(storage_service_id="MictlanX")
        NODE_ID = os.environ.get("MICTLANX_ID","activex")
        BUCKET_ID       = os.environ.get("MICTLANX_BUCKET_ID",NODE_ID)
        routers_str = os.environ.get("MICTLANX_ROUTERS","mictlanx-router-0:localhost:60666")
        MICTLANX_PROTOCOL  = os.environ.get("MICTLANX_PROTOCOL","http")
        # Parse the routers_str to a Router object
        routers     = list(Utils.routers_from_str(routers_str,protocol=MICTLANX_PROTOCOL))

        self.client =  Client(
            # Unique identifier of the client
            client_id   = os.environ.get("MICTLANX_CLIENT_ID",NODE_ID),
            # Storage peers
            routers     = routers,
            # Number of threads to perform I/O operations
            max_workers = int(os.environ.get("MICTLANX_MAX_WORKERS","2")),
            # This parameters are optionals only set to True if you want to see some basic metrics ( this options increase little bit the overhead please take into account).
            debug       = True,
            log_output_path= os.environ.get("MICTLANX_LOG_OUTPUT_PATH","./log"),
            bucket_id=BUCKET_ID
        )
    def put(self,obj:ActiveX,key:str="",bucket_id:str="",chunk_size:str="1MB")->Result[str,Exception]:
        tags = {
                **obj._acx_metadata.to_json_with_string_values()
        }
        # tags.pop("pivot_storage_node")
        # tags.pop("replica_nodes")
        result= self.client.put_chunked(
            chunks= obj.to_stream(chunk_size=chunk_size),
            bucket_id=bucket_id,
            key=key,
            tags=tags

        )
        # result:Result[PutResponse,Exception]  = future.result()
        if result.is_ok:
            response = result.unwrap()
            return Ok(response.key)
        else :
            return Err(result.unwrap_err())
        

    def get(self,key:str,bucket_id:str="")->Result[ActiveX,Exception]:
        response_size = 0 
        while response_size ==0:
            result:Result[GetBytesResponse,Exception]= self.client.get_with_retry(
                bucket_id=bucket_id,
                key=key)
            if result.is_err:
                return Err(Exception("{} not found".format(key)))
            response:GetBytesResponse = result.unwrap()
            response_size = len(response.value)
            if response_size ==0:
                logger.warning({
                    "event":"EMPTY.RESPONSE",
                    "key":key,
                    "response_size":response_size
                })
                continue
            return Ok(ActiveX.from_bytes(response.value))
    def get_data_to_file(self, key: str,bucket_id:str="",filename:str="",output_path:str="/activex/data",chunk_size:str="1MB") -> Result[str, Exception]:
        try:
            return self.client.get_to_file(key=key,bucket_id=bucket_id,filename=filename,output_path=output_path,chunk_size=chunk_size)
        except Exception as e:
            return Err(e)
    def put_data_from_file(self,source_path: str,key: str="",bucket_id:str="", tags: Dict[str, str]={},chunk_size:str="1MB") -> Result[bool, Exception]:
        try:
            result = self.client.put_file_chunked(path=source_path,key=key,chunk_size=chunk_size,bucket_id=bucket_id,tags=tags)
            return Ok(result.is_ok)
        except Exception as e:
            return Err(e)
   