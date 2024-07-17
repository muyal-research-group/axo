from abc import ABC,abstractmethod
import logging
import humanfriendly as HF
from activex import ActiveX
from mictlanx.v4.client import Client
from mictlanx.utils.index import Utils
from mictlanx.logger.tezcanalyticx.tezcanalyticx import TezcanalyticXHttpHandler,TezcanalyticXParams
import mictlanx.v4.interfaces as InterfaceX
from mictlanx.v4.client import Client
from option import Result,Err,Ok,Some,NONE,Option
from nanoid import generate as nanoid
from typing import Dict,Generator,Iterator,Tuple,Any
import time as T
import string

import os
logger = logging.getLogger(__name__)
console_handler = logging.StreamHandler()
logger.addHandler(console_handler)
logger.setLevel(logging.DEBUG)


# class Metadata()

class StorageService(ABC):
    def __init__(self,storage_service_id:str):
        self.storage_service_id = storage_service_id
    
    @abstractmethod
    def put(self,bucket_id:str,key:str,data:bytes,tags:Dict[str,str]={},chunk_size:str="1MB")->Result[str,Exception]:
        pass

    @abstractmethod
    def put_streaming(self,bukcet_id:str,key:str, data:Iterator[bytes], tags:Dict[str,str]={})->Result[str,Exception]:
        pass
    

    @abstractmethod
    def get(self,bucket_id:str,key:str,chunk_size:str="1MB")->Result[bytes, Exception]:
        pass
    @abstractmethod
    def get_streaming(self,bucket_id:str,key:str,chunk_size:str="1MB")->Result[Tuple[Iterator[bytes], Dict[str,Any]], Exception]:
        pass
    @abstractmethod
    def _get_active_object(self,key:str,bucket_id:str)->Result[ActiveX, Exception]:
        pass
    
    # @abstractmethod
    # def put_bytes(self,bucket_id:str,key:str,data:bytes,chunk_size:str="1MB")->Result[InterfaceX.PutChunkedResponse,Exception]:
    #     pass

    # @abstractmethod
    # def get_bytes(self,key:str,bucket_id:str)->Result[bytes, Exception]:
    #     pass

    # @abstractmethod
    # def get_streaming(self,key:str,bucket_id:str,chunk_size:str="1MB")->Result[Iterator[bytes], Exception]:
    #     pass

    # @abstractmethod
    # def get_data_to_file(self,key:str,bucket_id:str,filename:str="",output_path:str="/activex/data")->Result[str, Exception]:
    #     pass

    # @abstractmethod 
    # def get_data_from_file(self,path:str,chunk_size:str="1MB")->Result[Generator[bytes, None,None],Exception]:
    #     return Utils.file_to_chunks_gen(path=path, chunk_size=chunk_size)
    
    @abstractmethod
    def put_data_from_file(self,key:str,source_path:str,bucket_id:str="",tags:Dict[str,str]={},chunk_size:str="1MB")->Result[bool, Exception]:
        pass


class LocalStorageService(StorageService):
    def __init__(self, storage_service_id: str):
        super().__init__(storage_service_id)
    
    # def put_bytes(self, bucket_id: str = "", key: str = "") -> Result[InterfaceX.PutChunkedResponse, Exception]:
    #     return self.put(bucket_id=bucket_id, key=key, obj={})
    
    # def get_bytes(self,key: str, bucket_id: str = "") -> Result[bytes, Exception]:
    #     base_path = os.environ.get("ACTIVE_LOCAL_PATH","/activex/data")
    #     combined_key = "{}@{}".format(bucket_id,key)
    #     path = "{}/{}".format(base_path,combined_key)
    #     with open(path,"rb") as f:
    #         return Ok(f.read())
        
    # def get_streaming(self, key: str, bucket_id: str = "",chunk_size:str="1MB") -> Result[Iterator[bytes], Exception]:
    #     try:
    #         return Ok(Utils.to_gen_bytes(data=self.get_bytes(bucket_id=bucket_id,key=key), chunk_size=chunk_size))
    #     except Exception as e:
    #         return Err(e)
    
    def get_streaming(self, bucket_id: str, key: str, chunk_size: str = "1MB") -> Result[Tuple[Iterator[bytes],Dict[str, Any]], Exception]:
        try:
            key               = nanoid(alphabet=string.digits+string.ascii_lowercase) if not key else key
            AXO_SINK_PATH     = os.environ.get("AXO_SINK_PATH","/sink")
            default_base_path = "{}/{}".format(AXO_SINK_PATH,bucket_id)
            os.makedirs(default_base_path,exist_ok=True)
            path = "{}/{}".format(default_base_path, key)
            file = open(path, "rb")
            size =0
            def __iterator():
                while True:
                    chunk = file.read(chunk_size)
                    if not chunk:
                        break
                    size+=len(chunk)
                    yield chunk
                file.close()
            return Ok((__iterator(), {"size":str(size)}))
        except Exception as e:
            return Err(e)
        # return super().get_streaming(bucket_id, key, chunk_size)
    def put(self, bucket_id:str,key: str,data:bytes,tags:Dict[str,str]={},chunk_size:str="1MB") -> Result[str, Exception]:
        start_time   = T.time()
        key          = nanoid(alphabet=string.digits+string.ascii_lowercase) if not key else key
        AXO_SINK_PATH     = os.environ.get("AXO_SINK_PATH","/sink")
        default_base_path = "{}/{}".format(AXO_SINK_PATH,bucket_id)
        os.makedirs(default_base_path,exist_ok=True)
        path = "{}/{}".format(default_base_path, key)
        # os.makedirs(path,exist_ok=True)
        # path = "{}/{}".format(path,combined_key)
        size = 0
        with open(path,"wb") as f:
            # data = obj.to_bytes()
            size = len(data)
            f.write(data)
        response_time = T.time() - start_time
        logger.debug("PUT.DATA %s %s %s %s",path,key,size,response_time)
        return Ok(path)
    def put_streaming(self, bukcet_id: str, key: str, data: Iterator[bytes],tags:Dict[str,str]={}) -> Result[str, Exception]:
        try:
            return self.put(
                bucket_id=bukcet_id,
                key=key,
                data= list(data),
                tags=tags,
            )
        except Exception as e :
            return Err(e)
        # return super().put_streaming(bukcet_id, key, data, chunk_size)
        # return Err(Exception("Not implemented yet."))
    
    def put_data_from_file(self,key:str,source_path:str,bucket_id:str="",tags:Dict[str,str]={},chunk_size:str="1MB")->Result[bool, Exception]:
        return Err(Exception("put_data_from_file not implemented"))

    def get(self, bucket_id: str, key: str, chunk_size: str = "1MB") -> Result[bytes, Exception]:
        try:
            AXO_SINK_PATH     = os.environ.get("AXO_SINK_PATH","/sink")
            default_base_path = "{}/{}".format(AXO_SINK_PATH,bucket_id)
            # os.makedirs(default_base_path,exist_ok=True)
            path = "{}/{}".format(default_base_path, key)
            with open(path,"rb") as f:
                return Ok(f.read())
        except Exception as e:
            return Err(e)

        # return super().get(bucket_id, key, chunk_size)

    def _get_active_object(self, key: str,bucket_id:str="")->Result[ActiveX, Exception]:
        base_path = os.environ.get("ACTIVEX_SINK_PATH","/sink")
        base_path = "{}/{}".format(base_path,bucket_id)
        os.makedirs(base_path, exist_ok=True)
        path = "{}/{}/{}".format(base_path,bucket_id,key)
        logger.debug("GET %s %s", key, path)
        with open(path,"rb") as f :
            data = f.read()
            return ActiveX.from_bytes(raw_obj=data)
    
    # def get_data_to_file(self,key:str,bucket_id:str="",filename:str="",output_path:str="/activex/data")->Result[str, Exception]:
        # return Err(Exception("get_data_to_file not implemented"))
        # return Err(Exception("Not implemented yet."))

class AWSS3(StorageService):
    pass

class DropboxStorageService(StorageService):
    pass


class MictlanXStorageService(StorageService):
    def __init__(self,
                  mictlanx_id:str="activex-mictlanx",
                  bucket_id:str = "activex-mictlanx",
                  routers_str = "mictlanx-rotuer-0:localhost:60666",
                  protocol:str ="http",
                  max_workers:int=4,
                  log_path:str="./log",
                  tezcanalyticx_params:Option[TezcanalyticXParams] = NONE,
                  client:Option[Client]= NONE
            ):
        super().__init__(storage_service_id="MictlanX")
        MICTLANX_ID = os.environ.get("MICTLANX_ID",mictlanx_id)
        BUCKET_ID       = os.environ.get("MICTLANX_BUCKET_ID",bucket_id)
        routers_str = os.environ.get("MICTLANX_ROUTERS",routers_str)
        MICTLANX_PROTOCOL  = os.environ.get("MICTLANX_PROTOCOL",protocol)
        # Parse the routers_str to a Router object
        routers     = list(Utils.routers_from_str(routers_str,protocol=MICTLANX_PROTOCOL))

        if client.is_none:
            self.client =  Client(
                # Unique identifier of the client
                client_id   = os.environ.get("MICTLANX_CLIENT_ID",MICTLANX_ID),
                # Storage peers
                routers     = routers,
                # Number of threads to perform I/O operations
                max_workers = int(os.environ.get("MICTLANX_MAX_WORKERS",max_workers)),
                # This parameters are optionals only set to True if you want to see some basic metrics ( this options increase little bit the overhead please take into account).
                debug       = True,
                log_output_path= os.environ.get("MICTLANX_LOG_OUTPUT_PATH",log_path),
                bucket_id=BUCKET_ID,
                tezcanalyticx_params=tezcanalyticx_params
            )
        else:
            self.client = client.unwrap()
    # def get_data_from_file(self, path: str, chunk_size: str = "1MB") -> Result[Generator[bytes, None, None], Exception]:
    #     try:
    #         return self.client.
    #     except Exception as e:
    #         return Err(e)
    def from_client(client:Client)->'MictlanXStorageService':
        return MictlanXStorageService(
            client= Some(client)
        )
    # def put(self,obj:ActiveX,key:str="",bucket_id:str="",chunk_size:str="1MB")->Result[str,Exception]:
    def put(self,bucket_id:str,key:str,data:bytes,tags:Dict[str,str]={},chunk_size:str="1MB")->Result[str,Exception]:
        try:
            result= self.client.put(
                bucket_id=bucket_id,
                key=key,
                value= data,
                tags=tags,
                chunk_size=chunk_size
            )
            if result.is_ok:
                response = result.unwrap()
                return Ok(response.key)
            else :
                return Err(result.unwrap_err())
        except Exception as e:
            return Err(e)
        
    def put_streaming(self, bukcet_id: str, key: str, data: Iterator[bytes], tags: Dict[str, str] = {}) -> Result[str, Exception]:
        try:
            res = self.client.put_chunked(
                bucket_id=bukcet_id,
                key= key, 
                chunks=data,
                tags=tags,
            )
            if res.is_ok:
                return Ok(res.unwrap().key)
            return res
        except Exception as e:
            return Err(e)
        # return super().put_streaming(bukcet_id, key, data, tags, chunk_size)
    def get_streaming(self, bucket_id: str, key: str, chunk_size: str = "1MB") -> Result[Tuple[Iterator[bytes], Dict[str, Any]], Exception]:
        try:
            res = self.client.get_streaming_with_retry(bucket_id=bucket_id,key=key,chunk_size=chunk_size)
            if res.is_ok:
                (iterr,metadata)= res.unwrap()
                return Ok((iterr, metadata.__dict__))
        except Exception as e:
            return Err(e)
        # return super().get_streaming(bucket_id, key, chunk_size)
    def get(self, bucket_id: str, key: str, chunk_size: str = "1MB") -> Result[bytes, Exception]:
        try:
            res = self.client.get_with_retry(
                bucket_id=bucket_id,
                key=key,
                chunk_size=chunk_size
            )
            if res.is_ok:
                r = res.unwrap()
                return Ok(r.value)
            return res
        except Exception as e:
            return Err(e)
        # return super().get(bucket_id, key, chunk_size)
    def _get_active_object(self,key:str,bucket_id:str="")->Result[ActiveX,Exception]:
        response_size = 0 
        while response_size ==0:
            result:Result[InterfaceX.GetBytesResponse,Exception]= self.client.get_with_retry(
                bucket_id=bucket_id,
                key=key)
            if result.is_err:
                return Err(Exception("{} not found".format(key)))
            response:InterfaceX.GetBytesResponse = result.unwrap()
            response_size = len(response.value)
            if response_size ==0:
                logger.warning({
                    "event":"EMPTY.RESPONSE",
                    "key":key,
                    "response_size":response_size
                })
                continue
            return ActiveX.from_bytes(response.value)
    # def get_data_to_file(self, key: str,bucket_id:str="",filename:str="",output_path:str="/activex/data",chunk_size:str="1MB") -> Result[str, Exception]:
        # try:
            # return self.client.get_to_file(key=key,bucket_id=bucket_id,filename=filename,output_path=output_path,chunk_size=chunk_size)
        # except Exception as e:
            # return Err(e)
    def put_data_from_file(self,source_path: str,key: str="",bucket_id:str="", tags: Dict[str, str]={},chunk_size:str="1MB") -> Result[bool, Exception]:
        try:
            result = self.client.put_file_chunked(path=source_path,key=key,chunk_size=chunk_size,bucket_id=bucket_id,tags=tags)
            return Ok(result.is_ok)
        except Exception as e:
            return Err(e)
   