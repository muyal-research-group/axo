from abc import ABC,abstractmethod
from typing import Any,TypeVar,Callable
import time as T
import json as J
from activex import ActiveX
from activex.storage.metadata import MetadataX
from option import Result,Err,Ok
import logging
import zmq
import cloudpickle as CP

# type variable
TV = TypeVar("TV")
GenericFunction = Callable[[TV],TV]

logger = logging.getLogger(__name__)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.setLevel(logging.DEBUG)





class MiddlewareX(ABC):
    def __init__(self,protocol:str="tcp",hostname:str= "127.0.0.1", port:int = 60667, encoding:str ="utf-8"):
        self.protocol = protocol
        self.hostname = hostname
        self.port=port
        self.encoding=encoding
        
    @abstractmethod
    def put(self, key:str, value:MetadataX)->Result[str, Exception]:
        return Err(Exception("No implemented yet."))

    @abstractmethod
    def get(self, key:str)->Result[MetadataX, Exception]:
        return Err(Exception("No implemented yet."))

    @abstractmethod
    def method_execution(self,
                         key:str,
                         fname:str,
                         ao:ActiveX,
                         f:GenericFunction = None, 
                         fargs:list=[],
                         fkwargs:dict={}
    )->Result[Any,Exception]:
        return Err(Exception("No implemented yet."))


class LocalMiddleware(MiddlewareX):
    def __init__(self, 
                 protocol:str="tcp",
                 hostname: str = "127.0.0.1",
                 port: int = 60667
    ):
        super().__init__(protocol,hostname, port)
        self.__db  = {}

    def put(self, key: str, value: MetadataX) -> Result[str, Exception]:
        try:
            if not key in self.__db:
                self.__db[key] = value
            return Ok(key)
        except Exception as e:
            return Err(e)
    def get(self, key: str) -> Result[MetadataX, Exception]:
        if key in self.__db:
            return Ok(self.__db[key])
        return Err(Exception("Not found: {}".format(key)))
    def method_execution(self,key:str,fname:str,ao:ActiveX,f:GenericFunction = None,fargs:list=[],fkwargs:dict={}) -> Result[Any, Exception]:
        try:
            # fn = getattr(ao, fname)
            # print(fn)
            return Ok(f(ao,*fargs, **fkwargs))
        except Exception as e:
            return Err(e)
        # return Err(Exception("No implemented yet."))
    
class DistributedMiddleware(MiddlewareX):
    def __init__(self, 
                 protocol:str="tcp",
                 hostname: str = "127.0.0.1",
                 port: int = 16666,
                 req_res_port:int = 16667
    ):
        super().__init__(protocol,hostname, port)
        self.context = zmq.Context()
        full_uri = f"{self.protocol}://{hostname}:{port}" if port != -1 else f"{self.protocol}://{hostname}"
        
        reqres_full_uri = f"{self.protocol}://{hostname}:{req_res_port}" if req_res_port != -1 else f"{self.protocol}://{hostname}"

        self.socket  = self.context.socket(zmq.PUB)
        self.socket.setsockopt(zmq.SNDTIMEO, 1000)
        self.socket.bind(full_uri)
        # _______________________________
        logger.debug("Connecting to {}".format(reqres_full_uri))
        self.reqres_socket = self.context.socket(zmq.REQ)
        self.reqres_socket.connect(reqres_full_uri)
        T.sleep(1)
        logger.debug("Connected to {}".format(reqres_full_uri))
        try:
            x = self.reqres_socket.send_multipart([b"activex",b"PING",b"{}"],track=True)
            y = self.reqres_socket.recv_multipart()
        except zmq.error.Again as e:
            logger.error(str(e))
        except Exception as e:
            logger.error(str(e))
        logger.debug("ActiveX metadata service connected successfully.. %s",full_uri)    
    def put(self, key: str, metadata: MetadataX)->Result[str,Exception]:
        try:
            start_time = T.time()
            json_metadata =metadata.model_dump().copy()
            json_metadata_str = J.dumps(json_metadata)
            json_metadata_bytes = json_metadata_str.encode(encoding="utf-8")
            request_tracker = self.reqres_socket.send_multipart([b"activex",b"PUT.METADATA", json_metadata_bytes])
            response = self.reqres_socket.recv_multipart()
            logger.debug("PUT.METADATA %s %s", key, T.time() - start_time)
            return Ok(key)
        except Exception as e:
            return Err(e)

    def get(self, key: str)->Result[MetadataX,Exception]:
        try:
            logger.debug("GET.METADATA %s", key)
            return Ok(key)
        except Exception as e:
            return Err(Exception("Not implemented yet."))

    def method_execution(self,key:str,fname:str,ao:ActiveX,f:GenericFunction,fargs:list=[],fkwargs:dict={}) -> Result[Any, Exception]:
        start_time = T.time()
        try:
            logger.debug("method_execution {} {}".format(key, fname))
            payload = {
                "key":key,
                "fname":fname
            }
            payload_bytes = J.dumps(payload).encode(self.encoding)

            f_bytes = CP.dumps(f)
            self.reqres_socket.send_multipart([b"activex",b"METHOD.EXEC", payload_bytes, f_bytes,CP.dumps(fargs), CP.dumps(fkwargs)])
            response_multipart = self.reqres_socket.recv_multipart()
            if len(response_multipart) == 5:
                topic_bytes,operation_bytes,status_bytes,metadata_bytes, result_bytes  = response_multipart
                
                topic           = topic_bytes.decode()
                operation       = operation_bytes.decode()
                status          = int.from_bytes(bytes=status_bytes, byteorder="little",signed=True )
                result          = CP.loads(result_bytes)
                result_metadata = J.loads(metadata_bytes)
                
                logger.info({
                    "event":"METHOD.EXEC.COMPLETED",
                    "status":status,
                    "topic":topic,
                    "operation":operation,
                    **result_metadata,
                    "response_time":T.time() - start_time
                })
                return Ok(result)
            else:
                return Err(Exception("Not expected response: {}".format(len(response_multipart))))
            # logger.debug(str(response))
            # return Ok(key)
        except Exception as e:
            return Err(e)
        # return super().method_execution()