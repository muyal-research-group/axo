from abc import ABC,abstractmethod
from typing import Any,TypeVar,Callable,List,Dict,Set
import time as T
import json as J
from activex import ActiveX
from activex.storage.metadata import MetadataX
import random
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



        

class EndpointX(ABC):
    def __init__(self,protocol:str="tcp",hostname:str= "127.0.0.1", req_res_port:int = 60667,pubsub_port:int=16666, encoding:str ="utf-8",endpoint_id:str="activex-endpoint-0"):
        self.endpoint_id  = endpoint_id
        self.protocol = protocol
        self.hostname = hostname
        self.req_res_port=req_res_port
        self.pubsub_port= pubsub_port
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

        

class LocalEndpoint(EndpointX):
    def __init__(self, endpoint_id:str="activex-endpoint-0"):
        super().__init__(
            endpoint_id=endpoint_id
        )
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
    
class DistributedEndpoint(EndpointX):
    def __init__(self, 
                 endpoint_id:str="",
                 protocol:str="tcp",
                 hostname: str = "127.0.0.1",
                 pubsub_port: int = 16666,
                 req_res_port:int = 16667
    ):
        super().__init__(protocol=protocol,hostname=hostname, req_res_port=req_res_port,endpoint_id=endpoint_id,pubsub_port=pubsub_port)
        self.context = zmq.Context()
        pubsub_uri = f"{self.protocol}://{hostname}:{pubsub_port}" if pubsub_port != -1 else f"{self.protocol}://{hostname}"
        
        reqres_full_uri = f"{self.protocol}://{hostname}:{req_res_port}" if req_res_port != -1 else f"{self.protocol}://{hostname}"

        self.socket  = self.context.socket(zmq.PUB)
        self.socket.setsockopt(zmq.SNDTIMEO, 1000)
        self.socket.bind(pubsub_uri)
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
        logger.debug("ActiveX metadata service connected successfully.. %s",pubsub_uri)    
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
            # logger.debug("method_execution {} {}".format(key, fname))
            logger.debug({
                "event":"METHOD.EXECUTION",
                "sink_bucket_id":ao.get_sink_bucket_id(), 
                "sink_key":"{}{}".format(fname,ao.get_sink_key()),
                "function_name":fname
            })
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
    
    
    def to_string(self):
        return "{}:{}:{}:{}:{}".format(self.endpoint_id,self.protocol,self.hostname,self.req_res_port,self.pubsub_port)
    @staticmethod
    def from_str(endpoint_str:str)->'DistributedEndpoint':
        x = endpoint_str.split(":")
        return DistributedEndpoint(
            endpoint_id=x[0],
            protocol=x[1],
            hostname=x[2],
            req_res_port=x[3],
            pubsub_port=x[4],
        )

class XoloEndpointManager(ABC):
    def __init__(self,endpoints:Dict[str,DistributedEndpoint]={}):
        self.endpoints = endpoints
        self.counter =0 
    def exists(self,endpoint_id:str)->bool:
        return endpoint_id in self.endpoints
        # eids = self.endpoints.values(
    def get_endpoint(self,endpoint_id:str="")->DistributedEndpoint:
        if not endpoint_id  in self.endpoints:
            i = self.counter%len(self.endpoints)
            self.counter+=1
            return list(self.endpoints.values())[ i]
        self.counter+=1
        return self.endpoints.get(endpoint_id)
    def add_endpoint(self,endpoint_id:str,hostname:str, pubsub_port:int, req_res_port:int,protocol:str="tcp"):
        self.endpoints[endpoint_id] = DistributedEndpoint(protocol=protocol,hostname=hostname,endpoint_id=endpoint_id, req_res_port=req_res_port,pubsub_port=pubsub_port)
    def del_endpoint(self,endpoint_id):
        return self.endpoints.pop(endpoint_id)
    def get_available_port(self,ports:Set[int], low:int=16000,high:int = 65000)->int:
        while True:
            port = random.randint(low, high)
            if port not in ports:
                return port
    def get_available_req_res_port(self)->int:
        return self.get_available_port(
            ports=  set(list(map(lambda x: x.req_res_port,self.endpoints.values())))
        )
    def get_available_pubsub_port(self)->int:
        return self.get_available_port(
            ports=  set(list(map(lambda x: x.pubsub_port,self.endpoints.values())))
        )
        # self.counter % len(self.endpoints)