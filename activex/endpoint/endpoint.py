from abc import ABC,abstractmethod
import humanfriendly as HF
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
    def __init__(self,
                 protocol:str="tcp",
                 hostname:str= "127.0.0.1",
                 req_res_port:int = 60667,
                 pubsub_port:int=16666,
                 encoding:str ="utf-8",
                 endpoint_id:str="activex-endpoint-0",
                 is_local:bool=True
        ):
        self.endpoint_id  = endpoint_id
        self.protocol = protocol
        self.hostname = hostname
        self.req_res_port=req_res_port
        self.pubsub_port= pubsub_port
        self.encoding=encoding
        self.is_local:bool = is_local
        
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
            return Ok(f(ao,*fargs, **fkwargs))
        except Exception as e:
            return Err(e)
        # return Err(Exception("No implemented yet."))
    
class DistributedEndpoint(EndpointX):
    def __init__(self, 
                 endpoint_id:str="",
                 protocol:str="tcp",
                 hostname: str = "127.0.0.1",
                 publisher_hostname:str = "*",
                 pubsub_port: int = 16666,
                 req_res_port:int = 16667,
                 max_health_ping_time:str = "1h",
                 max_recv_timeout:str = "120s",
                 max_retries:int=10
    ):
        super().__init__(protocol=protocol,hostname=hostname, req_res_port=req_res_port,endpoint_id=endpoint_id,pubsub_port=pubsub_port,is_local=False)
        self.pubsub_uri = f"{self.protocol}://{publisher_hostname}:{pubsub_port+1}" if pubsub_port != -1 else f"{self.protocol}://{publisher_hostname}"
        
        self.reqres_full_uri = f"{self.protocol}://{hostname}:{req_res_port}" if req_res_port != -1 else f"{self.protocol}://{hostname}"
        self.pubsub_socket = None
        self.reqres_socket = None
        self.last_ping_at = -1
        self.max_health_tick_time = HF.parse_timespan(max_health_ping_time)
        self.max_retries = max_retries
        self.is_connected = False
        self.max_recv_timeout = int(HF.parse_timespan(max_recv_timeout)*1000)

    
    def start(self)->int:
        current_tries = 0
        # current_time = T.time()
        # diff = current_time - self.last_ping_at
        while not self.is_connected and current_tries < self.max_retries:
            try:
                if current_tries > 0:
                    logger.warn({
                        "event":"DISTRIBUTED.ENDPOINT.RETRY",
                        "endpoint_id":self.endpoint_id,
                        "hostname":self.hostname,
                        "req_res_port":self.req_res_port,
                        "pubsub_port":self.pubsub_port,
                        "protocol":self.protocol,
                        "current_retry":current_tries,
                        "max_retries":self.max_retries
                    })
                if self.pubsub_socket == None or self.reqres_socket == None:
                    self.context = zmq.Context()

                # if self.pubsub_socket == None:
                #     self.pubsub_socket  = self.context.socket(zmq.PUB)
                #     self.pubsub_socket.setsockopt(zmq.SNDTIMEO, 1000)
                #     self.pubsub_socket.bind(self.pubsub_uri)
                    # _______________________________
                if self.reqres_socket==None:
                    logger.debug("Connecting to {}".format(self.reqres_full_uri))
                    self.reqres_socket = self.context.socket(zmq.REQ)
                    self.reqres_socket.setsockopt(zmq.RCVTIMEO, self.max_recv_timeout)
                    self.reqres_socket.connect(self.reqres_full_uri)
                    logger.debug("Connected to {}".format(self.reqres_full_uri))
                current_time = T.time()
                diff = current_time - self.last_ping_at
                
                logger.debug({
                    "event":"PING",
                    "last_ping_at":self.last_ping_at,
                    "diff":diff,
                    "max_health_tick_time":self.max_health_tick_time,
                    "reqres_socket":str(self.reqres_socket)
                })
                if not self.reqres_socket  == None and ( self.last_ping_at ==-1 or  diff >= self.max_health_tick_time ):
                    T.sleep(1)
                    try:
                        logger.debug("BEFORE.SEND.MULTIPLART")
                        x = self.reqres_socket.send_multipart([b"activex",b"PING",b"{}"],track=True)
                        logger.debug("BEGORE.SEND.AFTER")
                        y = self.reqres_socket.recv_multipart()
                        logger.debug("ActiveX metadata service connected successfully.. %s",self.pubsub_uri) 
                        self.last_ping_at = T.time()   
                        self.is_connected = True
                        return 0
                    except Exception as e:
                        if not self.reqres_socket == None:
                            self.reqres_socket.close(linger=1)
                        # if not self.pubsub_socket == None:
                            # self.pubsub_socket.close(linger=1)
                        if not self.context == None:
                            self.context.destroy()
                        self.context=None
                        self.reqres_socket= None 
                        self.pubsub_socket = None
                        logger.error(str(e))
            except Exception as e:
                logger.error(str(e))
            finally:
                current_tries+=1
        return 0 if self.is_connected else -1

    def put(self, key: str, metadata: MetadataX)->Result[str,Exception]:
        try:
            # if self.is_local:
                # return Err(Exception("{} is a local endpoint".format(self.endpoint_id)))
            # print("BEGORE_START",self.endpoint_id, self.hostname,self.req_res_port)
            status = self.start()
            # print("AFTER_STATUs", status)
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

    def __deserialize(self,x:bytes)->Any:
        try:
            return CP.loads(x)
        except Exception as e:
            return J.loads(x)
        
    def method_execution(self,key:str,fname:str,ao:ActiveX,f:GenericFunction,fargs:list=[],fkwargs:dict={}) -> Result[Any, Exception]:
        start_time = T.time()
        try:
            # logger.debug("method_execution {} {}".format(key, fname))
            # logger.debug({
            #     "event":"METHOD.EXECUTION",
            #     # "sink_bucket_id":ao.get_sink_bucket_id(), 
            #     # # "sink_key":"{}{}".format(fname,ao.get_sink_key()),
            #     "function_name":fname,
            #     # **fkwargs
            # })
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
                result          = self.__deserialize(x= result_bytes)
                # CP.loads(result_bytes)
                result_metadata = J.loads(metadata_bytes)
                
                logger.info({
                    "event":"METHOD.EXEC.COMPLETED",
                    "key":key,
                    "fname":fname,
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
    def __init__(self,endpoints:Dict[str,DistributedEndpoint]={},endpoint_id:str=""):
        self.endpoints = endpoints
        self.endpoint_id = endpoint_id
        self.counter =0 
    def exists(self,endpoint_id:str)->bool:
        return endpoint_id in self.endpoints
        # eids = self.endpoints.values(
    def get_endpoint(self,endpoint_id:str="")->DistributedEndpoint:
        if not endpoint_id  in self.endpoints:
            i = self.counter%len(self.endpoints)
            self.counter+=1
            return list(self.endpoints.values())[i]
        self.counter+=1
        return self.endpoints.get(endpoint_id)
    def add_endpoint(self,endpoint_id:str,hostname:str, pubsub_port:int, req_res_port:int,protocol:str="tcp"):
        x                           = DistributedEndpoint(protocol=protocol,hostname=hostname,endpoint_id=endpoint_id, req_res_port=req_res_port,pubsub_port=pubsub_port)
        if not self.endpoint_id== endpoint_id:
            status = x.start()
            if status == -1:
                logger.error({
                    "event":"DISTRIBUTED.ENDPOINT.FAILED",
                    "endpoint_id":endpoint_id,
                    "hostname":hostname,
                    "pubsub_port":pubsub_port,
                    "req_res_port":req_res_port,
                    "protocol":protocol
                })
        self.endpoints[endpoint_id] = x

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