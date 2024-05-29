from abc import ABC,abstractmethod
from activex import MetadataX
import logging
import json as J
import time as T
from option import Result,Ok,Err
import zmq

logger = logging.getLogger(__name__)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.setLevel(logging.DEBUG)
class MetadataService(ABC):
    # protocol:str = "tcp"
    # hostname:str
    # port:int 
    def __init__(self,protocol:str="tcp", hostname:str="localhost" , port:int=-1):
        self.protocol :str = protocol
        self.hostname = hostname
        self.port = port
    @abstractmethod
    def put(self,key:str, metadata:MetadataX)->Result[str, Exception]:
        pass
    @abstractmethod
    def get(self,key:str)->Result[MetadataX, Exception]:
        pass

class LocalMetadataService(MetadataService):
    def __init__(self) -> None:
        super().__init__()
        
    def put(self, key: str, metadata: MetadataX):
        logger.debug("PUT.METADATA %s", key)
        print(metadata.model_dump_json(indent=4))
        return Err(Exception("Not implemented"))

    def get(self, key: str):
        logger.debug("GET.METADATA %s", key)
        return Err(Exception("Not implemented"))

class ActiveXMetadataService(MetadataService):
    def __init__(self,protocol:str="tcp", hostname:str="localhost" , port:int=-1) -> None:
        try: 
            super().__init__(protocol=protocol, hostname=hostname, port=port)
            self.context = zmq.Context()
            self.socket  = self.context.socket(zmq.PUB)
            full_uri = f"{self.protocol}://{hostname}:{port}" if port != -1 else f"{self.protocol}://{hostname}"
            
            reqres_full_uri = f"{self.protocol}://{hostname}:{port+1}" if port != -1 else f"{self.protocol}://{hostname}"
            self.socket.setsockopt(zmq.SNDTIMEO, 1000)
            self.socket.bind(full_uri)
            # _______________________________
            logger.debug("Connecting to {}".format(reqres_full_uri))
            self.reqres_socket = self.context.socket(zmq.REQ)
            self.reqres_socket.connect(reqres_full_uri)
            T.sleep(1)
            try:
                x = self.reqres_socket.send_multipart([b"activex",b"PING",b"{}"],track=True)
                y = self.reqres_socket.recv_multipart()
            except zmq.error.Again as e:
                print("ERROR",e)
            except Exception as e:
                print("EXPETION",e)
            logger.debug("ActiveX metadata service connected successfully.. %s",full_uri)
            #_________________________________ 


        except Exception as e:
            logger.error("Metadata service failed to connect. %s", e)
    
    def put(self, key: str, metadata: MetadataX):
        logger.debug("(not implemented yet) PUT.METADATA %s", key)
        json_metadata =metadata.model_dump().copy()
        rn = json_metadata.get("replica_nodes",[])
        json_metadata["replica_nodes"] = list(rn)
  
        json_metadata_str = J.dumps(json_metadata)
        json_metadata_bytes = json_metadata_str.encode(encoding="utf-8")
        x = self.reqres_socket.send_multipart([b"activex",b"PUT.METADATA", json_metadata_bytes])
        y = self.reqres_socket.recv_multipart()
        print("RESPONSE", y)

    def get(self, key: str):
        logger.debug("GET.METADATA %s", key)