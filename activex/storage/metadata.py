from abc import ABC,abstractmethod
from activex import MetadataX
import logging
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
    def put(self,key:str, metadata:MetadataX):
        pass
    @abstractmethod
    def get(self,key:str):
        pass

class LocalMetadataService(MetadataService):
    def __init__(self) -> None:
        super().__init__()
        
    def put(self, key: str, metadata: MetadataX):
        logger.debug("PUT.METADATA %s", key)
        print(metadata.model_dump_json(indent=4))

    def get(self, key: str):
        logger.debug("GET.METADATA %s", key)

class ActiveXMetadataService(MetadataService):
    def __init__(self,protocol:str="tcp", hostname:str="localhost" , port:int=-1) -> None:
        try: 
            super().__init__(protocol=protocol, hostname=hostname, port=port)
            self.context = zmq.Context()
            self.socket  = self.context.socket(zmq.PUB)
            # uri          = hostname if port == -1 else "{}:{}".format(hostname,port)
            # full_uri     = "{}://{}".format(self.protocol, uri)
            full_uri = f"{self.protocol}://{hostname}:{port}" if port != -1 else f"{self.protocol}://{hostname}"

            self.socket.bind(full_uri)
            # 
            self.socket.send_string("activex: PING")
            logger.debug("Send start to ActiveX Metadata Service.. %s",full_uri)
        except Exception as e:
            logger.error("Metadata service failed to connect. %s", e)
    
    def put(self, key: str, metadata: MetadataX):
        logger.debug("(not implemented yet) PUT.METADATA %s", key)
        # print(metadata.model_dump_json(indent=4))

    def get(self, key: str):
        logger.debug("GET.METADATA %s", key)