from abc import ABC,abstractmethod
from typing import Optional
from nanoid import generate as nanoid
import string
from weakref import WeakKeyDictionary
from activex.storage.metadata import MetadataService
from activex.storage.data import StorageService,ActiveX
from option import Result
import logging

logger = logging.getLogger(__name__)
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.setLevel(logging.DEBUG)

ALPHABET = string.digits+string.ascii_lowercase



class ActiveXRuntime(ABC):
    def __init__(self,metadata_service:MetadataService,storage_service:StorageService, runtime_id:str, is_distributed:bool = False ):
        self.runtime_id = runtime_id if len(runtime_id) > 16 else nanoid(alphabet=ALPHABET)
        self.is_distributed:bool = is_distributed
        self.inmemory_objects = WeakKeyDictionary()
        self.metadata_service = metadata_service
        self.storage_service = storage_service
    
    def get_by_key(self,key:str)->Result[ActiveX,Exception]:
        return self.storage_service.get(key=key)
    def persistify(
            self,
            instance: ActiveX,
            key:Optional[str] = None,
            storage_node:Optional[str] = None
    )->Result[str,Exception]:
        self.metadata_service.put(
            key=key,
            metadata=instance._acx_metadata
        )
        self.storage_service.put(
            obj=instance,
            key=key
        )
        # logger.debug("%s persistify",key)

    @abstractmethod
    def stop(self):
        pass
        