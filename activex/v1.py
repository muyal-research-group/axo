from __future__ import annotations
import inspect
import os
import string
import logging
import re
import cloudpickle as CP
from functools import wraps
from nanoid import generate as nanoid 
from typing import Dict,Optional,ClassVar,Set,Annotated,get_type_hints
from pydantic import BaseModel,Field
from pydantic.functional_validators import AfterValidator
from activex.runtime import get_runtime
from activex.scheduler import Task
from activex.storage.mictlanx import GetKey,PutPath
from pathlib import Path
import humanfriendly as HF
import time as T
ALPHABET = string.ascii_lowercase+string.digits
ACTIVEX_OBJECT_ID_SIZE = 16
logger = logging.getLogger(__name__)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.setLevel(logging.DEBUG)

ACX_PROPERTY_PREFIX = "_acx_property_"

ACX_ENQUEUE_EXTRA_SECS = os.environ.get("ACX_ENQUEUE_EXTRA_SECS","1s")

def resolve_annotations(self:ActiveX):
    runtime = get_runtime()
    logger.debug("RESOLVE_ANNOTATIONS {}".format(self))
    # cls = self.__class__
    attributes = self.__dict__
    for attr_name, attr_value in attributes.items():
        # attr_type = type(attr_value)
        attr_type_hint = self.__annotations__.get(attr_name)

        # Check if the annotated key is GetKey
        if attr_type_hint == Annotated[str,GetKey]:
            value = getattr(self,attr_name)
            logger.debug("GET {}".format(value))
            result = runtime.storage_service.get_data_to_file(
                key=value
            )
            if result.is_err:
                logger.error("{} not found in the storage service".format(value))
                # raise Exception("" .format(value))
        
        # Check if the annotated key is PutPath
        if attr_type_hint == Annotated[str,PutPath]:
            value = getattr(self,attr_name)
            task = Task(
                operation="PUT",
                executed_at= T.time() + HF.parse_timespan(ACX_ENQUEUE_EXTRA_SECS) ,
                metadata={
                    "path":value 
                } 
            )
            # Schedule a put task.
            runtime.scheduler.schedule(
                task=task
            )
            

def activex(f):
    @wraps(f)
    def __activex(self:ActiveX,*args,**kwargs):
        resolve_annotations(self=self)
        logger.debug("is_local=%s", self._acx_local)
        result = f(self,*args,**kwargs)
        return result
    return __activex


def generate_id(v:str)->str:
    if v == None or v == "":
        return nanoid(ALPHABET, size=ACTIVEX_OBJECT_ID_SIZE)
    return re.sub(r'[^\w\s]', '', v)


ActiveXObjectId = Annotated[Optional[str], AfterValidator(generate_id)]

class MetadataX(BaseModel):
    path:ClassVar[str]               = os.environ.get("ACTIVE_LOCAL_PATH","/activex")
    pivot_storage_node:Optional[str] =None
    replica_nodes:Set[str]           = Field(default_factory=set)
    is_read_only:bool                = False
    id:ActiveXObjectId               = None            
    class_name:str 

    def to_json_with_string_values(self)->Dict[str,str]:
        json_data = self.model_dump()
        # Convert all values to strings
        for key, value in json_data.items():
            json_data[key] = str(value)

        return json_data

    # @validator("id", pre=True, always=True)
    # def generate_id(cls, v):
    #     return v if v is not None else nanoid(ALPHABET, size=ACTIVEX_OBJECT_ID_SIZE)
    
class ActiveX:
    _acx_metadata: MetadataX
    _acx_local:bool = True
    _acx_remote:bool = False

    def __init_subclass__(cls, **kwargs):
        logger.debug(f"Subclass {cls.__name__} created.")
    def __new__(cls,*args,**kwargs):
        obj = super().__new__(cls)
        obj._acx_metadata = MetadataX(
            class_name= cls.__module__ + "." + cls.__name__, 
            id= None
        )
        #  check runtime
        logger.debug(
            "(%s) New instance %s args=%s kwargs=%s", 
            obj._acx_metadata.id,
            cls.__name__,
            args,
            kwargs
        )
        return obj

        
    def __init__(cls,
                 object_id:str, 
                 tags:Dict[str,str]={}
    ):     
        obj = super().__new__(cls)
        obj.object_id = nanoid(ALPHABET) if object_id =="" else object_id
        obj.tags= tags
        # print(self.metadata)
        

    def to_bytes(self):
        return CP.dumps(self)

    @staticmethod
    def from_bytes(raw_obj:bytes):
        return CP.loads(raw_obj)
    
    @staticmethod
    def get_by_key(key):
        return get_runtime().get_by_key(key)

    def persistify(self,key:str=""):
        
        _key = self._acx_metadata.id if key == "" else key
        runtime = get_runtime()
        runtime.persistify(self,key=_key)
        self._acx_remote = True




