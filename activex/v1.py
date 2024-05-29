from __future__ import annotations
from option import Result,Ok,Err
import os
import string
import logging
import re
import cloudpickle as CP
from functools import wraps
from nanoid import generate as nanoid 
from typing import Dict,Optional,ClassVar,Set,Annotated,Generator
from pydantic import BaseModel,Field
from pydantic.functional_validators import AfterValidator
from activex.runtime import get_runtime
# from activex.runtime.runtime import ActiveXRuntime
from activex.scheduler import Task
from activex.storage.mictlanx import GetKey,PutPath
import humanfriendly as HF
from activex.utils import serialize_and_yield_chunks
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
    print(attributes)
    T.sleep(2)
    for attr_name, attr_value in attributes.items():
        # attr_type = type(attr_value)
        attr_type_hint = self.__annotations__.get(attr_name)
        print("ATTR_TYPE",attr_type_hint)
        # Check if the annotated key is GetKey
        T.sleep(2)
        if attr_type_hint == Annotated[str,GetKey]:
            value = getattr(self,attr_name)
            logger.debug("GET {}".format(value))
            result = runtime.storage_service.get_data_to_file(
                key=value
            )
            print("RESULT",result)
            T.sleep(2)
            if result.is_err:
                logger.error("{} not found in the storage service".format(value))
                # raise Exception("" .format(value))
        
        # Check if the annotated key is PutPath
        if attr_type_hint == Annotated[str,PutPath]:
            value = getattr(self,attr_name)
            logger.debug("SCHEDULE.TASK {}".format(value))
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
    print("FINISH")
    T.sleep(5)
            

def activex_method(f):
    @wraps(f)
    def __activex(self:ActiveX,*args,**kwargs):
        try:
            # resolve_annotations(self=self)
            runtime = get_runtime()
            logger.debug("is_local=%s", self._acx_local)

            res = runtime.middleware.method_execution(
                key=self._acx_metadata.id,
                fname=f.__name__,
                ao=self,
                f= f,
                # f= f_serialized,
                fargs=args,
                fkwargs=kwargs
            )
            return res
        
        except Exception as e:
            logger.error(str(e))
    return __activex


def generate_id(v:str)->str:
    if v == None or v == "":
        return nanoid(ALPHABET, size=ACTIVEX_OBJECT_ID_SIZE)
    return re.sub(r'[^\w\s]', '', v)


ActiveXObjectId = Annotated[Optional[str], AfterValidator(generate_id)]

class MetadataX(BaseModel):
    path:ClassVar[str]               = os.environ.get("ACTIVE_LOCAL_PATH","/activex")
    pivot_storage_node:Optional[str] = ""
    # replica_nodes:Set[str]           = Field(default_factory=set)
    is_read_only:bool                = False
    id:ActiveXObjectId               = None            
    module:str
    name:str
    class_name:str 
    version:str                       = "v0"

    def to_json_with_string_values(self)->Dict[str,str]:
        json_data = self.model_dump()
        # Convert all values to strings
        for key, value in json_data.items():
            # if key == "replica_nodes"
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
            module= cls.__module__,
            name= cls.__name__,
            id= None
        )
        #  check runtime
        logger.debug(
            # "(%s) New instance %s args=%s kwargs=%s", 
            "(%s) New instance %s", 
            obj._acx_metadata.id,
            cls.__name__,
            # args,
            # kwargs
        )
        return obj

        
    def __init__(cls,
                 object_id:str, 
                 tags:Dict[str,str]={},
                 version:str  = "v0"
    ):     
        obj = super().__new__(cls)
        obj.object_id = nanoid(ALPHABET) if object_id =="" else object_id
        obj.tags= tags
        obj._acx_metadata.version=version
        # print(self.metadata)
        

    def to_bytes(self):
        return CP.dumps(self)
    def to_stream(self,chunk_size:str="1MB")->Generator[bytes,None,None]:
        return serialize_and_yield_chunks(obj=self, chunk_size=chunk_size)

    @staticmethod
    def from_bytes(raw_obj:bytes):
        return CP.loads(raw_obj)
    
    @staticmethod
    def get_by_key(key:str):
        return get_runtime().get_by_key(key)

    def persistify(self,key:str="")->Result[str, Exception]:
        try:
            _key = self._acx_metadata.id if key == "" else key
            runtime = get_runtime()
            persistify_result = runtime.persistify(self,key=_key)
            self._acx_remote = persistify_result.is_ok
            return persistify_result
        except Exception as e:
            return Err(e)




