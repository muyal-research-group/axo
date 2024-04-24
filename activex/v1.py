import os
import string
import logging
import re
import cloudpickle as CP
from functools import wraps
from typing_extensions import Annotated
from nanoid import generate as nanoid 
from typing import Dict,Optional,ClassVar,Set
from pydantic import BaseModel,Field
from pydantic.functional_validators import AfterValidator
from activex.runtime import get_runtime

ALPHABET = string.ascii_lowercase+string.digits
ACTIVEX_OBJECT_ID_SIZE = 16
logger = logging.getLogger(__name__)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.setLevel(logging.DEBUG)

ACX_PROPERTY_PREFIX = "_acx_property_"

def activex(f):
    @wraps(f)
    def __activex(self:ActiveX,*args,**kwargs):
        runtime = get_runtime()
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




