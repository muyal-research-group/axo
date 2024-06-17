from __future__ import annotations
from option import Result,Ok,Err
import os
import string
import logging
import re
import cloudpickle as CP
from functools import wraps
from nanoid import generate as nanoid 
from typing import Dict,Optional,ClassVar,Set,Annotated,Generator,List
from pydantic import BaseModel,Field
from pydantic.functional_validators import AfterValidator
from activex.runtime import get_runtime
from mictlanx.logger.log import Log
from activex.utils import serialize_and_yield_chunks
import time as T



ALPHABET                  = string.ascii_lowercase+string.digits
AXO_ID_SIZE               =int(os.environ.get("AXO_ID_SIZE","16"))
AXO_DEBUG                 = bool(int(os.environ.get("AXO_DEBUG","1")))
AXO_PRODUCTION_LOG_ENABLE = bool(int(os.environ.get("AXO_PRODUCTION_LOG_ENABLE","1")))
AXO_PROPERTY_PREFIX       = "_acx_property_"
AXO_LOG_PATH              = os.environ.get("AXO_LOG_PATH","/activex/log")

# AXO_ENQUEUE_EXTRA_SECS = os.environ.get("ACX_ENQUEUE_EXTRA_SECS","1s")

if AXO_PRODUCTION_LOG_ENABLE:
    logger = Log(
        name= __name__,
        console_handler_filter=lambda x: AXO_DEBUG,
        path = AXO_LOG_PATH
    )
else:
    logger = logging.getLogger(__name__)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    logger.setLevel(logging.DEBUG)



def activex_method(f):

    @wraps(f)
    def __activex(self:ActiveX,*args,**kwargs):
        try:
            start_time                 = T.time()
            runtime                    = get_runtime()
            endpoint                   = runtime.endpoint_manager.get_endpoint(endpoint_id= kwargs.get("endpoint_id",""))
            kwargs["endpoint_id"]      = endpoint.endpoint_id
            kwargs["axo_key"]          = self.get_axo_key()
            kwargs["axo_bucket_id"]    = self.get_axo_bucket_id()
            kwargs["sink_bucket_id"]   = self.get_sink_bucket_id()
            kwargs["source_bucket_id"] = self.get_source_bucket_id()
            # kwargs = {**kwargs}

            logger.debug({
                "event":"METHOD.EXECUTION",
                "remote":self._acx_remote, 
                "local":self._acx_local, 
                "fname":f.__name__,
                "endpoint_id":endpoint.endpoint_id,
                "axo_key":kwargs.get("axo_key"),
                "axo_bucket_id":kwargs.get("axo_bucket_id"),
                "sink_bucket_id":kwargs.get("sink_bucket_id"),
                "source_bucket_id":kwargs.get("source_bucket_id"),
            })
            res = endpoint.method_execution(
                key=self.get_axo_key(),
                fname=f.__name__,
                ao=self,
                f= f,
                # f= f_serialized,
                fargs=args,
                fkwargs=kwargs
            )
            logger.info({
                "event":"METHOD.EXECUTION",
                "remote":self._acx_remote, 
                "local":self._acx_local, 
                "fname":f.__name__,
                "endpoint_id":endpoint.endpoint_id,
                "axo_key":kwargs.get("axo_key"),
                "axo_bucket_id":kwargs.get("axo_bucket_id"),
                "sink_bucket_id":kwargs.get("sink_bucket_id"),
                "source_bucket_id":kwargs.get("source_bucket_id"),
                "response_time":T.time() - start_time
            })
            return res
        
        except Exception as e:
            logger.error(str(e))
    return __activex


def generate_id(v:str)->str:
    if v == None or v == "":
        return nanoid(alphabet=ALPHABET, size=AXO_ID_SIZE)
    return re.sub(r'[^a-z0-9_]', '', v)
def generate_id_size(size:int=AXO_ID_SIZE):
    def __in(v:str)->str:
        print(v,size)
        if v == None or v == "":
            return nanoid(alphabet=ALPHABET, size=size)
        return re.sub(r'[^a-z0-9_]', '', v)
    return __in
    # return generate_id(v=)


ActiveXObjectId = Annotated[Optional[str], AfterValidator(generate_id_size(AXO_ID_SIZE))]

class MetadataX(BaseModel):
    path:ClassVar[str]        = os.environ.get("ACTIVE_LOCAL_PATH","/activex/data")
    source_path:ClassVar[str] = os.environ.get("AXO_SOURCE_PATH","/activex/source")
    sink_path:ClassVar[str]   = os.environ.get("AXO_SINK_PATH","/activex/sink")
    pivot_storage_node:Optional[str] = ""
    # replica_nodes:Set[str]           = Field(default_factory=set)
    is_read_only:bool      = False
    axo_key:ActiveXObjectId = ""
    module:str
    name:str
    class_name:str 
    version:str            = "v0"
    # This bucket is to store the object
    axo_bucket_id:str          = ""
    # 
    source_bucket_id:str   = ""
    sink_bucket_id:str     = ""

    # sink_keys:list[str]    = []
    endpoint_id:str        = ""
    dependencies:list[str] = []
    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        self.axo_key = nanoid(alphabet=ALPHABET, size=AXO_ID_SIZE)
        self.axo_bucket_id = nanoid(alphabet=ALPHABET,size=AXO_ID_SIZE*2)
        self.sink_bucket_id = nanoid(alphabet=ALPHABET,size=AXO_ID_SIZE*2)
        self.source_bucket_id = nanoid(alphabet=ALPHABET,size=AXO_ID_SIZE*2)
        
        
        # if self.axo_key == "":
            

    def to_json_with_string_values(self)->Dict[str,str]:
        json_data = self.model_dump()
        # Convert all values to strings
        for key, value in json_data.items():
            # if key == "replica_nodes"
            if isinstance(value, list):
                json_data[key] = ";".join(value)
            else:
                json_data[key] = str(value)

        return json_data

    # @validator("id", pre=True, always=True)
    # def generate_id(cls, v):
    #     return v if v is not None else nanoid(ALPHABET, size=ACTIVEX_OBJECT_ID_SIZE)
    
class ActiveX:
    _acx_metadata: MetadataX
    _acx_local:bool = True
    _acx_remote:bool = False
    

    def get_sink_path(self)->str:
        return "{}/{}".format(self._acx_metadata.sink_path,self.get_sink_bucket_id())
    def get_source_path(self)->str:
        return "{}/{}".format(self._acx_metadata.source_path,self.get_source_bucket_id())
    
    def get_dependencies(self)->List[str]:
        return self._acx_metadata.dependencies

    def set_dependencies(self,dependencies:List[str])->List[str]:
        self._acx_metadata.dependencies=list(set(dependencies))
        return self._acx_metadata.dependencies
    
    def append_dependencies(self,dependencies:List[str])->List[str]:
        A = set(dependencies)
        B = set(self._acx_metadata.dependencies)
        self._acx_metadata.dependencies = list(A.union(B))
        return self._acx_metadata.dependencies

    def get_axo_key(self):
        return self._acx_metadata.axo_key
    
    #  AXO Bucket ID
    def get_axo_bucket_id(self):
        return self._acx_metadata.axo_bucket_id

    # Sink bucket_id
    def set_sink_bucket_id(self,sink_bucket_id:str="")->str:
        self._acx_metadata.sink_bucket_id = generate_id_size(size=AXO_ID_SIZE*2)(v = sink_bucket_id)
        return self._acx_metadata.sink_bucket_id
    
    def get_sink_bucket_id(self)->str:
        if self._acx_metadata.sink_bucket_id == "":
            return self.set_sink_bucket_id(sink_bucket_id="")
        return self._acx_metadata.sink_bucket_id
    # _________________________________________________
    # Source bucket_id

    def set_source_bucket_id(self,source_bucket_id:str="")->str:
        self._acx_metadata.source_bucket_id = generate_id_size(size=AXO_ID_SIZE*2)(v = source_bucket_id)
        return self._acx_metadata.source_bucket_id

    def get_source_bucket_id(self)->str:
        if self._acx_metadata.source_bucket_id == "":
            return self.set_source_bucket_id(source_bucket_id="")
        return self._acx_metadata.source_bucket_id
    # _________________________________________________

    
    def set_endpoint_id(self,endpoint_id:str="")->str:
        if endpoint_id =="":
            _endpoint_id = "activex-endpoint-{}".format(generate_id_size(8)(v=endpoint_id))
        else:
            _endpoint_id = endpoint_id
        
        self._acx_metadata.endpoint_id = _endpoint_id
        return self._acx_metadata.endpoint_id
    
    def get_endpoint_id(self)->str:
        if self._acx_metadata.endpoint_id =="":
            return self.set_endpoint_id()
        return self._acx_metadata.endpoint_id

    def __init_subclass__(cls, **kwargs):
        pass
        # logger.debug({
        #     "event":"INIT.SUBCLASS",
        #     "class_name":cls.__name__
        # })
        # logger.debug(f"Subclass {cls.__name__} created.")


    def __new__(cls,*args,**kwargs):
        obj = super().__new__(cls)
        class_name = cls.__module__ + "." + cls.__name__
        module = cls.__module__
        name = cls.__name__
        obj._acx_metadata = MetadataX(
            class_name= class_name,
            module= module,
            name= name,

            # id= None
        )
        runtime= get_runtime()
        # obj.get_source_bucket_id()
        # obj.get_sink_bucket_id()
        obj.set_endpoint_id(endpoint_id=runtime.endpoint_manager.get_endpoint().endpoint_id)
        # obj.get_sink_key()
        # obj.get_sink_keys()
        # logger.debug({
        #     "event":"NEW",
        #     "class_name":cls.__name__,
        #     "module":module,
        #     # "name":name
        # })
        return obj

        
    def __init__(cls,
                #  object_id:str, 
                 tags:Dict[str,str]={},
                 version:str  = "v0"
    ):     
        obj = super().__new__(cls)
        # obj.object_id = nanoid(ALPHABET) if object_id =="" else object_id
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
    def get_by_key(key:str,bucket_id:str="")->Result[ActiveX,Exception]:
        return get_runtime().get_by_key(key=key,bucket_id=bucket_id)

    def persistify(self,bucket_id:str="",key:str="")->Result[str, Exception]:
        try:
            start_time = T.time()
            _key       = self.get_axo_key() if key == "" else key
            _bucket_id = self.get_axo_bucket_id() if bucket_id =="" else bucket_id
            runtime    = get_runtime()
            persistify_result = runtime.persistify(
                instance  = self,
                bucket_id = _bucket_id,
                key       = _key
            )
            self._acx_remote = persistify_result.is_ok
            self._acx_local = persistify_result.is_err
            logger.info({
                "event":"PERSISTIFY",
                "axo_bucket_id":_bucket_id,
                "axo_key":_key,
                "source_bucket_id":self.get_source_bucket_id(),
                "sink_bucket_id":self.get_sink_bucket_id(),
                "response_time":T.time()- start_time
            })
            return persistify_result
        except Exception as e:
            return Err(e)





# def resolve_annotations(self:ActiveX):
#     runtime = get_runtime()
#     logger.debug("RESOLVE_ANNOTATIONS {}".format(self))
#     # cls = self.__class__
#     attributes = self.__dict__
#     # print(attributes)
#     T.sleep(2)
#     for attr_name, attr_value in attributes.items():
#         # attr_type = type(attr_value)
#         attr_type_hint = self.__annotations__.get(attr_name)
#         # print("ATTR_TYPE",attr_type_hint)
#         # Check if the annotated key is GetKey
#         T.sleep(2)
#         if attr_type_hint == Annotated[str,GetKey]:
#             value = getattr(self,attr_name)
#             logger.debug("GET {}".format(value))
#             result = runtime.storage_service.get_data_to_file(
#                 key=value
#             )
#             print("RESULT",result)
#             T.sleep(2)
#             if result.is_err:
#                 logger.error("{} not found in the storage service".format(value))
#                 # raise Exception("" .format(value))
        
#         # Check if the annotated key is PutPath
#         if attr_type_hint == Annotated[str,PutPath]:
#             value = getattr(self,attr_name)
#             logger.debug("SCHEDULE.TASK {}".format(value))
#             task = Task(
#                 operation="PUT",
#                 executed_at= T.time() + HF.parse_timespan(ACX_ENQUEUE_EXTRA_SECS) ,
#                 metadata={
#                     "path":value 
#                 } 
#             )
#             # Schedule a put task.
#             runtime.scheduler.schedule(
#                 task=task
#             )
#     print("FINISH")
#     T.sleep(5)
            