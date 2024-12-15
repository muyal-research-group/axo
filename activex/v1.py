from __future__ import annotations
from option import Result,Ok,Err
import os
import inspect
import types 
from typing import TypeVar,Generic,Any,Iterator,Tuple,Type
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
import struct
R = TypeVar('R')


ALPHABET                  = string.ascii_lowercase+string.digits
AXO_ID_SIZE               =int(os.environ.get("AXO_ID_SIZE","16"))
AXO_DEBUG                 = bool(int(os.environ.get("AXO_DEBUG","1")))
AXO_PRODUCTION_LOG_ENABLE = bool(int(os.environ.get("AXO_PRODUCTION_LOG_ENABLE","0")))
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



def axo_method(f):

    @wraps(f)
    def __axo(self:Axo,*args,**kwargs):
        try:
            start_time                 = T.time()
            runtime                    = get_runtime()
            self.set_endpoint_id(endpoint_id=runtime.endpoint_manager.get_endpoint().endpoint_id)
            endpoint                   = runtime.endpoint_manager.get_endpoint(endpoint_id= kwargs.get("endpoint_id",""))
            kwargs["endpoint_id"]      = endpoint.endpoint_id
            kwargs["axo_key"]          = kwargs.get("axo_key",self.get_axo_key())
            kwargs["axo_bucket_id"]    = kwargs.get("axo_bucket_id",self.get_axo_bucket_id())
            kwargs["sink_bucket_id"]   = kwargs.get("sink_bucket_id",self.get_sink_bucket_id())
            kwargs["source_bucket_id"] = kwargs.get("source_bucket_id",self.get_source_bucket_id())
            if not runtime.is_distributed:
                kwargs["storage"] = runtime.storage_service

            if runtime.is_distributed and self._acx_local:
                self.persistify()
            


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
    __axo.original = f
    return __axo


def axo_task(f):

    @wraps(f)
    def __axo(self:Axo,*args,**kwargs):
        try:
            start_time                 = T.time()
            runtime                    = get_runtime()
            self.set_endpoint_id(endpoint_id=runtime.endpoint_manager.get_endpoint().endpoint_id)

            endpoint                   = runtime.endpoint_manager.get_endpoint(endpoint_id= kwargs.get("endpoint_id",""))
            logger.debug({
                "evevent":"GET.ENDPOINT",
                "endpoint_id":endpoint.endpoint_id
            })
            kwargs["endpoint_id"]      = endpoint.endpoint_id
            kwargs["axo_key"]          = kwargs.get("axo_key",self.get_axo_key())
            kwargs["axo_bucket_id"]    = kwargs.get("axo_bucket_id",self.get_axo_bucket_id())
            kwargs["sink_bucket_id"]   = kwargs.get("sink_bucket_id",self.get_sink_bucket_id())
            kwargs["source_bucket_id"] = kwargs.get("source_bucket_id",self.get_source_bucket_id())
            if not runtime.is_distributed:
                kwargs["storage"] = runtime.storage_service

            if runtime.is_distributed and self._acx_local:
                res = self.persistify()
                if res.is_err:
                    raise res.unwrap_err()
                # print("PERSISTIFY==========================", res)
            # raise Exception("BOOOOM!")
            


            logger.debug({
                "event":"TASK.EXECUTION",
                "remote":self._acx_remote, 
                "local":self._acx_local, 
                "fname":f.__name__,
                "endpoint_id":endpoint.endpoint_id,
                "axo_key":kwargs.get("axo_key"),
                "axo_bucket_id":kwargs.get("axo_bucket_id"),
                "sink_bucket_id":kwargs.get("sink_bucket_id"),
                "source_bucket_id":kwargs.get("source_bucket_id"),
            })
            response = endpoint.task_execution(
                task_function=f,
                payload={
                    "fname":f.__name__,
                    "axo_key":kwargs.get("axo_key"),
                    "axo_bucket_id":kwargs.get("axo_bucket_id"),
                    "sink_bucket_id":kwargs.get("sink_bucket_id"),
                    "source_bucket_id":kwargs.get("source_bucket_id"),
                }
            )
            print("RESPONSE", response)
            return Ok(True)
            
            # res = endpoint.method_execution(
            #     key=self.get_axo_key(),
            #     fname=f.__name__,
            #     ao=self,
            #     f= f,
            #     # f= f_serialized,
            #     fargs=args,
            #     fkwargs=kwargs
            # )
            # logger.info({
            #     "event":"METHOD.EXECUTION",
            #     "remote":self._acx_remote, 
            #     "local":self._acx_local, 
            #     "fname":f.__name__,
            #     "endpoint_id":endpoint.endpoint_id,
            #     "axo_key":kwargs.get("axo_key"),
            #     "axo_bucket_id":kwargs.get("axo_bucket_id"),
            #     "sink_bucket_id":kwargs.get("sink_bucket_id"),
            #     "source_bucket_id":kwargs.get("source_bucket_id"),
            #     "response_time":T.time() - start_time
            # })
            # return res
        
        except Exception as e:
            logger.error(str(e))
    __axo.original = f
    return __axo



def generate_id(v:str)->str:
    if v == None or v == "":
        return nanoid(alphabet=ALPHABET, size=AXO_ID_SIZE)
    return re.sub(r'[^a-z0-9_]', '', v)
def generate_id_size(size:int=AXO_ID_SIZE):
    def __in(v:str)->str:
        if v == None or v == "":
            return nanoid(alphabet=ALPHABET, size=size)
        return re.sub(r'[^a-z0-9_]', '', v)
    return __in
    # return generate_id(v=)


AxoObjectId = Annotated[Optional[str], AfterValidator(generate_id_size(AXO_ID_SIZE))]

class MetadataX(BaseModel):
    path:ClassVar[str]        = os.environ.get("ACTIVE_LOCAL_PATH","/activex/data")
    source_path:ClassVar[str] = os.environ.get("AXO_SOURCE_PATH","/activex/source")
    sink_path:ClassVar[str]   = os.environ.get("AXO_SINK_PATH","/activex/sink")
    pivot_storage_node:Optional[str] = ""
    # replica_nodes:Set[str]           = Field(default_factory=set)
    is_read_only:bool      = False
    axo_key:AxoObjectId = ""
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

    
class Axo:
    _acx_metadata: MetadataX
    _acx_local:bool = True
    _acx_remote:bool = False
    
    @staticmethod
    def call(instance,method_name:str,*args,**kwargs)->Result[R,Exception]:
        # print("methods",method_name,instance)
        try:
            if hasattr(instance,method_name):
                value = getattr(instance, method_name)
                is_callable = inspect.isfunction(value) or inspect.ismethod(value)
                if is_callable:
                    output = value(*args,**kwargs)
                    return Ok(output)
                else:
                    return Ok(value)
                # if 
                # return Ok(value(*args,kwargs)) if  else Ok(value)
            return Err(Exception("{} not found in the object instance.".format(method_name)))
        except Exception as e:
            return Err(e)

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

  

    def __new__(cls,*args,**kwargs):
        obj = super().__new__(cls)
        # class_name = cls.__module__ + "." + cls.__name__
        class_name = cls.__name__
        module = cls.__module__
        name = cls.__name__
        obj._acx_metadata = MetadataX(
            class_name= class_name,
            module= module,
            name= name,
        )

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
        

    # def to_bytes(self):
        # return CP.dumps(self)
    def to_bytes(self)->bytes:
        attrs            = self.__dict__
        methods          = dict([ (attr,getattr(self, attr) )  for attr in dir(self) if callable(getattr(self, attr))])
        attrs_bytes      = CP.dumps(attrs)
        methods_bytes    = CP.dumps(methods)
        class_def_bytes  = CP.dumps(self.__class__)
        class_code_str   = inspect.getsource(self.__class__)
        class_code_bytes = CP.dumps(class_code_str.encode())
        # pack             = struct.pack(self._acx_format,attrs_bytes, methods_bytes, class_def_bytes,class_code_bytes)
        # print(pack)
        packed_data = b''
        for data in (attrs_bytes, methods_bytes, class_def_bytes, class_code_bytes):
            # Prefix each part with its length (using 4 bytes for the length)
            packed_data += struct.pack('I', len(data)) + data
        # print(packed_data)
        return packed_data
        # return (attrs_bytes,methods_bytes)
        # return CP.dumps(self)

    def to_stream(self,chunk_size:str="1MB")->Generator[bytes,None,None]:
        return serialize_and_yield_chunks(obj=self, chunk_size=chunk_size)
    # def t


    @staticmethod
    def get_object_parts(raw_obj:bytes,original_f:bool=False)->Result[Tuple[
        Dict[str, Any], # 
        Dict[str, Any],
        Type[Axo],
        str 
    ],Exception]:
        try:
            index = 0
            unpacked_data = []
            while index < len(raw_obj):
                # Read length
                length = struct.unpack_from('I', raw_obj, index)[0]
                index += 4  # Move past the length field
                # Read data
                data = raw_obj[index:index+length]
                index += length
                unpacked_data.append(CP.loads(data))  # Deserialize each component
            attrs    = unpacked_data[0]
            methods = unpacked_data[1]
            class_df = unpacked_data[2]
            return Ok((attrs,methods,class_df,unpacked_data[-1]))
            # instance:ActiveX = class_df()
            # for attr_name, attr_value in attrs.items():
            #     if attr_name not in ('__class__', '__dict__', '__module__', '__weakref__'):
            #         setattr(instance, attr_name, attr_value)
            # for method_name, func in methods.items():
            #     if "original" in dir(func) and original_f:
            #         func = func.original
            #     bound_method = types.MethodType(func, instance)
            #     if method_name not in ('__class__', '__dict__', '__module__', '__weakref__'):
            #         setattr(instance, method_name, bound_method)
            # # f0       = methods["test"].original
            # return Ok(instance)
        except Exception as e:
            return Err(e)
    @staticmethod
    def from_bytes(raw_obj:bytes,original_f:bool=False)->Result[Axo,Exception]:
        try:
            index = 0
            # Attrs, Methods, ClassDefinition, ClassCode
            unpacked_data = []
            while index < len(raw_obj):
                # Read length
                length = struct.unpack_from('I', raw_obj, index)[0]
                index += 4  # Move past the length field
                # Read data
                data = raw_obj[index:index+length]
                index += length
                unpacked_data.append(CP.loads(data))  # Deserialize each component
            attrs    = unpacked_data[0]
            methods = unpacked_data[1]
            class_df = unpacked_data[2]
            instance:Axo = class_df()
            for attr_name, attr_value in attrs.items():
                if attr_name not in ('__class__', '__dict__', '__module__', '__weakref__'):
                    setattr(instance, attr_name, attr_value)
            for method_name, func in methods.items():
                if "original" in dir(func) and original_f:
                    func = func.original
                bound_method = types.MethodType(func, instance)
                if method_name not in ('__class__', '__dict__', '__module__', '__weakref__'):
                    setattr(instance, method_name, bound_method)
            # f0       = methods["test"].original
            return Ok(instance)
        except Exception as e:
            return Err(e)
        # return CP.loads(raw_obj)
    
    @staticmethod
    def get_by_key(key:str,bucket_id:str="")->Result[Axo,Exception]:
        return get_runtime().get_active_object(key=key,bucket_id=bucket_id)
    
    @staticmethod
    def class_def_persistify(class_def:Any,bucket_id:str="", key:str="")->Result[bool,Exception]:
        runtime = get_runtime()
        endpoint = runtime.endpoint_manager.get_endpoint()
        # print("ENDPOINT",endpoint)
        result = endpoint.add_class_definition(
            class_def=class_def,
            bucket_id=bucket_id,
            key=key
        )
        return result 
 
    def persistify(self,bucket_id:str="",key:str="")->Result[str, Exception]:
        try:
            start_time = T.time()
            _key       = self.get_axo_key() if key == "" else key
            _bucket_id = self.get_axo_bucket_id() if bucket_id =="" else bucket_id
            runtime    = get_runtime()
            endpoint = runtime.endpoint_manager.get_endpoint()
            # print("ENDPOINT",endpoint.endpoint_id)
            self.set_endpoint_id(endpoint_id=endpoint.endpoint_id)
            persistify_result = runtime.persistify(
                instance  = self,
                bucket_id = _bucket_id,
                key       = _key
            )
            self._acx_remote = persistify_result.is_ok
            self._acx_local = persistify_result.is_err
            if persistify_result.is_ok:
                logger.info({
                    "event":"PERSISTIFY",
                    "axo_bucket_id":_bucket_id,
                    "axo_key":_key,
                    "source_bucket_id":self.get_source_bucket_id(),
                    "sink_bucket_id":self.get_sink_bucket_id(),
                    "response_time":T.time()- start_time
                })
                return persistify_result
            logger.error({
                "event":"PERSISTIFY.FAILED",
                "reason":str(persistify_result.unwrap_err())
            })
            return persistify_result
        except Exception as e:
            return Err(e)




