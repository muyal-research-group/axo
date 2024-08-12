
from abc import ABC,abstractmethod
from nanoid import generate as nanoid
import string 
from dataclasses import dataclass
from activex import Axo
from option import Result, Ok,Err
from enum import Enum
from typing import List
import json as J
ALPHABET =  string.ascii_lowercase + string.digits
from activex.runtime import get_runtime

class PatternX(ABC):
    pass

class SourceX(ABC):
    def __init__(self,id:str):
        self.id = id
        # self.id = nanoid(alphabet=ALPHABET) if id =="" else id

    @abstractmethod
    def to_sink(self)->'SinkX':
        pass
    @abstractmethod
    def reset(self,id:str="")->'SourceX':
        pass

    def __str__(self):
        return "SourceX(id={})".format(self.id)
class SinkX(ABC):
    def __init__(self,id:str):
        self.id = id
    @abstractmethod
    def to_source(self)->SourceX:
        pass

    @abstractmethod
    def reset(self,id:str="")->'SinkX':
        pass
    def __str__(self):
        return "SinkX(id={})".format(self.id)

class BucketSource(SourceX):
    def __init__(self, bucket_id:str=""):
        super().__init__(id=nanoid(alphabet=ALPHABET) if bucket_id =="" else bucket_id)
    def reset(self,id:str = "") -> SourceX:
        return BucketSource(bucket_id=id)
    def to_sink(self) -> SinkX:
        return BucketSink(bucket_id=self.id)
    

class BucketSink(SinkX):
    def __init__(self, bucket_id:str=""):
        # super().__init__(id=bucket_id)

        super().__init__(id=nanoid(alphabet=ALPHABET) if bucket_id =="" else bucket_id)
    def reset(self, id: str="") -> SinkX:
        return BucketSink(bucket_id=id)
    def to_source(self) -> SourceX:
        return BucketSource(bucket_id=self.id)


@dataclass
class FilterXOut:
    source_bukcet_id:str = nanoid(alphabet=ALPHABET,size=16)
    sink_bucket_id:str   = nanoid(alphabet=ALPHABET,size=16)
    response_time:int    = -1

class FilterX(Axo,ABC):
    def __init__(self,source:SourceX =BucketSource(), sink:SinkX = BucketSink()):
        self.source = source
        self.sink   =  sink
        self.set_source_bucket_id(source_bucket_id=self.source.id)
        self.set_sink_bucket_id(sink_bucket_id=self.sink.id)

    @abstractmethod
    def run(self,*args,**kwargs)->FilterXOut:
        pass
    def __str__(self) -> str:
        return "FilterX(source = {}, sink = {})".format(self.source, self.sink)

class WorkerX(Axo,ABC):
    @abstractmethod
    def run(self,*args,**kwargs):
        pass
    def __str__(self) -> str:
        return "WorkerX(source = {}, sink = {})".format(self.source, self.sink)


class ManagerWorkerX(PatternX):
    def __init__(self,
                 
                 source_bucket_id:str,
                 workers:List[WorkerX]=[],
                 sink_buckets_ids:List[str]=[]
    ):
        super().__init__()
        self.source_bucket_id = source_bucket_id
        self.workers =workers
        print(self.workers)
        self.sink_buckets_ids = sink_buckets_ids
    
    def add_worker(self,worker:WorkerX):
        self.workers.append(worker)

    def run(self):
        runtime  = get_runtime()
        # endpoint = runtime.endpoint_manager.get_endpoint()
        # endpoint.
        metadatas_result = runtime.storage_service.get_bucket_metadata(bucket_id=self.source_bucket_id)
        if metadatas_result.is_err:
            return Err(False)
        metadatas = metadatas_result.unwrap()
        num_workers = len(self.workers)
        for i,metadata in enumerate(metadatas):
            worker_index = i%num_workers
            w = self.workers[worker_index]
            w.source = self.source_bucket_id
            w.source_keys.append(metadata.key)
            print("Worker ",worker_index, w.source, w.source_keys)
            print("*"*50)
        for i, w in enumerate(self.workers):
            res = w.run()
            print(i, res)



class PipeAndFilter(PatternX):
    def __init__(self,
                 source:SourceX= BucketSource(), 
                 sink:SinkX = BucketSink()
    ):
        self.source:SourceX = source
        self.sink:SinkX = sink
        self.filters:List[FilterX] = []

    def set_source(self, source:SourceX):
        self.source =source

    def set_sink(self, sink:SinkX):
        self.sink =sink

    def add_filter(self,filter:FilterX,root_as_source:bool = True):
        filters_len = len(self.filters)
        if  filters_len == 0:
            filter.source = self.source if not root_as_source else filter.source
            filter.set_source_bucket_id(source_bucket_id=filter.source.id)

            filter.sink = self.sink
            filter.set_sink_bucket_id(sink_bucket_id=filter.sink.id)

            self.filters.append(filter)
            return
        

        if filters_len >= 1:
            last_filter = self.filters[filters_len - 1] 
            last_filter.sink = filter.sink
            last_filter.set_sink_bucket_id(sink_bucket_id=last_filter.sink.id)

            filter.source = last_filter.sink.to_source()
            filter.sink = self.sink
            filter.set_source_bucket_id(source_bucket_id= filter.source.id)
            filter.set_sink_bucket_id(sink_bucket_id=filter.sink.id)
            self.filters.append(filter)

    def run(self):
        if not self.source:
            return Err(Exception("No source has been attached to the pattern"))
        responses = []
        print("_"*50)
        for index,f in enumerate(self.filters):
            # print("BEGORE", f)
            persistify_res = f.persistify()
            print("FilterX[{}] =  {}".format(index, persistify_res))
            res = f.run()
            responses.append(res)
        print("RUN_FINISHEd")
        return responses
    def __str__(self) -> str:
        filters = [ str(f) for f in self.filters]

        return J.dumps({
            "source": str(self.source),
            "sink": str(self.sink),
            "filter": filters
        }, indent=4)
        # return s
        



