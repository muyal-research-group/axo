from activex.runtime import set_runtime,get_runtime
from activex.runtime.local import LocalRuntime
from activex.runtime.distributed import DistributedRuntime
from activex.runtime.runtime import ActiveXRuntime
from typing import Optional
from nanoid import generate as nanoid
import string
class ActiveXHandler:
    is_running = False
    def __init__(self, runtime:Optional[ActiveXRuntime] = None):
        self.prev_runtime = get_runtime()
        if runtime == None:
            self.runtime = LocalRuntime(
                runtime_id="local-{}".format(nanoid(alphabet=string.ascii_lowercase+string.digits))
            )
        else:
            self.runtime = runtime
        set_runtime(self.runtime)
        self.is_running = True
    @staticmethod
    def distributed()->'ActiveXHandler':
        return ActiveXHandler(
            runtime= DistributedRuntime(
                runtime_id="distributed-{}".format(nanoid(alphabet=string.ascii_lowercase+string.digits))
            )
        )
        
    def stop(self):
        if not self.is_running:
            return
        self.runtime.stop()
        set_runtime(self.prev_runtime)
        self.is_running=False
    def __del__(self):
        self.stop()