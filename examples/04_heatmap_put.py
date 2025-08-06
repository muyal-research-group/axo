
from axo.contextmanager import AxoContextManager
from axo.runtime import get_runtime
import logging
from common import HeatmapProducer
import more_itertools as mit
import time as T
import sys

logger          = logging.getLogger(__name__)
formatter       = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.setLevel(logging.DEBUG)

args = sys.argv[1:]

def main():
    """ 
        main: Function that run the following steps:

        1. Init the ActiveX handler (important!)
        There are 2 context where the handler can run code: Local and Distributed. 

        Local: Execute the object method locally and save the objects in memory.
        Distributed: Executes the object method on endpoints and save it in an object store.

        The distributed context is in working progress, for now the method execution is local, but
        the object are saved in MictlanX (read more about https://github.com/muyal-research-group/mictlanx-client)

        2. Create an object instance as usual of a HeatmapProducer.
        3. Call the method persistify in the plotter instance to allocate the object in MictlanX
    """
    key:str = mit.nth(args, 0, "")

    logger.debug("Step 1. init ActiveX object handler")
    _ = AxoContextManager.distributed()
    logger.debug("Step 1.1 Get the runtime")
    runtime  = get_runtime()
    
    logger.debug("Step 1.2 Put the input data (you can skip this step if u got already uploaded)")
    input_data_key = "{}inputdata".format(key)
    runtime.storage_service.put_data_from_file(
        key=input_data_key,
        source_path="examples/data/sample01.csv"
    )

    logger.debug("Step 2. create an object instance")
    obj = HeatmapProducer()
    
    obj.input_path="/activex/data/sample01.csv"
    obj.input_data_key =input_data_key 
    logger.debug("Step 2.1 Select a valid key for the input data (first upload to mictlanX): {}".format(obj.input_data_key))
    obj.heatmap_output_path = "/sink/hugoplot.png"
    logger.debug("Step 2.2 Select a valid path for the result of the object: {}".format(obj.heatmap_output_path))
    obj.plot(cas=obj.cas)
    logger.debug("Step 2.3 Execute plot() method")
    obj.persistify(key=key)
    # obj.persistify(key=key) 
    
if __name__  == "__main__":
    main()
