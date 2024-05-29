

from activex.handler import ActiveXHandler

# from activex.storage import Put
import logging
import more_itertools as mit
import sys
# 
import time as T
from activex import ActiveX,activex_method
from activex.storage.mictlanx import GetKey,PutPath
from typing import Annotated, Dict,Generator,Tuple
import numpy.typing as npt  
import pandas as pd
import numpy  as np
from nanoid import generate as nanoid
import string
from activex.runtime import get_runtime
from rory.core.security.dataowner import DataOwner
from rory.core.security.cryptosystem.liu import Liu
from rory.core.clustering.secure.distributed.skmeans import SKMeans
# from rory

logger          = logging.getLogger(__name__)
formatter       = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.setLevel(logging.DEBUG)
ALPHABET = string.digits+ string.ascii_lowercase

args = sys.argv[1:]


class SkmeanX(ActiveX):
    # encrypted_data_id:Annotated[str,GetKey]
    # udm_id:Annotated[str,GetKey]
    def __init__(self,
                #  encrypted_data_id:Annotated[str,GetKey], 
                #  udm_id:Annotated[str,GetKey], 
                he_id:str,
                 k:int = 3,
                 m:int= 3 
    ):
        # self.encrypted_data_id = encrypted_data_id
        # self.udm_id = udm_id
        self.he_id = he_id
        self.k = k
        self.m = m
    @activex_method
    def run(self,status:int=0):
        skmeans =SKMeans()
        h3_result = HomomorphicCipher.get_by_key(key=self.he_id )
        if h3_result.is_ok:
            h3:HomomorphicCipher  = h3_result.unwrap()
            run1_reuslt = skmeans.run1(status=status, k = self.k , m=self.m,encryptedMatrix=h3.encrypted_chunk,UDM=h3.udm)
            print("RUN1_RESULT",run1_reuslt)

            # print("UDM", h3.udm)
        # print("---------H3", h3)
        # encrypted_matrix = np.load("/activex/data/{}.npy".format(self.encrypted_data_id))
        # udm = np.load("/activex/data/{}.npy".format(self.udm_id))
    

class HomomorphicCipher(ActiveX):
    encrypted_data_path:Annotated[str,PutPath] 
    udm_path:Annotated[str,PutPath] 
    # = "/activex/data/encrypted_chunk.npy"
    def __init__(self,plaintext:npt.NDArray):
        # super().__init__(object_id="",tags={})
        self.encrypted_data_path = "/activex/data/{}.npy".format(self._acx_metadata.id)
        self.udm_path ="/activex/data/{}_udm.npy".format(self._acx_metadata.id)
        self.plaintext = plaintext
        self.dataowner = DataOwner(
            liu_scheme= Liu()
        )
        self.udm = None
        self.encrypted_chunk = None
    
    @activex_method
    def liu_encrypt(self)->npt.NDArray:
        x = self.dataowner.liu_encrypt_matrix_chunk(plaintext_matrix=self.plaintext)
        np.save(self.encrypted_data_path, x)
        self.encrypted_chunk = x
        return x
    @activex_method
    def calculate_udm(self,mode:str="diff")->npt.NDArray:
        x= self.dataowner.calculate_UDM(plaintext_matrix= self.plaintext,mode=mode)
        np.save(self.udm_path, x)
        self.udm = x
        return x
    
        

class Slicer(ActiveX):
    input_data_id:Annotated[str,GetKey] 

    def __init__(self,input_data_id:Annotated[str,GetKey],format:str = "npy",chunk_prefix:str = nanoid(alphabet=ALPHABET)):
        self.path = "/activex/data/{}.npy".format(input_data_id)
        self.input_data_id = input_data_id
        self.format = format
        self.base_path = "/activex/data"
        self.chunk_prefix = chunk_prefix
    @staticmethod
    def read_ndarray_in_chunks(array:npt.NDArray, chunk_size:int = 2)->Generator[npt.NDArray, None, None]:
        # Calculate the number of chunks
        num_chunks = len(array) // chunk_size + (1 if len(array) % chunk_size != 0 else 0)
        # Iterate over the chunks and process each chunk
        for i in range(num_chunks):
            if i == num_chunks-2:
                print("LAST CHUNK")
            start = i * chunk_size
            end = min((i + 1) * chunk_size, len(array))
            chunk = array[start:end]
            yield i,chunk
            # print(chunk)
    @activex_method
    def slice(self,shape:Tuple[int,int],dtype:str="float32",num_chunks:int=2)->Generator[Tuple[int, npt.NDArray], None,None]:
        if self.format == "csv":
            reader = pd.read_csv(self.path,chunksize=num_chunks)
            for chunk in reader:
                yield chunk
        elif self.format == "npy":
            # with open(self.path,"rb") as f :
            plaintext_ndarray = np.memmap(self.path, dtype=dtype, mode="r", shape=shape )
            yield from Slicer.read_ndarray_in_chunks(array=plaintext_ndarray,chunk_size=num_chunks)
            # for chunk_index,chunk in Slicer.read_ndarray_in_chunks(array=plaintext_ndarray,chunk_size=num_chunks):
                # h = HomomorphicCipher(plaintext_path="{}/{}-{}".format(self.base_path,self.chunk_prefix,chunk_index))


            

        



def main():
    """ 
        main: Function that run the following steps:

        1. Init the ActiveX handler (important!)
        There are 2 context where the handler can run code: Local and Distributed. 

        Local: Execute the object method locally and save the objects in memory.
        Distributed: Executes the object method on endpoints and save it in an object store.

        The distributed context is in working progress, for now the method execution is local, but
        the object are saved in MictlanX (read more about https://github.com/muyal-research-group/mictlanx-client)

        2. Create an object instance as usual of a Plotter.
        2.1 Perform some number crunching using the add,substract.. methods implemented in Plotter object.
        3. Call the method persistify in the plotter instance to allocate the object in MictlanX
    """
    key:str = mit.nth(args, 0, "")

    logger.debug("Step 1. init ActiveX object handler")
    _ = ActiveXHandler.distributed()
    logger.debug("Step 2. create an object instance")
    # The allocation of the input data occurs in another component
    runtime = get_runtime()
    input_data_id = "clusteringc0r1001a12k517"
    input_data_result = runtime.storage_service.put_data_from_file(
        key=input_data_id,
        source_path="examples/data/{}.npy".format(input_data_id)
    )
    print(input_data_result)
    #  ______________________________________________________________________________________________
    slicer = Slicer(format="npy",input_data_id=input_data_id )
    chunks = slicer.slice(shape=(1001,12),num_chunks=250)

    for (chunk_index, chunk) in chunks:
        he = HomomorphicCipher(plaintext=chunk)
        print(he.plaintext.shape)
        # he.liu_encrypt()
        # he.calculate_udm()
        # he.plaintext = None
        # T.sleep(10)
        # he_result = he.persistify()
        # print("H3_RSULT", he_result)
        # sk = SkmeanX(he_id= he._acx_metadata.id)
        # sk.run()
        # T.sleep(30)
    T.sleep(100)
    

    
if __name__  == "__main__":
    main()
