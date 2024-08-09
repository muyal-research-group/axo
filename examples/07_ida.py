import time as T
from activex import Axo,axo_method
from typing import Generator,Any,List
from activex.contextmanager import ActiveXContextManager
import humanfriendly as  HF
from activex.endpoint import XoloEndpointManager
import cloudpickle as CP
from mictlanx.v4.client import Client
from mictlanx.utils.index import Utils as UtilsX
from concurrent.futures import ProcessPoolExecutor,as_completed

import galois
import numpy as np
import logging
import os 
import inspect
AXO_ENDPOINT_ID           = os.environ.get("AXO_ENDPOINT_ID","activex-endpoint-0")
AXO_ENDPOINT_PROTOCOL     = os.environ.get("AXO_ENDPOINT_PROTOCOL","tcp")
AXO_ENDPOINT_HOSTNAME     = os.environ.get("AXO_ENDPOINT_HOSTNAME","localhost")
AXO_ENDPOINT_PUBSUB_PORT  = int(os.environ.get("AXO_ENDPOINT_PUBSUB_PORT","16000"))
AXO_ENDPOINT_REQ_RES_PORT = int(os.environ.get("AXO_ENDPOINT_REQ_RES_PORT","16667"))
# logger          = logging.getLogger(__name__)
# formatter       = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# console_handler = logging.StreamHandler()
# console_handler.setFormatter(formatter)
# logger.addHandler(console_handler)
# logger.setLevel(logging.DEBUG)
MICTLANX_CLIENT_ID         = os.environ.get("MICTLANX_CLIENT_ID","client-0-activex")
MICTLANX_DEFAULT_BUCKET_ID = os.environ.get("MICTLANX_DEFAULT_BUCKET_ID","jbddhwnqf606qgwr1ix42vayqmsl1ejl")
MICTLANX_DEBUG             = bool(int(os.environ.get("MICTLANX_DEBUG","1")))
MICTLANX_MAX_WORKERS       = int(os.environ.get("MICTLANX_MAX_WORKERS","2"))
MICTLANX_LOG_PATH          = os.environ.get("MICTLANX_LOG_PATH","./log")
SOURCE_PATH                = os.environ.get("SOURCE_PATH","./source")
routers = list(UtilsX.routers_from_str(os.environ.get("MICTLANX_ROUTERS","mictlanx-router-0:localhost:60666")))
client = Client(
    client_id      = MICTLANX_CLIENT_ID,
    routers         = routers,
    debug           = MICTLANX_DEBUG,
    max_workers     = MICTLANX_MAX_WORKERS,
    bucket_id       = MICTLANX_DEFAULT_BUCKET_ID,
    log_output_path = MICTLANX_LOG_PATH    
)

class IDAx:
# class IDAx(ActiveX):
    def __init__(self,k:int=3,n:int=5,p:int=8,verify:bool= False):
        
        # Define the Galois field
        # alpha = galois.primitive_root(n=p,start=1, stop=None, method="min")
        self.GF = galois.GF(2**p,verify=verify)
        self.n:int = n  # Number of shares
        self.k:int = k  # Threshold for reconstruction
        self.max_workers =2
        # self.pool = ProcessPoolExecutor(max_workers=self.max_workers)
    
    # @activex_method
    def test(self,*args,**kwargs):
        return "TESTING RETURN!!!"
        # return self.pool.submit(lambda x: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAHHHHHH!!!")


    def generate_vandermonde_matrix(self,k:int,n:int,*args,**kwargs):
        """
        Generate a k x n Vandermonde matrix over the specified Galois field.
        """
        # print("k",k)
        # print("n",n)
        x = self.GF.elements[:n]
        # print("x",x)
        V = self.GF.Zeros((n, k))
        # print("V",V)
        for i in range(n):
            for j in range(k):
                V[i, j] = x[i] ** j
        return V

    def encode_data(self,data,*args,**kwargs):
        """
        Encode the data using a Vandermonde matrix to create n shares.
        Each byte of data is treated as a separate element in the field.
        """
        t1= T.time()
        data_size = len(data)
        block_size  = (data_size + self.k - 1) // self.k
        # print("block_size", block_size,"data_size", data_size)
        padded_data = data.ljust(block_size * self.k, b'\0')
        blocks = [
            padded_data[i * block_size:(i + 1) * block_size] for i in range(self.k)
        ]
        matrix = self.generate_vandermonde_matrix(k=self.k,n=self.n)
        shares = [self.GF.Zeros(block_size) for _ in range(self.n)]

        for i in range(block_size):
            for j in range(self.n):
                for m in range(self.k):
                    v = self.GF(blocks[m][i]) * matrix[j, m]
                    # print(i,j,m, v)
                    shares[j][i] += v
        
        # print("encode","time",T.time() - t1)
        return [bytes(share) for share in shares]

    # @activex_method
    def encode_data_to_file(self,*args,**kwargs):
        """
        Encode the data using a Vandermonde matrix to create n shares.
        Each byte of data is treated as a separate element in the field.
        """
        # axo_sink_path    = self.get_sink_path()
        axo_sink_path_sink_bucket_id_path = kwargs.get("axo_sink_path_sink_bucket_id_path","/sink/")
        axo_sink_key:str = kwargs.get("axo_sink_key","sharex")
        # print("AXO_SINK_PATH",axo_sink_path)
        axo_source_path  = kwargs.get("axo_source_path","/source/{}".format(axo_sink_key))
        print("ARGS",args)
        print("KWARGS",kwargs)
        print("SELF",self)
        print(self.generate_vandermonde_matrix)
        print(inspect.signature(self.generate_vandermonde_matrix))
        print("AXO_SINK_PATH", axo_sink_path_sink_bucket_id_path)
        print("AXO_SIN_KEU", axo_sink_key)
        print("AXO_SOURCE_PATH",axo_source_path)
        print("K: {} N:{}".format(self.k, self.n))
        with open(axo_source_path,"rb") as f:
            data = f.read()
        data_size = len(data)
        print("SIZE",data_size)
        block_size  = (data_size + self.k - 1) // self.k
        print("BLOCK_SIZE",block_size)
        padded_data = data.ljust(block_size * self.k, b'\0')
        print("PADDED_DATA", len(padded_data))
        blocks = [
            padded_data[i * block_size:(i + 1) * block_size] for i in range(self.k)
        ]
        print("BLOCKS",len(blocks))
        matrix = self.generate_vandermonde_matrix(k=self.k,n=self.n)
        print("matrix", matrix.size)
        shares = [self.GF.Zeros(block_size) for _ in range(self.n)]

        for i in range(block_size):
            for j in range(self.n):
                for m in range(self.k):
                    v = self.GF(blocks[m][i]) * matrix[j, m]
                    shares[j][i] += v
        
        for index,share in enumerate(shares):
            share_path = "{}/{}_{}".format(axo_sink_path_sink_bucket_id_path,axo_sink_key,index)
            print("SHARE_PATH", share_path)
            with open(share_path,"wb") as f:
                bytes_shares = bytes(share)
                f.write(bytes_shares)
            
        # return [bytes(share) for share in shares]
    def decode_data(self,shares):
        """
        Decode the original data from k shares using the inverse of the Vandermonde matrix.
        """
        # t1 = T.time()
        try:
            block_size   = len(shares[0])
            x            = self.GF.elements[:self.k]
            print("BEFORE","VENDERMON_MATRIX")
            matrix       = self.generate_vandermonde_matrix(k=self.k,n= self.k)
            print("AFTER","VENDERMON_MATRIX")
            matrix_inv   = np.linalg.inv(matrix)
            decoded_data = bytearray(block_size * self.k)

            print("BEFORE","FOR_BS")
            for i in range(block_size):
                column = self.GF([share[i] for share in shares[:self.k]])
                decoded_column = matrix_inv @ column

                # print(column,decoded_column)
                for j, value in enumerate(decoded_column):
                    index = j * block_size + i
                    decoded_data[index] = value
            
            print("DECODED")
            # print("encode","time",T.time() - t1)
            return bytes(decoded_data).rstrip(b'\0')
        except Exception as e:
            print(e)

class Splitter:
    def to_chunks(self,data, chunk_size:str="1MB")->Generator[bytes,Any,Any]:
        """
        Generator that yields chunks of data.
        
        Args:
        - data: The data to be chunked.
        - chunk_size: The size of each chunk in bytes.
        
        Yields:
        - Chunks of data of the specified chunk size.
        """
        cs = HF.parse_size(chunk_size)
        for i in range(0, len(data), cs):
            yield data[i:i + cs]
def fx(index:int,data:bytes):
    t1  = T.time()
    idax = IDAx(p=8)
    res = idax.encode_data(data)
    print("encode","index",index,"time",T.time()-t1)
    t2 = T.time()
    res = idax.decode_data(res)
    print("decode","index",index,"time",T.time()-t2)
    return res
    

def main():

    # class_definition_bytes = CP.dumps(IDAx)

    sptx = Splitter()
    with open("/source/nodonkey.gif","rb") as f:
        data = f.read()
    # data = b""
    # print(len)
    chunks = sptx.to_chunks(data,chunk_size="10kb")


    with ProcessPoolExecutor(max_workers=6) as executor:
        futures = []
        t1 = T.time()
        for index,chunk in enumerate(chunks):
            fut = executor.submit(fx,index,chunk)
            futures.append(fut)
        for fut in as_completed(futures):
            res = fut.result()
            print(res)
        print("COMPLETED", T.time() - t1)
        #     executor.submit(idax.decode_data, res)

def mainx():
    endpoint_manager = XoloEndpointManager(
        endpoint_id=AXO_ENDPOINT_ID,
        endpoints={}
    )
    endpoint_manager.add_endpoint(
        endpoint_id= AXO_ENDPOINT_ID,
        hostname=AXO_ENDPOINT_HOSTNAME,
        protocol=AXO_ENDPOINT_PROTOCOL,
        pubsub_port=AXO_ENDPOINT_PUBSUB_PORT,
        req_res_port=AXO_ENDPOINT_REQ_RES_PORT
    )
    axcm = ActiveXContextManager.distributed(
        endpoint_manager= endpoint_manager
    )
    # # axcm = ActiveXContextManager.local()
    # # Example usage
    # data = b"Secret data to be split into shares. LOTS OF SHARXX"
    idax = IDAx(n=5,k=3)
    # # res = idax.encode_data()
    x = idax.persistify()
    print("PERSISTY", x)
    # x = idax.to_bytes()
    # obj = ActiveX.from_bytes(x).unwrap()
    # print("TEST_CALLED",ActiveX.call(obj,"test" ))
    # print(struct.unpack("!ssss",idax.to_bytes()))
    # res =  client.put(
    #     bucket_id="jbddhwnqf606qgwr1ix42vayqmsl1ejl",
    #     key="class_definition1",
    #     value=class_definition_bytes
    # )
    # print("RESSS",res)
    # code_bytes = inspect.getsource(IDAx).encode()
    # res =  client.put(
    #     bucket_id="jbddhwnqf606qgwr1ix42vayqmsl1ejl",
    #     key="class_definition_code",
    #     value=code_bytes
    # )
    # print("RESSS",res)
    T.sleep(1000)
    # idax_bytes = idax.to_bytes()
    # print(idax_bytes)
    # obj = CP.loads(idax_bytes)
    # print(obj.GF)
    # idax.set_dependencies(["numpy==1.26","numba==0.59.1","galois"])
   
    # shares = idax.encode_data(data)
    # logger.debug("Shares: {}".format([share.hex() for share in shares]))

    # # Decode data (using any k shares)
    # reconstructed_data = idax.decode_data(shares[:idax.k])
    # logger.debug("Original Data: {}".format(data))
    # logger.debug("Reconstructed Data: {}".format(reconstructed_data))


if __name__ =="__main__":
    main()