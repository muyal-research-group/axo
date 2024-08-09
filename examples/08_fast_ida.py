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

import logging
import os 
import inspect

import numpy as np
from numba import jit
import galois

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

# @jit(nopython=True)
# def fast_matrix_multiply(matrix, vector):
    # return np.dot(np.ascontiguousarray(matrix), np.ascontiguousarray(vector))

class IDAx:
    def __init__(self, k=3, n=5, p=8, verify=False):
        # Initialize the Galois field
        self.GF = galois.GF(2**p, verify=verify)
        self.n = n  # Number of shares
        self.k = k  # Threshold for reconstruction

    def generate_vandermonde_matrix(self):
        x = self.GF.primitive_elements[:self.n]
        V = self.GF.Zeros((self.n, self.k))
        for i in range(self.n):
            for j in range(self.k):
                V[i, j] = x[i] ** j
        return V

    def encode_data(self, data):
        block_size = (len(data) + self.k - 1) // self.k
        padded_data = data.ljust(block_size * self.k, b'\0')
        blocks = [padded_data[i * block_size:(i + 1) * block_size] for i in range(self.k)]
        matrix = self.generate_vandermonde_matrix()
        shares = [self.GF.Zeros(block_size) for _ in range(self.n)]

        for i in range(block_size):
            for j in range(self.n):
                share = self.GF(0)
                for m in range(self.k):
                    share += self.GF(blocks[m][i]) * matrix[j, m]
                shares[j][i] = share
        return [bytes(share) for share in shares]

    def decode_data(self, shares):
        block_size = len(shares[0])
        print("decode,before,gvm")
        matrix = self.generate_vandermonde_matrix()[:self.k, :]
        print("decode,before,avm")
        matrix_inv = np.linalg.inv(matrix)
        decoded_data = self.GF.Zeros(block_size * self.k)
        print("before,for")
        for i in range(block_size):
            column = self.GF([share[i] for share in shares[:self.k]])
            decoded_column =  matrix_inv @ column
            # fast_matrix_multiply(matrix_inv, column)
            for j, value in enumerate(decoded_column):
                decoded_data[j * block_size + i] = value

        return bytes(decoded_data).rstrip(b'\0')

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
    res = idax.decode_data(res[:idax.k])
    print("decode","index",index,"time",T.time()-t2)
    return res
    


def main():
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

    with open("/source/nodonkey.gif","rb") as f:
        data = f.read()
    # # axcm = ActiveXContextManager.local()
    # # Example usage
    # data = b"Secret data to be split into shares. LOTS OF SHARXX"
    chunks = Splitter().to_chunks(data=data, chunk_size="12kb")
    idax = IDAx(n=5,k=3)
    tx = T.time()
    result = bytearray()
    for chunk in chunks:
        t1 = T.time()
        res = idax.encode_data(chunk)
        print("encode", "time", T.time() - t1) 
        t1 = T.time()
        res = idax.decode_data(res)
        result.extend(res)
        print("decode", "time", T.time() - t1) 
    print("COMPLETED", "time",T.time() - tx, "size",len(result))

    T.sleep(1000)


if __name__ =="__main__":
    main()