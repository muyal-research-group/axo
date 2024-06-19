import time as T
from activex import ActiveX,activex_method
from activex.contextmanager import ActiveXContextManager
from activex.endpoint import XoloEndpointManager
import cloudpickle as CP
from mictlanx.v4.client import Client
from mictlanx.utils.index import Utils as UtilsX

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

# class IDAx:
class IDAx(ActiveX):
    def __init__(self,k:int=3,n:int=5,p:int=2,verify:bool= False):
        
        # Define the Galois field
        alpha = galois.primitive_root(p)
        self.GF = galois.GF(p,1,primitive_element=alpha, verify=verify)
        self.n:int = n  # Number of shares
        self.k:int = k  # Threshold for reconstruction
    
    @activex_method
    def test(self,*args,**kwargs):
        return "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAHHHHHH!!!"


    def generate_vandermonde_matrix(self,k:int,n:int):
        """
        Generate a k x n Vandermonde matrix over the specified Galois field.
        """
        x = self.GF.elements[:n]
        V = self.GF.Zeros((n, k))
        for i in range(n):
            for j in range(k):
                V[i, j] = x[i] ** j
        return V

    def encode_data(self,data,*args,**kwargs):
        """
        Encode the data using a Vandermonde matrix to create n shares.
        Each byte of data is treated as a separate element in the field.
        """
        block_size  = (len(data) + self.k - 1) // self.k
        padded_data = data.ljust(block_size * self.k, b'\0')
        blocks = [
            padded_data[i * block_size:(i + 1) * block_size] for i in range(self.k)
        ]
        # logger.debug({
        #     "event":"ENCODE.DATA",
        #     "n":self.n,
        #     "k":self.k,
        #     "block_size":block_size,
        #     "padded_data":padded_data,
        #     "size":len(padded_data),
        #     "blocks":len(blocks)
        # })
        matrix = self.generate_vandermonde_matrix(k=self.k,n=self.n)
        shares = [self.GF.Zeros(block_size) for _ in range(self.n)]

        for i in range(block_size):
            for j in range(self.n):
                for m in range(self.k):
                    v = self.GF(blocks[m][i]) * matrix[j, m]
                    # print(i,j,m, v)
                    shares[j][i] += v
        
        return [bytes(share) for share in shares]

    @activex_method
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
        with open(axo_source_path,"rb") as f:
            data = f.read()


        print("AXO_SINK_PATH", axo_sink_path_sink_bucket_id_path)
        print("AXO_SIN_KEU", axo_sink_key)
        data_size = len(data)
        print("SIZE",data_size)
        block_size  = (data_size + self.k - 1) // self.k
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
        block_size   = len(shares[0])
        x            = self.GF.elements[:self.k]
        matrix       = self.generate_vandermonde_matrix(k=self.k,n= self.k)
        matrix_inv   = np.linalg.inv(matrix)
        decoded_data = bytearray(block_size * self.k)

        for i in range(block_size):
            column = self.GF([share[i] for share in shares[:self.k]])
            decoded_column = matrix_inv @ column

            print(column,decoded_column)
            for j, value in enumerate(decoded_column):
                index = j * block_size + i
                decoded_data[index] = value
        
        return bytes(decoded_data).rstrip(b'\0')
def main():

    class_definition_bytes = CP.dumps(IDAx)


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
    # Example usage
    data = b"Secret data to be split into shares. LOTS OF SHARXX"
    idax = IDAx(n=5,k=3)
    import struct
    x = idax.to_bytes()
    obj = ActiveX.from_bytes(x).unwrap()
    print("TEST_CALLED",ActiveX.call(obj,"test" ))
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
    idax.set_dependencies(["numpy==1.23.2"])
    persistify_res = idax.persistify()
    print(persistify_res)
    T.sleep(2)

    res = idax.encode_data_to_file(
        dependencies = ["numba==0.59.1","galois==0.3.8"],
        source_bucket_id = "moringas"
    )
    print("RES",res)
    # Encode data
    # shares = idax.encode_data(data)
    # logger.debug("Shares: {}".format([share.hex() for share in shares]))

    # # Decode data (using any k shares)
    # reconstructed_data = idax.decode_data(shares[:idax.k])
    # logger.debug("Original Data: {}".format(data))
    # logger.debug("Reconstructed Data: {}".format(reconstructed_data))


if __name__ =="__main__":
    main()