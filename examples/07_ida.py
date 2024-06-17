from activex import ActiveX,activex_method
from activex.contextmanager import ActiveXContextManager
from activex.endpoint import XoloEndpointManager


import galois
import numpy as np
import logging
import os 
AXO_ENDPOINT_ID           = os.environ.get("AXO_ENDPOINT_ID","activex-endpoint-0")
AXO_ENDPOINT_PROTOCOL     = os.environ.get("AXO_ENDPOINT_PROTOCOL","tcp")
AXO_ENDPOINT_HOSTNAME     = os.environ.get("AXO_ENDPOINT_HOSTNAME","localhost")
AXO_ENDPOINT_PUBSUB_PORT  = int(os.environ.get("AXO_ENDPOINT_PUBSUB_PORT","16000"))
AXO_ENDPOINT_REQ_RES_PORT = int(os.environ.get("AXO_ENDPOINT_REQ_RES_PORT","16667"))
logger          = logging.getLogger(__name__)
formatter       = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.setLevel(logging.DEBUG)

class IDAx(ActiveX):
    def __init__(self,k:int,n:int):
        
        # Define the Galois field
        self.GF = galois.GF(2**8)
        self.n:int = n  # Number of shares
        self.k:int = k  # Threshold for reconstruction
    
    @activex_method
    def test(self,*args,**kwargs):
        print("N = {}".format(self.n))
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
        logger.debug({
            "event":"ENCODE.DATA",
            "n":self.n,
            "k":self.k,
            "block_size":block_size,
            "padded_data":padded_data,
            "size":len(padded_data),
            "blocks":len(blocks)
        })
        matrix = self.generate_vandermonde_matrix(k=self.k,n=self.n)
        shares = [self.GF.Zeros(block_size) for _ in range(self.n)]

        for i in range(block_size):
            for j in range(self.n):
                for m in range(self.k):
                    v = self.GF(blocks[m][i]) * matrix[j, m]
                    # print(i,j,m, v)
                    shares[j][i] += v
        
        return [bytes(share) for share in shares]

    # @activex_method
    def encode_data_to_file(self,data:bytes,filename:str="",*args,**kwargs):
        axo_sink_path = self.get_sink_path()
        """
        Encode the data using a Vandermonde matrix to create n shares.
        Each byte of data is treated as a separate element in the field.
        """
        block_size  = (len(data) + self.k - 1) // self.k
        padded_data = data.ljust(block_size * self.k, b'\0')
        blocks = [
            padded_data[i * block_size:(i + 1) * block_size] for i in range(self.k)
        ]
        logger.debug({
            "event":"ENCODE.DATA",
            "n":self.n,
            "k":self.k,
            "block_size":block_size,
            "padded_data":padded_data,
            "size":len(padded_data),
            "blocks":len(blocks)
        })
        matrix = self.generate_vandermonde_matrix(k=self.k,n=self.n)
        shares = [self.GF.Zeros(block_size) for _ in range(self.n)]

        for i in range(block_size):
            for j in range(self.n):
                for m in range(self.k):
                    v = self.GF(blocks[m][i]) * matrix[j, m]
                    # print(i,j,m, v)
                    shares[j][i] += v
        
        for share in shares:
            with open(axo_sink_path,"wb") as f:
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
    _ = ActiveXContextManager.distributed(
        endpoint_manager= endpoint_manager
    )
    # Example usage
    data = b"Secret data to be split into shares. LOTS OF SHARXX"
    idax = IDAx(n=5,k=3)
    idax.set_dependencies(["galois","numba==0.56.4","numpy==1.26.0"])
    idax.persistify()
    res = idax.test()
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