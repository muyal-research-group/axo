from activex import ActiveX, activex_method
import numpy as np
import numpy.typing as npt
import pandas as pd
from option import Result,Ok
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
# import base64

class Cipher(ActiveX):

    def __init__(self,security_level:int=128,bucket_id:str="activex",output_key:str="test"):
        self.security_level = security_level
        self.bucket_id = bucket_id
        self.output_key=output_key

    @activex_method
    def encrypt(self,plaintext:bytes,key:bytes,*args,**kwargs)->bytes:
        cipher = AES.new(key, AES.MODE_CBC)
        iv = cipher.iv
        ciphertext = cipher.encrypt(pad(plaintext, AES.block_size))
        return iv + ciphertext
        # return b"CIPHERTEXT"
    @activex_method
    def decrypt(self,ciphertext:bytes,key:bytes,*args,**kwargs)->bytes:
        iv = ciphertext[:AES.block_size]
        cipher = AES.new(key, AES.MODE_CBC, iv)
        plaintext = unpad(cipher.decrypt(ciphertext[AES.block_size:]), AES.block_size)
        return plaintext
    @activex_method
    def local_encrypt(self,*args,**kwargs):
        return "LOCAL_ARMANDO_PREGUNTON"
        # return b"PLAINTEXT"
    
class Dog(ActiveX):
    def __init__(self):
        self.dog_name = "PERRITO"
        self.df = pd.DataFrame({
            "col1":[1],
            "col2":[2],
            "col3":[3],
        })
    @activex_method
    def bark(self, x:str="HOLA")->Result[str, Exception]:
        return"WOOF "+x

    @activex_method
    def get_name(self,name:str="PERRITO")->Result[str,Exception]:
        return "{}-{}".format(self.dog_name,name)

    @activex_method
    def get_df(self, df: pd.DataFrame= pd.DataFrame()):
        return pd.concat([self.df,df]).reset_index(drop=True)
    
    
class Calculator(ActiveX):
    def __init__(self):
        self.example_id = "02"
    @activex_method
    def add(self,x:float,y:float):
        return x + y
    @activex_method
    def substract(self,x:float,y:float):
        return x - y

    @activex_method
    def multiply(self,x:float,y:float):
        return x * y
    @activex_method
    def divide(self,x:float,y:float):
        if y == 0:
            raise ZeroDivisionError()
        return x / y
    @activex_method
    def add_vectors(self,x:npt.NDArray,y:npt.NDArray):
        res = x+y
        return res
    
