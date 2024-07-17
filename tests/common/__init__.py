from activex import ActiveX, activex_method
import numpy.typing as npt
from option import Result,Ok
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
import datetime as DT
from Crypto.Random import get_random_bytes
# from activex.storage import StorageService
# import base64

class Cipher(ActiveX):

    def __init__(self,security_level:int=128):
        self.security_level = security_level
        # self.bucket_id = bucket_id
        # self.output_key=output_key

    def key_gen(self)->bytes:
        return get_random_bytes(16)  # 16 bytes key for AES-128
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
    
class Dog(ActiveX):
    def __init__(self,name:str):
        self.name =name
 
    @activex_method
    def bark(self,*args,**kwargs)->Result[str, Exception]:
        name    = kwargs.get("name","Rex")
        storage = kwargs.get("storage")
        dt   = DT.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        response =  "{}: Woof woof barking to {} at {}".format(self.name,name,dt )
        return Ok(response)
    
    @activex_method
    def bite(self,*args,**kwargs)->Result[str, Exception]:
        name = kwargs.get("name","Rex")
        return"{}:  biting {}".format(self.name,name)
    

    @activex_method
    def get_name(self)->Result[str,Exception]:
        return "{}-{}".format(self.name)
    
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
    
