from activex import ActiveX, activex_method
import numpy as np
import numpy.typing as npt
import pandas as pd
from option import Result,Ok
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
    
