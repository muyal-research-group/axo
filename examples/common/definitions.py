from activex import Axo, axo_method

import numpy.typing as npt
import numpy as np
from typing import Optional,Tuple
from typing_extensions import Annotated
from activex.storage.mictlanx import GetKey
import random
import math

OptionalFloat = Optional[float]
OptionalNDArray = Optional[npt.NDArray]

class Perceptron(Axo):
    #Initialization
    def __init__(self, learning_rate=0.01, activation=None, epochs=100):
        self.n = learning_rate
        self.epochs=epochs
        if activation!=None and callable(activation):
            self.activation=activation
        else:
            activation=lambda x: math.tanh(x)
            self.activation =activation
        self.w=None
    


    def save_model(self,filename:str="perceptronmodel.npy"):
        with open("/sink/{}".format(filename), "wb") as f:
            np.save(f, np.array(self.w))
    #Training Method
    def fit(self, xTrain:list, yTrain:list):
        #How many weigths?
        self.w = self.__generarPesos(len(xTrain[0])+1)
        x = self.__fillX(xTrain)
        y = yTrain
        for i in range(self.epochs):
            deltaW = self.__aprendizaje(x, y) #Basic perceptron operation
            self.w = self.__actualizarPesos(deltaW) # Update Weigths
        return self.w
    #Prediction Method
    def predict(self, dataSet: list):
        predicciones = []

        for i in range(0,len(dataSet)):
            prediccion = self.__predictVector(dataSet[i])
            predicciones.append(prediccion)

        return predicciones

######################## Métodos auxiliares ########################
    def __predictVector(self, vector: list):
        vectorX0 = self.__addX0(vector)

        resultado = self.__funcionActivacion(self.__productoPunto(self.w,vectorX0))
        return resultado

    def __generarPesos(self, longitud: int, seed=1):
        random.seed(seed)
        w = []
        for i in range(0,longitud):
            w.append(random.uniform(-1,1))
        return w

    def __funcionActivacion(self, yValue: float):
        evaluacion = self.activation(yValue)
        #evaluacion =math.ceil(evaluacion)
        if (evaluacion <=0): # consider tanh, sigm
            return 0
        else:
            return 1

    def __productoPunto(self, w: list, x: list):
        resultado = np.dot(w, x)
        return resultado

    def __aprendizaje(self, x: list, y: list):
        deltaW = []
        for i in range(len(self.w)):
            wi = 0
            for j in range(len(x)):
                od = self.__predictVector(x[j])
                wi += (y[j] - od) * x[j][i] #Gradiente descendente
            wi *= self.n
            deltaW.append(wi)
        return deltaW

    def __actualizarPesos(self, deltaW: list):
        resultado = []
        for i in range(len(self.w)):
            resultado.append(self.w[i]+deltaW[i])
        return resultado

    def __addX0(self, xi):
        xiX0 = xi.copy()

        if len(xiX0) != len(self.w):
            xiX0.insert(0,1)

        return xiX0

    def __fillX(self, x):
        xFilled = []
        for i in range(len(x)):
            xFilled.append(self.__addX0(x[i]))
        return xFilled

    def obtenerX(self, dataSet: list):
        x = []
        for i in range(len(dataSet)):
            vX = []
            for j in range(len(dataSet[i])-1):
                vX.append(dataSet[i][j])
            x.append(vX)
        return x

    def obtenerY(self, dataSet: list):
        y = []
        for i in range(len(dataSet)):
                y.append(dataSet[i][-1])
        return y

class Calculator(Axo):
    """
        Calculator:
        add(float,float): Add two float numbers
        substract(float,float): Substract two float numbers
    """
    # key:Annotated[str, GetKey] 
    
    def __init__(self,x:float=0,y:float=1):
        self.example_id = "02"
        self.x=x
        self.y=y

    @axo_method
    def add(self,*args,**kwargs):
        x=kwargs.get("x",0)
        y=kwargs.get("y",1)
        return x + y
    