
from typing import Dict
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from typing_extensions import Annotated
from activex import ActiveX,activex
from activex.storage.mictlanx import GetKey,PutPath

class HeatmapProducer(ActiveX):
    input_data_key:Annotated[str, GetKey] = "2a45605714f82cd082c0b607cca6b0aff36ba5383a8498021be7fa8328e8e3ac"
    heatmap_output_path:Annotated[str,PutPath] 


    def __init__(
        self,
        cas:str="7440-38-2",
        input_path:str ="/source/hugodata.csv",
        output_path:str="/sink/theplot.png",
        key:str ="2a45605714f82cd082c0b607cca6b0aff36ba5383a8498021be7fa8328e8e3ac"
    ):
        self.cas         = cas
        self.input_path  = input_path
        self.df          = pd.read_csv(self.input_path)
        self.input_data_key         = key
        self.heatmap_output_path = output_path


    @activex
    def plot(self,cas:str):
        df = self.df
        sustancia_nombre = df["sustancia"].iloc[0]
        del df["cas"]
        del df["sustancia"]
        del df["Clave Entidad"]
        df.set_index('Entidad', inplace=True)
        df['sum'] = df.sum(axis=1) 
        df = df.sort_values("sum", ascending=False)
        del df['sum']

        fig,ax = plt.subplots(figsize=(40,20))
        sns.heatmap(df, annot=True, annot_kws={"size": 20}, fmt='g', cmap="Blues", ax=ax)
        cbar = ax.collections[0].colorbar
        cbar.ax.tick_params(labelsize=25)
        ax.collections[0].colorbar.set_label("Cantidad (Toneladas/Año)", size=25)
        plt.xticks(size=25, rotation=1)
        plt.yticks(size=25, rotation=1)


        plt.ylabel(
            'Entidad', size=30
        )
        plt.xlabel(
            'Tipo de emisión y transferencia',
            size=30
        )
        plt.title("Cantidad de Emisiones y transferencias\nregistradas en la base de datos del RETC para \n{}, número CAS: {}".format(sustancia_nombre, cas), size=30, fontweight="bold")

        fig.subplots_adjust(
            left=0.18,
            right=1,
            wspace=0.99
        )

        sustancia_nombre = sustancia_nombre.replace("/","")
        sustancia_nombre = sustancia_nombre.replace(" ","_")
        sustancia_nombre = sustancia_nombre.replace("(","-")
        sustancia_nombre = sustancia_nombre.replace(")","-")

        # plt.savefig("{}/heatmap_2004-2022_Ent-All_Mun-ALL_EyT-ALL_{}_{}_IARC-ALL_by_entity.jpg".format(self.output_folder, sustancia_nombre,cas))
        plt.savefig(self.heatmap_output_path)
        plt.close(fig=fig)
        plt.clf()