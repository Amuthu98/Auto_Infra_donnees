import pyspark
import pandas as pd

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark import SparkContext
import os

conf = SparkConf()\
    .setAppName("weblogs processing")\
    .set("spark.executor.memory", "8g")
spark = SparkSession.builder.config(conf=conf).getOrCreate()

# Chemin à adapter pour chacun
chemin = "/Volumes/DD DISQUE/Automatisation_PySpark/Auto_Infra_donnees/GetData/Data/"

#df = spark.read.csv(chemin+'.csv',header=True)


directory = chemin+'MetaArtist/'
i = 0
special_char = ['#', '*', '<', '>', '?', '/', '\\', '|', ':', '"', '.', ',', '…']

for dir in os.listdir(directory):
    complete_directory = directory+dir
    for filename in os.listdir(complete_directory):
        # Si le nom possède un caractère spécial, on l'ignore
        if any(c in dir for c in special_char):
            break

        if filename.endswith(".csv"):
            if i == 0:
                df_final = spark.read.csv(complete_directory+"/"+filename, header=True, sep=";")
                i = 1
                break
            else:
                try:
                    df = spark.read.csv(complete_directory+"/"+filename, header=True, sep=";")
                    df_final = df_final.union(df.select(df_final.columns))
                    break
                except:
                    print('x')
                    break

df_final.show()
df_final.coalesce(1).write.format('csv').save('/Volumes/DD DISQUE/Automatisation_PySpark/Auto_Infra_donnees/GetData/Data/All_Meta_Artist_csv/All_data_artists', header='true')
