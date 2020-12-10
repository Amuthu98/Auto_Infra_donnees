import pyspark
import pandas as pd

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark import SparkContext

conf = SparkConf()\
    .setAppName("weblogs processing")\
    .set("spark.executor.memory", "8g")
spark = SparkSession.builder.config(conf=conf).getOrCreate()

# A modifier selon la machine
file = "/Volumes/DD DISQUE/Automatisation_PySpark/Auto_Infra_donnees/TransfData/All_Meta_Artist_csv/All_data_artists"

df = pd.read_csv(file+'.csv')

moy_followers = df['artist_followers'].mean()

df_petit = df.loc[df['artist_followers'] < moy_followers]
df_grand = df.loc[df['artist_followers'] > moy_followers]



def calculate_evolution(row,data):

    followers = row.artist_followers
    popularity = row.artist_popularity
    date = row.artist_date_extract
    artist = row.artist_id
    d = data.loc[(data['artist_id'] == artist) & (data['artist_date_extract'] < date)]
    d = d.sort_values(by=["artist_date_extract"], ascending=False)
    list_follow = d['artist_followers'].values.tolist()
    list_follow.insert(0,followers)
    list_pop = d['artist_popularity'].values.tolist()
    list_pop.insert(0,popularity)
    d = d.tail(1)
    nb = d['artist_followers']
    old_pop = d['artist_popularity']

    if len(nb) > 0:
        nb = nb.values[0]
        result = round((((followers - nb)/nb)*100), 2)
    else:
        result = 0

    if len(old_pop) > 0:
        old_pop = old_pop.values[0]
        result_pop = round((((popularity - old_pop)/old_pop)*100), 2)
    else:
        result_pop = 0
    row['evolution_followers'] = result
    row['evolution_popularity'] = result_pop
    row['liste_evol_followers'] = list_follow
    row['liste_evol_popularity'] = list_pop
    return row

# -- Petits Artistes

df_sort = df_petit.sort_values(by=["artist_id", "artist_date_extract"])
df_evol = df_sort.apply(lambda x: calculate_evolution(x,df_sort), axis=1)

print('dd')
# A modifier selon la machine
df_evol.to_csv('/Volumes/DD DISQUE/Automatisation_PySpark/Auto_Infra_donnees/TransfData/Data_Transf_PetitsArtistes.csv', index = 0)

# -- Grands Artistes

df_sort = df_grand.sort_values(by=["artist_id", "artist_date_extract"])
df_evol = df_sort.apply(lambda x: calculate_evolution(x,df_sort), axis=1)

print('dd')
# A modifier selon la machine
df_evol.to_csv('/Volumes/DD DISQUE/Automatisation_PySpark/Auto_Infra_donnees/TransfData/Data_Transf_GrandsArtistes.csv', index = 0)
print(df_evol.columns)
#df_evol = df.withColumn("evolution", col("artist_followers")/df.where(col("artist_id")==))
