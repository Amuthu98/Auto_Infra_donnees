import pandas as pd
import os

def rmDuplicate(directory):
    directory = directory

    for filename in os.listdir(directory):
        subdirectory = directory+filename
        for csvname in os.listdir(subdirectory):
            if csvname.endswith(".csv"):
                data = pd.read_csv(directory+filename+'/'+csvname, sep=';', header=0, encoding='utf-8')
                data.drop_duplicates(subset=['artist_date_extract'], inplace=True, keep='first')
                data.to_csv(directory+filename+'/'+csvname, index=False, sep=';')
            else:
                continue