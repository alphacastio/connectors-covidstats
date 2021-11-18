#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import requests

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

# In[ ]:


#Se descarga la informacion diaria de covid por país (pesa más de 20 mb)
url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv"
df = pd.read_csv(url)


# In[ ]:


#Conector 155, que descarga la info diaria de covid

#Creo la variable country
df["country"] = df["location"]
#Convierto a fecha el campo Date
df["Date"] = pd.to_datetime(df["date"], errors= "coerce", format= "%Y-%m-%d")


# In[ ]:


#mantengo solo los no nulos
df = df[df["Date"].notnull()]
#Establezco Date como indice
df = df.set_index("Date")

#Elimino el campo Location y date
del df["location"]
del df["date"]

#El csv cambio y se agregaron nuevos campos

alphacast.datasets.dataset(155).upload_data_from_df(df, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

#Conector 324, que calcula el ranking

#Itero sobre las columnas
df.reset_index(inplace=True)

#Elimino iso code y continent que son string
del df['iso_code']
del df['continent']
del df['tests_units']

for col in df.columns:
    df[col] = df.groupby("country")[col].transform(lambda x: x.ffill(limit=5))

df_ranked = df[["Date", "country"]].merge(df.groupby(["Date"]).rank(method='dense',ascending=0), 
                                          left_index=True, right_index=True, how="left")

#Elimino columnas y renombro otras
df_ranked['country'] = df_ranked['country_x']
del df_ranked['country_x']
del df_ranked['country_y']


df_ranked["Date"] =  pd.to_datetime(df_ranked["Date"], format="%Y-%m-%d")
df_ranked.set_index("Date", inplace=True)


alphacast.datasets.dataset(324).upload_data_from_df(df_ranked, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)
