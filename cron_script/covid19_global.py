import os, glob
import datetime
import requests 
import pandas as pd
import numpy as np
import json 

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

json_url = 'https://pomber.github.io/covid19/timeseries.json'

def load_covid_json(url):
    covid19_json = requests.get(url)
    covid19_json = covid19_json.json()
    append_covid = []
    for key in covid19_json.keys():
        data = pd.json_normalize(covid19_json, key)
        data['country'] = key
        data['recovery_rate'] = (data.recovered/data.confirmed).fillna(0)
        data['deaths_rate'] = (data.deaths/data.confirmed).fillna(0)
        data['date'] = pd.to_datetime(data.date)
        append_covid.append(data)
    append_covid = pd.concat(append_covid)   
    append_covid['country'] = append_covid['country'].replace({'US': 'United States',
                  'Korea,South': 'South Korea'})
    return append_covid


def main():
    dfc = load_covid_json(json_url)
    dfc = dfc.sort_values(by='confirmed',ascending=False).groupby(['date', 'country']).first().reset_index()
    dfc['date'] = pd.to_datetime(dfc['date']).dt.date
    dfc_yest = dfc.copy()
    dfc_yest['date'] = pd.to_datetime(dfc_yest['date']).dt.date + pd.to_timedelta(np.ceil(1), unit="D")
    final_df = dfc.merge(dfc_yest.iloc[:,:-2], on=['date','country'],suffixes=('', '_yest'), how='left').fillna(0)
    final_df['recovery_rate'] = final_df.confirmed - final_df.confirmed_yest
    final_df = final_df.iloc[:,:-3]
    
    #Elastic Index
    es = Elasticsearch(['https://a1f9141091724c6d8501f6a56443982e.southeastasia.azure.elastic-cloud.com:9243'], 
                        http_auth=('elastic', 'sPevZu2SAgMDF2CbEpBSaFIn'), timeout = 3000)
    print("Indexing documents..")                    
    # delete index if exists
    if es.indices.exists('covid19-global'):
        es.indices.delete(index='covid19-global')
        
    # Insert document into index    
    es.indices.create(index='covid19-global', ignore=400, body={})
    documents = final_df.to_dict(orient='records')
    bulk(es, documents, index = 'covid19-global', raise_on_error = True)
    print(f'No of global inserted records:', len(documents))    
    

if __name__ == "__main__":
    main()    