import os, glob
import datetime
from requests import get
import pandas as pd
import numpy as np
import tqdm
import urllib3
from bs4 import BeautifulSoup

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

WEB_URL = "https://www.mohfw.gov.in"
REPO_FILFES = "/Volumes/Lab/covid19-AnalyticsHbub/India/data/mhw_ind_covid_daily_reports/"
GEO_DATA = "/Volumes/Lab/covid19-AnalyticsHbub/India/data/state_geo_table.csv"


def ind_mh_web_scrap(url, data_dir):
    #df = pd.read_html(url, header=0)[1].iloc[:,1:]
    df = pd.read_html(url)[-1].iloc[:,1:] #Read covid-19 india 
    df.columns = ['State/UT', 'Confirmed','Recovered', 'Deaths']
    df.loc[(df['State/UT'].str.contains("Total")),'State/UT'] = "India"
    if df.Confirmed.dtypes == 'O':
        df = df.select_dtypes(include=['object']).apply(lambda x: x.str.replace('#','')).iloc[:-1,:]
    #df = df.select_dtypes(include=['object']).apply(lambda x: x.str.replace('#',''))
    df['report_date'] = pd.Timestamp.now().normalize()
    df['report_date'] = pd.to_datetime(df['report_date']).dt.date
    file_name = datetime.datetime.now().strftime("%Y-%m-%d")
    df.to_csv(data_dir+file_name+'.csv', index=False)


def load_reports(data_dir):
    ind_state_geo = pd.read_csv(GEO_DATA, index_col=0).reset_index()
    files = sorted([file for file in os.listdir(data_dir)])[2:]
    reports_df = pd.concat((pd.read_csv(data_dir+ file) for file in files),ignore_index=False) 
    reports_df['report_date'] = pd.to_datetime(reports_df['report_date']).dt.date
    reports_df['State/UT'] = reports_df['State/UT'].str.replace('Orissa', 'Odisha')
    #reports_df['Confirmed_ALL'] = [x+y for x,y in zip(reports_df.Confirmed_IN, reports_df.Confirmed_FN)]
    reports_df = reports_df.merge(ind_state_geo, on='State/UT', how = 'left')
    reports_df = reports_df.sort_values(by='Confirmed',ascending=False).groupby(['report_date', 'State/UT']).first().reset_index()
    reports_df_yest = reports_df.copy()
    reports_df_yest['report_date'] = pd.to_datetime(reports_df_yest['report_date']).dt.date + pd.to_timedelta(np.ceil(1), unit="D")
    return reports_df, reports_df_yest

def data_process():
    df1, df2 = load_reports(REPO_FILFES)
    final_df = df1.merge(df2.iloc[:,:-2], on=['report_date','State/UT'],suffixes=('', '_yest'), how='left')
    final_df.fillna(0, inplace=True)
    final_df['new_cases'] = final_df.Confirmed - final_df.Confirmed_yest
    final_df['new_Deaths'] = final_df.Deaths - final_df.Deaths_yest
    final_df['new_Recovd'] = final_df.Recovered - final_df.Recovered_yest
    final_df['recovery_rate'] = final_df.Recovered / final_df.Confirmed
    final_df['death_rate'] = final_df.Deaths / final_df.Confirmed
    final_df['report_date'] = pd.to_datetime(final_df['report_date']).dt.date
    final_df = final_df[final_df['State/UT'] != 'India']
    return final_df

def main():
    print('Loading dataset..')
    df = data_process()
    # Create indexs and connection for df to elasticsearch
    es = Elasticsearch(['http://localhost:9200'], 
                    http_auth=('user', 'passXXXX'), timeout = 3000)
    print("Indexing documents..")                    
    # delete index if exists
    if es.indices.exists('covid19-india'):
        es.indices.delete(index='covid19-india')

    settings = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
    },
    "mappings": {
        "properties": {
        "location": {
            "type": "geo_point"
        },
        "death_rate": { "type":"float" },
        "recovery_rate":  { "type": "float"  } 
        }
    }
    }
    
    # Load the json doc in index
    es.indices.create(index='covid19-india', ignore=400, body=settings)
    documents = df.to_dict(orient='records')
    for i in range(0, len(documents)):
        documents[i].update({'location': {'lat':documents[i]['lat'], 'lon': documents[i]['lon']}})
    bulk(es, documents, index = 'covid19-india', raise_on_error = True)
    print(f'No of inserted records:', len(documents))

    
if __name__ == "__main__":
    ind_mh_web_scrap(WEB_URL, REPO_FILFES)
    main()
    
