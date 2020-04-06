import os, glob
import pandas as pd
import numpy as np
import datetime
import requests
import json
#from pandas.json_normalize import json_normalize
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk 

API_URL = 'https://api.covid19india.org/raw_data.json'
DIR ="/Volumes/Lab/covid19-AnalyticsHbub/India/data/"

def load_covid19in_org(api_url, dir_):
    RAW_D = requests.get(api_url)
    raw_data = RAW_D.json()
    raw_df = pd.json_normalize(raw_data, 'raw_data')
    raw_df.replace(r'^\s*$', np.nan, regex=True, inplace=True) #Replace Blank with NaN for pandas operation
    raw_df = raw_df[raw_df.count(1) >5 ]
    raw_df.nationality.fillna("India",inplace=True)
    raw_df.notes.fillna("no notest", inplace=True)
    raw_df['gender'] = raw_df.gender.replace({"M": "Male","F": "Female"}).fillna("Not Available")
    raw_df.statuschangedate.fillna(raw_df.dateannounced,inplace=True) # replace status date nan value with dignosed date
    raw_df.agebracket.replace({'28-35':'32'}, inplace=True) # Fill Age 
    raw_df['agebracket'] = raw_df.agebracket.fillna(-1).astype(int)
    raw_df['dateannounced'] = pd.to_datetime(raw_df['dateannounced']).dt.date
    raw_df['statuschangedate'] = pd.to_datetime(raw_df['statuschangedate']).dt.date
    raw_df = raw_df.fillna('Not Available')
    fname = datetime.datetime.now().strftime("%Y-%m-%d")
    raw_df.to_csv(dir_+'/covid19org/raw_'+fname+'.csv')
    return raw_df


def main():
    df = load_covid19in_org(API_URL,DIR)
    print("Indexing..")
    es = Elasticsearch(['https://a1f9141091724c6d8501f6a56443982e.southeastasia.azure.elastic-cloud.com:9243'], 
                                http_auth=('elastic', 'sPevZu2SAgMDF2CbEpBSaFIn'), timeout = 3000)
        # delete index if exists
    if es.indices.exists('covid19india-api2'):
        es.indices.delete(index='covid19india-api2')
        
    # Load the json doc in index
    es.indices.create(index='covid19india-api2', ignore=400, body={})
    documents = df.to_dict(orient='records')
    bulk(es, documents, index = 'covid19india-api2', raise_on_error = True)
    print(f'No. of raw data inserted records:', len(documents))

    # ICMR test
    icmr_df = pd.read_csv(DIR+'icmr_test_india.csv')
    icmr_df['date'] = pd.to_datetime(icmr_df.date, format='%d-%m-%y')
    if es.indices.exists('covid19in-icmr'):
        es.indices.delete(index='covid19in-icmr')
    # Load the json doc in index
    es.indices.create(index='covid19in-icmr', ignore=400, body={})
    report = icmr_df.to_dict(orient='records')
    bulk(es, report, index = 'covid19in-icmr', raise_on_error = True)
    print(f'No. of icmr_test data inserted records:', len(report))

if __name__ == "__main__":
    load_covid19in_org(API_URL,DIR)
    main()










# ORG_URL = "https://raw.githubusercontent.com/covid19india/CovidCrowd/master/data/raw_data.csv"
# DIR ="/Volumes/Lab/covid19-AnalyticsHbub/India/data/"

# def load_raw_clean_data(data_url, data_dir):
#     df = pd.read_csv(data_url)
#     df = df.drop(['Estimated Onset Date', 'Source_1', 'Source_2', 'Source_3', 'Backup Notes'], axis=1)
#     df.columns = ['id', 'government_id', 'diagnosed_date', 'age', 'gender',
#         'detected_city', 'detected_district', 'detected_state', 'current_status', 
#                 'notes', 'contracted','nationality', 'status_change_date']

#     df = df[df.count(1) > 5]
#     df.nationality.fillna("India",inplace=True)
#     df.notes.fillna("no notest", inplace=True)
#     df['gender'] = df.gender.replace({"M": "Male","F": "Female"}).fillna("Not Available")
#     df['age'] = df.age.fillna(-1)
#     df.status_change_date.fillna(df.diagnosed_date,inplace=True) # replace status date nan value with dignosed date
#     df[['government_id', 'detected_city','detected_district', 'contracted']] = df[['government_id', 
#                                                     'detected_city', 'detected_district', 'contracted']].fillna('Not Available')
#     df['diagnosed_date'] = pd.to_datetime(df['diagnosed_date']).dt.date
#     df['status_change_date'] = pd.to_datetime(df['status_change_date']).dt.date
#     fname = datetime.datetime.now().strftime("%Y-%m-%d")
#     df.to_csv(data_dir+'/covid19org/raw_'+fname+'.csv')
#     return df
