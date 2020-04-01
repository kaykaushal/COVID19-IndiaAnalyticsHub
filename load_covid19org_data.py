import os, glob
import pandas as pd
import numpy as np
import datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk 

ORG_URL = "https://raw.githubusercontent.com/covid19india/CovidCrowd/master/data/raw_data.csv"
DIR ="/Volumes/Lab/covid19-AnalyticsHbub/India/data/"

def load_raw_clean_data(data_url, data_dir):
    df = pd.read_csv(data_url)
    df = df.drop(['Estimated Onset Date', 'Source_1', 'Source_2', 'Source_3', 'Backup Notes'], axis=1)
    df.columns = ['id', 'government_id', 'diagnosed_date', 'age', 'gender',
        'detected_city', 'detected_district', 'detected_state', 'current_status', 
                'notes', 'contracted','nationality', 'status_change_date']

    df = df[df.count(1) > 5]
    df.nationality.fillna("India",inplace=True)
    df.notes.fillna("no notest", inplace=True)
    df['gender'] = df.gender.replace({"M": "Male","F": "Female"}).fillna("Not Available")
    df['age'] = df.age.fillna(-1)
    df.status_change_date.fillna(df.diagnosed_date,inplace=True) # replace status date nan value with dignosed date
    df[['government_id', 'detected_city','detected_district', 'contracted']] = df[['government_id', 
                                                    'detected_city', 'detected_district', 'contracted']].fillna('Not Available')
    df['diagnosed_date'] = pd.to_datetime(df['diagnosed_date']).dt.date
    df['status_change_date'] = pd.to_datetime(df['status_change_date']).dt.date
    fname = datetime.datetime.now().strftime("%Y-%m-%d")
    df.to_csv(data_dir+'/covid19org/raw_'+fname+'.csv')
    return df



def main():
    df = load_raw_clean_data(ORG_URL,DIR)
    es = Elasticsearch(['http://localhost:9200'], 
                    http_auth=('user', 'passXXXX'), timeout = 3000)
        # delete index if exists
    if es.indices.exists('covid19indiaorg'):
        es.indices.delete(index='covid19indiaorg')
        
    # Load the json doc in index
    es.indices.create(index='covid19indiaorg', ignore=400, body={})
    documents = df.to_dict(orient='records')
    bulk(es, documents, index = 'covid19indiaorg', raise_on_error = True)

if __name__ == "__main__":
    load_raw_clean_data(ORG_URL,DIR)
    main()
    