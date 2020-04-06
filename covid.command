#! /bin/bash

cd /Volumes/Lab/covid19-AnalyticsHbub

python covid19_global.py
python load_covid19org_data.py
python load_daily_data.py

