import requests as req
import pandas as pd
import sqlite3
ofset=1000

response = req.get("https://api.eia.gov/v2/electricity/rto/daily-region-data/data/?frequency=daily&data[0]=value&facets[respondent][]=AEC&facets[type][]=D&facets[type][]=DF&facets[type][]=NG&facets[type][]=TI&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=100&api_key=WJCdtpf7cqm6q9JGNnEFrvVEqO8Vbf4CkiCNghy4").json()

print(response['response'])


#"""df['period'] = pd.to_datetime(df['period'])
#df['value'] = pd.to_numeric(df['value'])
#print(df.info())
#print(df.head(1))"""


