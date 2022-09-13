import pandas as pd 
from sqlalchemy import create_engine

engine = create_engine('sqlite:///covid19.sqlite.db', echo=True)

counties = pd.read_csv("us-counties.csv")
counties.to_sql('counties', con=engine, index_label='id', if_exists='replace')
print(counties.head())

census = pd.read_csv("us-census.csv", encoding = "ISO-8859-1")
census.to_sql('census', con=engine, index_label='id', if_exists='replace')
print(census.head())

print("Done")
