import pandas as pd 
import sqlite3

engine = sqlite3.connect('covid19.sqlite.db')

counties = pd.read_csv("us-counties.csv")
counties.to_sql('counties', con=engine, index_label='id', if_exists='replace')
print(counties.head())

census = pd.read_csv("us-census.csv", encoding = "ISO-8859-1")
census.to_sql('census', con=engine, index_label='id', if_exists='replace')
print(census.head())

print("Done")
