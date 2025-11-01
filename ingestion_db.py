import pandas as pd
import os
from sqlalchemy import create_engine
engine = create_engine("sqlite:///inventory.db")

#chunksize=1000, means it loads and inserts 1000 rows at a time 
def ingest_db(df,table_name,engine,chunksize=1000):
    df.to_sql(table_name,con=engine, if_exists='replace',index=False,chunksize=chunksize)
    
def load_raw_data():
    for file in os.listdir('data'):
        if '.csv' in file:
            df = pd.read_csv("data/"+file)
            print(df.shape)
            ingest_db(df,file[:-4],engine)
if __name__ == "__main__":
    load_raw_data()