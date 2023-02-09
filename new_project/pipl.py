import os
from dotenv import load_dotenv

from sqlalchemy import create_engine, text, select
from sqlalchemy import MetaData, Table, String, Integer, Column, Text, DateTime, Boolean
from sqlalchemy.engine import URL

from urllib.parse import urlparse

import pandas as pd


load_dotenv()

DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')

class Pipeline:
    @staticmethod
    def run(input, output):
        LoadToDB.run(input, output)
        CreateTableAs.run(output)
        CopyToFile.run(output)
    
class BaseTask:
    @staticmethod
    def create_connection():
        url_object = URL.create(
            "postgresql",
            username=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            database=DB_NAME,
        )
        engine = create_engine(url_object)

        return engine
    
class LoadToDB(BaseTask): 
    @staticmethod  
    def read_df( input):
        return pd.read_csv(input)
    
    @staticmethod
    def run(input, output):
        con = LoadToDB.create_connection()
        df = LoadToDB.read_df(input)
        df.to_sql(con=con, name=output, if_exists='replace')


class CreateTableAs(BaseTask):
    
    @staticmethod
    def run(input):
        con = BaseTask.create_connection()
        query = text(f"""SELECT * FROM {input}""")
        with con.begin() as conn:
            df = pd.read_sql_query(query, conn)
            df['domain'] = df['url'].apply(lambda x: urlparse(x).netloc)
            df.to_sql(con=conn, name='norm', if_exists='replace')
                
class CopyToFile(BaseTask):      
    @staticmethod
    def run(output):
        con = BaseTask.create_connection()
        with con.begin() as conn:
            query = text("""SELECT * FROM norm""")
            pd.read_sql_query(query, conn).to_csv(output, sep=',')
        
        

def run():
    Pipeline.run()
    
Pipeline.run('in.csv', 'test')