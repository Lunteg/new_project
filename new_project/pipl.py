import os
from dotenv import load_dotenv

from sqlalchemy import create_engine, text, select
from sqlalchemy import MetaData, Table, String, Integer, Column, Text, DateTime, Boolean
from sqlalchemy.engine import URL

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
    class Question(Base):
        __tablename__ = "questions"
        id=Column(...)
        user_id=Column(...)
    
    @staticmethod
    def run(output):
        con = BaseTask.create_connection()
        with con.connect() as connection:
            metadata = MetaData()

            Norm = Table('norm', metadata, 
                Column('index', Integer(), primary_key=True),
                Column('post_title', Integer(), nullable=False),
                Column('name', Text(),  nullable=False),
                Column('url', Text(), nullable=False)
            )
            metadata.create_all(con)
            
            sql = text('''
                    insert into norm (id, name, url)
                    select *, domain_of_url(url)
                    from :out 
                    ''')
            result = connection.execute(sql, {"out" : output})
        
        

def run():
    Pipeline.run()
    
Pipeline.run('in.csv', 'test')