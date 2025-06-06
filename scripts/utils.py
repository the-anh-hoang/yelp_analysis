import os 
from dotenv import load_dotenv
import mysql.connector
from sqlalchemy import create_engine
def db_connect():
    load_dotenv()
    return mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )
    
def get_engine():
    load_dotenv()
    return create_engine(os.getenv("ENGINE_URL"))