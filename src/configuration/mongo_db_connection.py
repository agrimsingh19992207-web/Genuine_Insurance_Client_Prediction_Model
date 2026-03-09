import os 
import sys
import pymongo
import certifi


from src.exception import MyException
from src.logger import logging
from src.constants import DATABASE_NAME , MONGODB_URL_KEY


ca = certifi.where()


class MongoDBClient:

    client = None

    def __init__(self , DATABASE_NAME):

        try:
            if MongoDBClient.client is None:
                mongo_db_url = os.getenv(MONGODB_URL_KEY)
                if mongo_db_url is None:
                    raise Exception(f"Environment variable '{MONGODB_URL_KEY}' is not set.")
                
                MongoDBClient.client = pymongo.MongoClient(mongo_db_url, tlsCAFILE= ca)

            
            self.client = MongoDBClient.client
            self.database = self.client[DATABASE_NAME]
            self.database_name = DATABASE_NAME
            logging.info("MongoDB connection successfull")

        except Exception as e:
            raise MyException(e,sys)
        