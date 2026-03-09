import os
import sys


from pandas import DataFrame
from sklearn.model_selection import train_test_split

from src.entity.config_entity import DataIngestionConfig
from src.entity.artifact_entity import DataIngestionArtifact
from src.exception import MyException
from src.logger import logging
from src.data_access.proj1_data import Proj1Data



class DataIngestion:
    def __init__(self,data_ingestion_config):
        try:
            self.data_ingestion_config = data_ingestion_config
        except Exception as e:
            raise MyException(e,sys)
        

    def export_data_into_feature_store(self):
        try:
            logging.info(f"Exporting data from mongodb")
            my_data = Proj1Data()
            dataframe = my_data.export_collection_as_dataframe(collection_name=
                                                               self.data_ingestion_config.collection_name)
            logging.info(f"shape of dataframe:{dataframe.shape}")
            feature_store_file_path = self.data_ingestion_config.feature_store_file_path
            dir_path = os.path.dirname(feature_store_file_path)
            os.makedirs(dir_path,exist_ok=True)
            logging.info(f"saving exported data into feature store file path: {feature_store_file_path}")
            dataframe.to_csv(feature_store_file_path,index=False,header=True)
            return dataframe
        except Exception as e:
            raise MyException(e,sys)
        
    def split_data_as_train_test(self,dataframe):
        logging.info("Entered split data as train test method of Ingestion class")

        try:
            train_set,test_set =train_test_split(dataframe,test_size=self.data_ingestion_config.train_test_split_ratio)
            logging.info("performed train test split on the dataframe")
            logging.info("exited split data as train test method od data_ingestion class")

            dir_path = os.path.dirname(self.data_ingestion_config.training_file_path)
            os.makedirs(dir_path,exist_ok=True)
            

            logging.info(f"exported train and test file path")
            train_set.to_csv(self.data_ingestion_config.training_file_path,index = False,header = True)
            test_set.to_csv(self.data_ingestion_config.testing_file_path,index = False,header = True)
            
        except Exception as e:
            raise MyException (e,sys) from e
        
    def initiate_data_ingestion(self):


        
        logging.info("enterd initate data ingestion method to data ingestion class")

        try:
            dataframe = self.export_data_into_feature_store()
            logging.info("got data from mongo db")
            self.split_data_as_train_test(dataframe)
            logging.info("performed train test split")
            logging.info("exited initiated data ingestion method of data ingestion class")
            data_ingestion_artifact = DataIngestionArtifact(trained_file_path=self.data_ingestion_config.training_file_path,
                                                            
                                                            test_file_path=self.data_ingestion_config.testing_file_path)
            
            logging.info(f"data ingestion artifact{data_ingestion_artifact}")
            return data_ingestion_artifact
        
        except Exception as e:
            raise MyException(e,sys)from e
        