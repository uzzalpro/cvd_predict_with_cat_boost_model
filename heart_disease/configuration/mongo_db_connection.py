import sys

from heart_disease.exception import HeartdieseaseException
from heart_disease.logger import logging
import ssl
import os
from heart_disease.constants import DATABASE_NAME, MONGODB_URL_KEY
import pymongo
import certifi

ca = certifi.where()

class MongoDBClient:
    """
    Class Name :   export_data_into_feature_store
    Description :   This method exports the dataframe from mongodb feature store as dataframe 
    
    Output      :   connection to mongodb database
    On Failure  :   raises an exception
    """
    client = None

    def __init__(self, database_name=DATABASE_NAME) -> None:
        try:
            if MongoDBClient.client is None:
                # mongo_db_url = os.getenv(MONGODB_URL_KEY)
                mongo_db_url = MONGODB_URL_KEY
                if mongo_db_url is None:
                    raise Exception(f"Environment key: {MONGODB_URL_KEY} is not set.")
                MongoDBClient.client = pymongo.MongoClient(mongo_db_url, tlsCAFile=ca,serverSelectionTimeoutMS=30000,connectTimeoutMS=30000)
                # verify connection immediately
                self.client.admin.command("ping")

            self.database = self.client[database_name]
            print(f"✅ MongoDB connection to '{database_name}' established successfully!")

            self.client = MongoDBClient.client
            self.database = self.client[database_name]
            self.database_name = database_name
            logging.info("MongoDB connection succesfull")
        except Exception as e:
            raise HeartdieseaseException(e,sys)
        

# import pymongo
# from urllib.parse import quote_plus
# import certifi
# import ssl
# from heart_disease.exception import HeartdieseaseException
# import sys

# class MongoDBClient:
#     def __init__(
#         self,
#         username: str,
#         password: str,
#         cluster_url: str,
#         db_name: str
#     ):
#         try:
#             encoded_password = quote_plus(password)
#             connection_url = (
#                 f"mongodb+srv://{username}:{encoded_password}@{cluster_url}/"
#                 f"?retryWrites=true&w=majority&appName=Cluster0"
#             )

#             self.client = pymongo.MongoClient(
#                 connection_url,
#                 tls=True,
#                 tlsCAFile=certifi.where(),
#                 ssl_cert_reqs=ssl.CERT_REQUIRED,
#                 serverSelectionTimeoutMS=30000,  # 30 seconds
#                 connectTimeoutMS=30000,
#             )

#             # verify connection immediately
#             self.client.admin.command("ping")

#             self.database = self.client[db_name]
#             print(f"✅ MongoDB connection to '{db_name}' established successfully!")

#         except Exception as e:
#             raise HeartdieseaseException(e, sys)

#     def get_database(self):
#         return self.database
