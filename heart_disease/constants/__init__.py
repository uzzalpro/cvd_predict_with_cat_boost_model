import os
from datetime import date
from urllib.parse import quote_plus
import certifi

DATABASE_NAME = "CVD"

COLLECTION_NAME = "cvd_data"
# Your credentials and cluster information
USERNAME = "rangomoviebd_db_user"
PASSWORD = "Pass@001122"
CLUSTER_URL = "cluster0.fspozs0.mongodb.net"

# Use quote_plus to encode the password
ENCODED_PASSWORD = quote_plus(PASSWORD)

MONGODB_URL_KEY = f"mongodb+srv://{USERNAME}:{ENCODED_PASSWORD}@{CLUSTER_URL}/?retryWrites=true&w=majority&appName=Cluster0"
# MONGODB_URL_KEY = "mongodb+srv://rangomoviebd_db_user:Pass@001122@cluster0.fspozs0.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

PIPELINE_NAME: str = "heartdisease"
ARTIFACT_DIR: str = "artifact"

MODEL_FILE_NAME = "model.pkl"


TARGET_COLUMN = "num"
CURRENT_YEAR = date.today().year
PREPROCSSING_OBJECT_FILE_NAME = "preprocessing.pkl"

FILE_NAME: str = "heartdisease.csv"
TRAIN_FILE_NAME: str = "train.csv"
TEST_FILE_NAME: str = "test.csv"
SCHEMA_FILE_PATH = os.path.join("config", "schema.yaml")


# AWS_ACCESS_KEY_ID_ENV_KEY = "AWS_ACCESS_KEY_ID"
# AWS_SECRET_ACCESS_KEY_ENV_KEY = "AWS_SECRET_ACCESS_KEY"
# REGION_NAME = "us-east-1"

"""
Azure Blob Storage
"""
STORAGE_ACCOUNT_CONNECTION="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
STORAGE_ACCOUNT_CONTAINER="cvd-uploads"

"""
Data Ingestion related constant start with DATA_INGESTION VAR NAME
"""
DATA_INGESTION_COLLECTION_NAME: str = "cvd_data"
DATA_INGESTION_DIR_NAME: str = "data_ingestion"
DATA_INGESTION_FEATURE_STORE_DIR: str = "feature_store"
DATA_INGESTION_INGESTED_DIR: str = "ingested"
DATA_INGESTION_TRAIN_TEST_SPLIT_RATIO: float = 0.2



"""
Data Validation realted contant start with DATA_VALIDATION VAR NAME
"""
DATA_VALIDATION_DIR_NAME: str = "data_validation"
DATA_VALIDATION_DRIFT_REPORT_DIR: str = "drift_report"
DATA_VALIDATION_DRIFT_REPORT_FILE_NAME: str = "report.yaml"



"""
Data Transformation ralated constant start with DATA_TRANSFORMATION VAR NAME
"""
DATA_TRANSFORMATION_DIR_NAME: str = "data_transformation"
DATA_TRANSFORMATION_TRANSFORMED_DATA_DIR: str = "transformed"
DATA_TRANSFORMATION_TRANSFORMED_OBJECT_DIR: str = "transformed_object"



"""
MODEL TRAINER related constant start with MODEL_TRAINER var name
"""
MODEL_TRAINER_DIR_NAME: str = "model_trainer"
MODEL_TRAINER_TRAINED_MODEL_DIR: str = "trained_model"
MODEL_TRAINER_TRAINED_MODEL_NAME: str = "model.pkl"
MODEL_TRAINER_EXPECTED_SCORE: float = 0.6
MODEL_TRAINER_MODEL_CONFIG_FILE_PATH: str = os.path.join("config", "model.yaml")



MODEL_EVALUATION_CHANGED_THRESHOLD_SCORE: float = 0.02
MODEL_BLOB_NAME = "cvd-uploads"
MODEL_PUSHER_BLOB_PATH = "model-registry"


APP_HOST = "0.0.0.0"
APP_PORT = 8080