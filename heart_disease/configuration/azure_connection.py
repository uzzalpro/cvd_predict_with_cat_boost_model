
# heart_disease/configuration/azure_connection.py
import os
from heart_disease.constants import STORAGE_ACCOUNT_CONNECTION, STORAGE_ACCOUNT_CONTAINER
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError
from heart_disease.logger import logging


class AzuriteClient:
    """
    Singleton wrapper around BlobServiceClient + container client.
    """
    _client_instance = None

    def __init__(self):
        if AzuriteClient._client_instance is None:
            connection_string = STORAGE_ACCOUNT_CONNECTION
            container_name = STORAGE_ACCOUNT_CONTAINER

            if not connection_string:
                raise Exception(f"Storage connection string (STORAGE_ACCOUNT_CONNECTION) is not set.")
            if not container_name:
                raise Exception(f"Storage container (STORAGE_ACCOUNT_CONTAINER) is not set.")

            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            logging.info("Azurite connection established")

            container_client = blob_service_client.get_container_client(container_name)
            # ensure container exists
            try:
                container_client.create_container()
                logging.info(f"Created container '{container_name}'")
            except ResourceExistsError:
                logging.info(f"Container '{container_name}' already exists")

            AzuriteClient._client_instance = {
                "blob_service_client": blob_service_client,
                "container_client": container_client,
                "container_name": container_name
            }

        self.client = AzuriteClient._client_instance


# import os
# from heart_disease.constants import STORAGE_ACCOUNT_CONNECTION, STORAGE_ACCOUNT_CONTAINER
# from azure.storage.blob import BlobServiceClient
# from heart_disease.logger import logging


# class AzuriteClient:
#     """
#     Initialize connection with Azurite (Azure Blob Storage emulator)
#     using credentials from environment variables.
#     """

#     _client_instance = None

#     def __init__(self):
#         if AzuriteClient._client_instance is None:
#             # connection_string = os.getenv(STORAGE_ACCOUNT_CONNECTION)
#             # container_name = os.getenv(STORAGE_ACCOUNT_CONTAINER)
#             connection_string = STORAGE_ACCOUNT_CONNECTION
#             container_name = STORAGE_ACCOUNT_CONTAINER

#             if not connection_string:
#                 raise Exception(f"Environment variable '{STORAGE_ACCOUNT_CONNECTION}' is not set.")
#             if not container_name:
#                 raise Exception(f"Environment variable '{STORAGE_ACCOUNT_CONTAINER}' is not set.")

#             blob_service_client = BlobServiceClient.from_connection_string(connection_string)
#             logging.info("Azurite connection stablished......")
#             container_client = blob_service_client.get_container_client(container_name)
            
#             # Ensure container exists
#             try:
#                 container_client.create_container()
#             except Exception:
#                 pass  # container may already exist

#             AzuriteClient._client_instance = {
#                 "blob_service_client": blob_service_client,
#                 "container_client": container_client,
#                 "container_name": container_name
#             }

#         self.client = AzuriteClient._client_instance




