# heart_disease/blob_store.py  (or wherever you keep it)
import os
import sys
import pickle
from io import StringIO
from typing import Union, List, Optional

import pandas as pd

from heart_disease.exception import HeartdieseaseException
from heart_disease.logger import logging
from heart_disease.configuration.azure_connection import AzuriteClient
from azure.storage.blob import BlobClient
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError


class SimpleStorageService:
    def __init__(self):
        azurite_client = AzuriteClient()
        logging.info("Connecting to Azurite......")

        # pull instances from the AzuriteClient singleton
        self.blob_service_client = azurite_client.client["blob_service_client"]
        self.container_client = azurite_client.client["container_client"]
        self.container_name = azurite_client.client["container_name"]

    def blob_are_available(self, blob_name, model_path) -> bool:
        try:
            blob_list = self.container_client.list_blobs(name_starts_with=model_path)
            return any(blob.name == blob_name for blob in blob_list)
        except Exception as e:
            raise HeartdieseaseException(e, sys) from e
        
    @staticmethod
    def read_object(self, blob_name: str, decode: bool = True, make_readable: bool = False) -> Union[StringIO, str, bytes]:
        """
        Read a blob and return either bytes, str or StringIO depending on flags.
        """
        logging.info("Entered read_object")
        try:
            blob_client = self.get_blob_client(blob_name)
            blob_bytes = blob_client.download_blob().readall()

            if not decode:
                return blob_bytes

            # decode text
            text = blob_bytes.decode("utf-8")
            if make_readable:
                return StringIO(text)
            return text

        except ResourceNotFoundError as e:
            raise HeartdieseaseException(f"Blob '{blob_name}' not found: {e}", sys) from e
        except Exception as e:
            raise HeartdieseaseException(e, sys) from e

    def get_blob(self, blob_name: str) -> BlobClient:
        """Get a blob client scoped to the default container."""
        try:
            return self.container_client.get_blob_client(blob_name)
        except Exception as e:
            raise HeartdieseaseException(e, sys) from e

    def get_blob_client(self, blob_name: str, container_name: Optional[str] = None) -> BlobClient:
        """
        Get a blob client from a specified container (or the default container if None).
        """
        try:
            if container_name is None:
                container_name = self.container_name
            return self.blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        except Exception as e:
            raise HeartdieseaseException(e, sys) from e

    def get_file_object(self, filename: str) -> Union[List[str], str]:
        try:
            blob_list = [blob.name for blob in self.container_client.list_blobs(name_starts_with=filename)]
            if not blob_list:
                raise Exception(f"No blobs found with prefix '{filename}'")
            return blob_list[0] if len(blob_list) == 1 else blob_list
        except Exception as e:
            raise HeartdieseaseException(e, sys) from e

    def load_model(self, model_name: str, blob_prefix: str = None, model_dir: str = None):
        try:
            blob_path = f"{blob_prefix}/{model_name}" if blob_prefix else model_name
            if model_dir:
                blob_path = f"{model_dir}/{blob_path}"

            blob_client = self.get_blob_client(blob_path)
            model_data = blob_client.download_blob().readall()
            model = pickle.loads(model_data)
            logging.info(f"Loaded model from blob: {blob_path}")
            return model
        except Exception as e:
            raise HeartdieseaseException(e, sys) from e

    def create_folder(self, folder_name: str, container_name: Optional[str] = None) -> None:
        """
        Create a zero-byte blob named `folder_name/` to represent a virtual folder.
        """
        try:
            if container_name is None:
                container_name = self.container_name

            blob_client = self.get_blob_client(f"{folder_name.rstrip('/')}/", container_name)
            try:
                # Try to create without overwrite; if exists, ResourceExistsError will be thrown
                blob_client.upload_blob(b'', overwrite=False)
            except ResourceExistsError:
                pass
        except Exception as e:
            raise HeartdieseaseException(e, sys) from e

    def upload_file(self, from_filename: str, to_filename: str, container_name: Optional[str] = None, remove: bool = True):
        try:
            if container_name is None:
                container_name = self.container_name

            blob_client = self.get_blob_client(to_filename, container_name)
            with open(from_filename, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)

            if remove:
                os.remove(from_filename)
        except Exception as e:
            raise HeartdieseaseException(e, sys) from e

    def upload_df_as_csv(self, data_frame: pd.DataFrame, local_filename: str, blob_filename: str, container_name: Optional[str] = None) -> None:
        """
        Upload a DataFrame as CSV (in-memory) to Azurite.
        local_filename is optional (kept for backward compatibility) but not required.
        """
        try:
            if container_name is None:
                container_name = self.container_name

            csv_buffer = StringIO()
            data_frame.to_csv(csv_buffer, index=False)
            blob_client = self.get_blob_client(blob_filename, container_name)
            blob_client.upload_blob(csv_buffer.getvalue(), overwrite=True)
        except Exception as e:
            raise HeartdieseaseException(e, sys) from e

    def get_df_from_object(self, blob_name: str, container_name: Optional[str] = None) -> pd.DataFrame:
        try:
            blob_client = self.get_blob_client(blob_name, container_name)
            blob_bytes = blob_client.download_blob().readall()
            text = blob_bytes.decode("utf-8")
            df = pd.read_csv(StringIO(text))
            return df
        except Exception as e:
            raise HeartdieseaseException(e, sys) from e

    def read_csv(self, blob_filename: str, container_name: Optional[str] = None) -> pd.DataFrame:
        # reuse get_df_from_object for simplicity
        return self.get_df_from_object(blob_filename, container_name)


# import os
# import sys
# import pickle
# import io
# from io import StringIO
# from typing import Union, List
# from heart_disease.exception import HeartdieseaseException
# from heart_disease.logger import logging
# from heart_disease.configuration.azure_connection import AzuriteClient
# from azure.storage.blob import BlobServiceClient, BlobClient
# from heart_disease.exception import HeartdieseaseException
# from azure.core.exceptions import ResourceNotFoundError
# from pandas import DataFrame, read_csv

# class SimpleStorageService:


#     def __init__(self):
#         azurite_client = AzuriteClient()
#         logging.info("Connecting Azurite......")
#         self.container_client = azurite_client.client["container_client"]

#     def blob_are_available(self, blob_name: str) -> bool:
#         try:
#             blob_list = self.container_client.list_blobs(name_starts_with=blob_name)
#             logging.info("Azurite blob available......")
#             return any(blob.name == blob_name for blob in blob_list)
#         except Exception as e:
#             raise HeartdieseaseException(e, sys)
        
#     @staticmethod
#     def read_object(self, blob_name: str, decode: bool = True, make_readable: bool = False) -> Union[StringIO, str, bytes]:
#         """
#         Method Name :   read_object
#         Description :   This method reads the object_name object with kwargs

#         Output      :   The column name is renamed
#         On Failure  :   Write an exception log and then raise an exception

#         Version     :   1.2
#         Revisions   :   moved setup to cloud
#         """
#         logging.info("Entered the read_object method of SimpleStorageService class")

#         try:
#             blob_client = self.container_client.get_blob_client(blob_name)
#             blob_data = blob_client.download_blob().readall()

#             if decode:
#                 blob_data = blob_data.decode()

#             if make_readable:
#                 blob_data = StringIO(blob_data)

#             logging.info("Exited the read_object method of SimpleStorageService class")
#             return blob_data

#         except Exception as e:
#             raise HeartdieseaseException(e, sys) from e

#     def get_blob(self, blob_name: str):
#         """
#         Method Name :   get_bucket
#         Description :   This method gets the bucket object based on the bucket_name

#         Output      :   Bucket object is returned based on the bucket name
#         On Failure  :   Write an exception log and then raise an exception

#         Version     :   1.2
#         Revisions   :   moved setup to cloud
#         """

#         logging.info("Entered the get_blob method of SimpleStorageService class")
#         try:
#             blob_client = self.container_client.get_blob_client(blob_name)
#             logging.info("Exited the get_blob method of SimpleStorageService class")
#             return blob_client
#         except Exception as e:
#             raise HeartdieseaseException(e, sys) from e

#     def get_blob_client(self, blob_name: str, container_name: str):
#         """
#         Method Name :   get_blob_client
#         Description :   Gets a blob client from the specified container and blob name.
#         """
#         logging.info("Entered the get_blob_client method of SimpleStorageService class")
#         try:
#             blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=blob_name)
#             logging.info("Exited the get_blob_client method of SimpleStorageService class")
#             return blob_client
#         except Exception as e:
#             raise HeartdieseaseException(e, sys) from e
        

#     #Get specific blob client
#     def get_blob_client(self, blob_name: str, container_name: str = None) -> BlobClient:
#         logging.info("Entered the get_blob_client method of SimpleStorageService class")
#         try:
#             if container_name is None:
#                 container_name = self.container_name
#             blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=blob_name)
#             logging.info("Exited the get_blob_client method of SimpleStorageService class")
#             return blob_client
#         except Exception as e:
#             raise HeartdieseaseException(e, sys) from e

#     # Get file (single or multiple)
#     def get_file_object(self, filename: str) -> Union[List[str], str]:
#         """
#         Method Name :   get_file_object
#         Description :   This method gets the file object from bucket_name bucket based on filename

#         Output      :   list of objects or object is returned based on filename
#         On Failure  :   Write an exception log and then raise an exception

#         Version     :   1.2
#         Revisions   :   moved setup to cloud
#         """
        
#         logging.info("Entered the get_file_object method of SimpleStorageService class")

#         try:
#             blob_list = [blob.name for blob in self.container_client.list_blobs(name_starts_with=filename)]

#             if not blob_list:
#                 raise Exception(f"No blobs found with prefix '{filename}'")

#             result = blob_list[0] if len(blob_list) == 1 else blob_list
#             logging.info("Exited the get_file_object method of SimpleStorageService class")
#             return result
#         except Exception as e:
#             raise HeartdieseaseException(e, sys) from e
            
#     def load_model(self, model_name: str, blob_prefix: str = None, model_dir: str = None):
#         """
#         Method Name :   load_model
#         Description :   This method loads the model_name model from bucket_name bucket with kwargs

#         Output      :   list of objects or object is returned based on filename
#         On Failure  :   Write an exception log and then raise an exception

#         Version     :   1.2
#         Revisions   :   moved setup to cloud
#         """

#         logging.info("Entered the load_model method of SimpleStorageService class")

#         try:
#             blob_path = f"{blob_prefix}/{model_name}" if blob_prefix else model_name
#             if model_dir:
#                 blob_path = f"{model_dir}/{blob_path}"

#             blob_client = self.container_client.get_blob_client(blob_path)
#             model_data = blob_client.download_blob().readall()
#             model = pickle.loads(model_data)
#             logging.info(f"Loaded model from blob: {blob_path}")
#             return model
#         except Exception as e:
#             raise HeartdieseaseException(e, sys) from e
        

#     def create_folder(self, folder_name: str, container_name: str) -> None:
#         """
#         Method Name :   create_folder
#         Description :   Creates a virtual folder by creating a zero-byte blob with the folder name as the blob name.
#         """
#         logging.info("Entered the create_folder method of SimpleStorageService class")
#         try:
#             # Azure Blob Storage does not have real folders. A "folder" is represented
#             # by a blob with a name containing the folder prefix.
#             # We create a zero-byte blob with the name 'folder_name/'.
#             blob_client = self.get_blob_client(f"{folder_name}/", container_name)
#             try:
#                 blob_client.upload_blob(b'', overwrite=False)
#             except ResourceNotFoundError:
#                 # Folder already exists (blob with this name exists), do nothing
#                 pass

#             logging.info("Exited the create_folder method of SimpleStorageService class")
#         except Exception as e:
#             raise HeartdieseaseException(e, sys) from e

#     def upload_file(self, from_filename: str, to_filename: str, container_name: str = None, remove: bool = True):
#         """
#         Method Name :   upload_file
#         Description :   Uploads the file from local storage (from_filename) 
#                         to the Azurite blob container with the blob name (to_filename).

#         Output      :   File is uploaded to Azurite container.
#         On Failure  :   Write an exception log and then raise an exception.

#         Version     :   1.0
#         Revisions   :   Refactored for Azurite / Azure Blob Storage Emulator.
#         """
#         logging.info("Entered the upload_file method of SimpleStorageService class")

#         try:
#             if container_name is None:
#                 container_name = self.container_name  # use default container from AzuriteClient

#             logging.info(f"Uploading '{from_filename}' to container '{container_name}' as '{to_filename}'")

#             #Get the blob client for the target file
#             blob_client = self.blob_service_client.get_blob_client(
#                 container=container_name,
#                 blob=to_filename
#             )

#             #Upload file from local path
#             with open(from_filename, "rb") as data:
#                 blob_client.upload_blob(data, overwrite=True)

#             logging.info(
#                 f"Successfully uploaded '{from_filename}' to blob '{to_filename}' in container '{container_name}'"
#             )

#             #Optionally remove local file after upload
#             if remove:
#                 os.remove(from_filename)
#                 logging.info(f"Local file '{from_filename}' removed after upload (remove=True)")
#             else:
#                 logging.info(f"Local file '{from_filename}' not removed (remove=False)")

#             logging.info("Exited the upload_file method of SimpleStorageService class")

#         except Exception as e:
#             raise HeartdieseaseException(e, sys) from e

#     def upload_df_as_csv(self,data_frame: DataFrame,local_filename: str,blob_filename: str,container_name: str = None,) -> None:
#             """
#             Method Name :   upload_df_as_csv
#             Description :   Uploads a DataFrame as a CSV file to Azurite blob storage.

#             Output      :   CSV file is created and uploaded to the Azurite container.
#             On Failure  :   Writes an exception log and raises HeartdieseaseException.

#             Version     :   1.0
#             Revisions   :   Refactored for Azurite / Azure Blob Storage.
#             """
#             logging.info("Entered the upload_df_as_csv method of SimpleStorageService class")

#             try:
#                 # Save dataframe locally as CSV
#                 data_frame.to_csv(local_filename, index=False, header=True)

#                 # Upload to Azurite container (using the previously refactored upload_file)
#                 self.upload_file(local_filename, blob_filename, container_name)

#                 logging.info("Exited the upload_df_as_csv method of SimpleStorageService class")

#             except Exception as e:
#                 raise HeartdieseaseException(e, sys) from e

#     # =======================================================
#     # Get DataFrame from blob (read blob -> convert to DataFrame)
#     # =======================================================
#     def get_df_from_object(self, blob_name: str) -> DataFrame:
#         """
#         Method Name :   get_df_from_object
#         Description :   Reads a blob CSV from Azurite and returns it as a pandas DataFrame.

#         Output      :   DataFrame is returned.
#         On Failure  :   Writes an exception log and raises HeartdieseaseException.

#         Version     :   1.0
#         Revisions   :   Refactored for Azurite / Azure Blob Storage.
#         """
#         logging.info("Entered the get_df_from_object method of SimpleStorageService class")

#         try:
#             blob_client = self.container_client.get_blob_client(blob_name)
#             blob_data = blob_client.download_blob().readall().decode("utf-8")

#             df = pd.read_csv(StringIO(blob_data))
#             logging.info("Exited the get_df_from_object method of SimpleStorageService class")
#             return df

#         except Exception as e:
#             raise HeartdieseaseException(e, sys) from e

#     # =======================================================
#     # Read CSV from Azurite container by blob name
#     # =======================================================
#     def read_csv(self, blob_filename: str, container_name: str = None) -> DataFrame:
#         """
#         Method Name :   read_csv
#         Description :   Downloads a CSV blob from Azurite and loads it into a DataFrame.

#         Output      :   DataFrame
#         On Failure  :   Writes an exception log and raises HeartdieseaseException.

#         Version     :   1.0
#         Revisions   :   Refactored for Azurite / Azure Blob Storage.
#         """
#         logging.info("Entered the read_csv method of SimpleStorageService class")

#         try:
#             if container_name is None:
#                 container_name = self.container_name

#             blob_client = self.blob_service_client.get_blob_client(
#                 container=container_name, blob=blob_filename
#             )

#             # Download blob content and read into pandas
#             blob_data = blob_client.download_blob().readall().decode("utf-8")
#             df = pd.read_csv(StringIO(blob_data))

#             logging.info("Exited the read_csv method of SimpleStorageService class")
#             return df

#         except Exception as e:
#             raise HeartdieseaseException(e, sys) from e