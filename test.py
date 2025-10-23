import sys
import logging
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError


class HeartdieseaseException(Exception):
    def __init__(self, message, *args):
        super().__init__(message)
        logging.error(f"HeartdieseaseException occurred: {message}")


class SimpleStorageService:
    def __init__(self, connection_string: str, account_container: str):
        """
        Initializes with the connection string and creates a BlobServiceClient.
        Ensures that the target container exists.
        """
        try:
            logging.info("Connecting to Azurite via BlobServiceClient...")
            self.service_client = BlobServiceClient.from_connection_string(connection_string)
            self.account_container_name = account_container

            # Create container if missing
            try:
                self.service_client.create_container(account_container)
                logging.info(f"Container '{account_container}' created successfully.")
            except ResourceExistsError:
                logging.info(f"Container '{account_container}' already exists.")

            # Get container client
            self.account_container = self.service_client.get_container_client(account_container)
            logging.info("Connected to Azurite container successfully.")

        except Exception as e:
            raise HeartdieseaseException(f"Failed to connect to BlobServiceClient: {e}", sys)

    def is_connected(self) -> bool:
        """
        Performs a health check by attempting to list containers.
        """
        try:
            _ = list(self.service_client.list_containers(results_per_page=1))
            logging.info("Successfully connected to Azurite blob service.")
            return True
        except Exception as e:
            logging.error(f"Connection to Azurite failed: {e}")
            return False

    def blob_are_available(self, blob_name: str) -> bool:
        """
        Checks if a blob exists within the configured container.
        """
        try:
            if not self.is_connected():
                return False

            logging.debug(f"Listing blobs with prefix: {blob_name}")
            blob_list = self.account_container.list_blobs(name_starts_with=blob_name)

            logging.info("Azurite blob availability check complete.")
            return any(blob.name == blob_name for blob in blob_list)

        except ResourceNotFoundError:
            logging.error(f"Container '{self.account_container_name}' not found.")
            raise HeartdieseaseException(f"Container '{self.account_container_name}' not found.", sys)
        except Exception as e:
            raise HeartdieseaseException(e, sys)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

    azurite_connection_string = (
        "DefaultEndpointsProtocol=http;"
        "AccountName=devstoreaccount1;"
        "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
        "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
    )

    azurite_account_container = "cvd-uploads"
    test_blob = "test.csv"

    try:
        pipeline = SimpleStorageService(azurite_connection_string, azurite_account_container)
        available = pipeline.blob_are_available(test_blob)
        print(f"Blob '{test_blob}' available: {available}")
    except HeartdieseaseException as e:
        print(f"An exception was caught: {e}")
