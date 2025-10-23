from heart_disease.cloud_storage.azure_blob_storage import SimpleStorageService
from heart_disease.exception import HeartdieseaseException
from heart_disease.entity.estimator import HeartDiseaseModel
import sys
from pandas import DataFrame


class HeartDieseaseEstimator:
    """
    This class is used to save and retrieve us_visas model in blobS bucket and to do prediction
    """

    def __init__(self,blob_name,model_path,):
        """
        :param blob_name: Name of your model bucket
        :param model_path: Location of your model in bucket
        """
        self.blob_name = blob_name
        self.blobS = SimpleStorageService()
        self.model_path = model_path
        self.loaded_model:HeartDiseaseModel=None


    def is_model_present(self,model_path):
        try:
            return self.blobS.blob_are_available(blob_name=self.blob_name, model_path=model_path)
        except HeartdieseaseException as e:
            print(e)
            return False

    def load_model(self,)->HeartDiseaseModel:
        """
        Load the model from the model_path
        :return:
        """

        return self.blobS.load_model(self.model_path,blob_name=self.blob_name)

    def save_model(self,from_file,remove:bool=False)->None:
        """
        Save the model to the model_path
        :param from_file: Your local system model path
        :param remove: By default it is false that mean you will have your model locally available in your system folder
        :return:
        """
        try:
            self.blobS.upload_file(from_file,
                                to_filename=self.model_path,
                                container_name=self.blob_name,
                                remove=remove
                                )
        except Exception as e:
            raise HeartdieseaseException(e, sys)


    def predict(self,dataframe:DataFrame):
        """
        :param dataframe:
        :return:
        """
        try:
            if self.loaded_model is None:
                self.loaded_model = self.load_model()
            return self.loaded_model.predict(dataframe=dataframe)
        except Exception as e:
            raise HeartdieseaseException(e, sys)