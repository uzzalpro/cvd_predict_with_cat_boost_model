import sys

from heart_disease.cloud_storage.azure_blob_storage import SimpleStorageService
from heart_disease.exception import HeartdieseaseException
from heart_disease.logger import logging
from heart_disease.entity.artifact_entity import ModelPusherArtifact, ModelEvaluationArtifact
from heart_disease.entity.config_entity import ModelPusherConfig
from heart_disease.entity.blob_estimator import HeartDieseaseEstimator


class ModelPusher:
    def __init__(self, model_evaluation_artifact: ModelEvaluationArtifact,
                 model_pusher_config: ModelPusherConfig):
        """
        :param model_evaluation_artifact: Output reference of data evaluation artifact stage
        :param model_pusher_config: Configuration for model pusher
        """
        self.blob = SimpleStorageService()
        self.model_evaluation_artifact = model_evaluation_artifact
        self.model_pusher_config = model_pusher_config
        self.heartdiesease_estimator = HeartDieseaseEstimator(blob_name=model_pusher_config.blob_name,
                                model_path=model_pusher_config.blob_model_key_path)

    def initiate_model_pusher(self) -> ModelPusherArtifact:
        """
        Method Name :   initiate_model_evaluation
        Description :   This function is used to initiate all steps of the model pusher
        
        Output      :   Returns model evaluation artifact
        On Failure  :   Write an exception log and then raise an exception
        """
        logging.info("Entered initiate_model_pusher method of ModelTrainer class")

        try:
            logging.info("Uploading artifacts folder to blob bucket")

            self.heartdiesease_estimator.save_model(from_file=self.model_evaluation_artifact.trained_model_path)


            model_pusher_artifact = ModelPusherArtifact(blob_name=self.model_pusher_config.blob_name,
                                                        blob_model_path=self.model_pusher_config.blob_model_key_path)

            logging.info("Uploaded artifacts folder to blob bucket")
            logging.info(f"Model pusher artifact: [{model_pusher_artifact}]")
            logging.info("Exited initiate_model_pusher method of ModelTrainer class")
            
            return model_pusher_artifact
        except Exception as e:
            raise HeartdieseaseException(e, sys) from e