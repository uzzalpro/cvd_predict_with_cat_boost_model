import os
import sys

import numpy as np
import pandas as pd
from heart_disease.entity.config_entity import HeartDiseasePredictorConfig
from heart_disease.entity.blob_estimator import HeartDieseaseEstimator 
from heart_disease.exception import HeartdieseaseException
from heart_disease.logger import logging
from heart_disease.utils.main_utils import read_yaml_file
from pandas import DataFrame



class HeartDieseaseData:
    def __init__(
        self,
        age: int,
        sex: str,
        cp: str,
        trestbps: int,
        restecg: str,
        thalch: int,
        exang: int,
        oldpeak: int,
        slope: str,

    ):
        """
        HeartDiseaseData constructor
        Input: all features of the trained model for prediction
        """
        try:

            self.age = age
            self.sex = sex
            self.cp = cp
            self.trestbps = trestbps
            self.restecg = restecg
            self.thalch = thalch
            self.exang = exang
            self.oldpeak = oldpeak
            self.slope = slope


        except Exception as e:
            raise HeartdieseaseException(e, sys) from e
        
    
    def get_heartdisease_input_data_frame(self)-> DataFrame:
        """
        This function returns a DataFrame from HeartDieseaseData class input
        """
        try:
            
            heart_disease_input_dict = self.get_heartdisease_data_as_dict()
            return DataFrame(heart_disease_input_dict)
        
        except Exception as e:
            raise HeartdieseaseException(e, sys) from e


    def get_heartdisease_data_as_dict(self):
        """
        This function returns a dictionary from HeartDieseaseData class input 
        """
        logging.info("Entered get_heartdisease_data_as_dict method as HeartDieseaseData class")

        try:
            input_data = {
                "age": [self.age],
                "sex": [self.sex],
                "cp": [self.cp],
                "trestbps": [self.trestbps],
                "restecg": [self.restecg],
                "thalch": [self.thalch],
                "exang": [self.exang],
                "oldpeak": [self.oldpeak],
                "slope": [self.slope],
            }

            logging.info("Created get_heart disease data dict")

            logging.info("Exited get_heartdisease_data_as_dict method as HeartDiseaseData class")

            return input_data

        except Exception as e:
            raise HeartdieseaseException(e, sys) from e
        

    


class HeartDiseaseClassifier:
    def __init__(self,prediction_pipeline_config: HeartDiseasePredictorConfig = HeartDiseasePredictorConfig(),) -> None:
        """
        :param prediction_pipeline_config: Configuration for prediction the value
        """
        try:
            # self.schema_config = read_yaml_file(SCHEMA_FILE_PATH)
            self.prediction_pipeline_config = prediction_pipeline_config
        except Exception as e:
            raise HeartdieseaseException(e, sys)

    def predict(self, dataframe) -> str:
        """
        This is the method of HeartDiseaseClassifier
        Returns: Prediction in string format
        """
        try:
            logging.info("Entered predict method of HeartDiseaseClassifier class")
            model = HeartDieseaseEstimator(
                blob_name=self.prediction_pipeline_config.model_blob_name,
                model_path=self.prediction_pipeline_config.model_file_path,
            )
            result =  model.predict(dataframe)
            
            return result
        
        except Exception as e:
            raise HeartdieseaseException(e, sys)