import sys

from pandas import DataFrame
from sklearn.pipeline import Pipeline

from heart_disease.exception import HeartdieseaseException
from heart_disease.logger import logging

class TargetValueMapping:
    def __init__(self):
        self.Certified:int = 0
        self.Denied:int = 1
    def _asdict(self):
        return self.__dict__
    def reverse_mapping(self):
        mapping_response = self._asdict()
        return dict(zip(mapping_response.values(),mapping_response.keys()))
    
class HeartDiseaseModel:
    def __init__(self, preprocessing_object: Pipeline, trained_model_object: object):
        """
        :param preprocessing_object: Input Object of preprocesser
        :param trained_model_object: Input Object of trained model 
        """
        self.preprocessing_object = preprocessing_object
        self.trained_model_object = trained_model_object

    def predict(self, dataframe: DataFrame) -> DataFrame:
        """
        Function accepts raw inputs and then transformed raw input using preprocessing_object
        which guarantees that the inputs are in the same format as the training data
        At last it performs prediction on transformed features
        """
        logging.info("Entered predict method of UTruckModel class")

        try:
            logging.info("Using the trained model to get predictions")

            transformed_feature = self.preprocessing_object.transform(dataframe)

            logging.info("Used the trained model to get predictions")
            return self.trained_model_object.predict(transformed_feature)

        except Exception as e:
            raise HeartdieseaseException(e, sys) from e

    def __repr__(self):
        return f"{type(self.trained_model_object).__name__}()"

    def __str__(self):
        return f"{type(self.trained_model_object).__name__}()"
# @staticmethod
# def handle_missing_values(X: pd.DataFrame, y: pd.Series) -> tuple[pd.DataFrame, pd.Series]:
#     """Removes rows with missing values in X and aligns y."""
#     try:
#         combined = pd.concat([X, y], axis=1)
#         before = combined.shape[0]
#         combined = combined.dropna()
#         after = combined.shape[0]
#         logging.info(f"Removed {before - after} rows containing missing values")
#         return combined.drop(columns=[y.name]), combined[y.name]
#     except Exception as e:
#         raise HeartdieseaseException(e, sys)

# @staticmethod
# def remove_duplicates(X: pd.DataFrame, y: pd.Series) -> tuple[pd.DataFrame, pd.Series]:
#     """Removes duplicate rows in X and aligns y."""
#     try:
#         combined = pd.concat([X, y], axis=1)
#         before = combined.shape[0]
#         combined = combined.drop_duplicates()
#         after = combined.shape[0]
#         logging.info(f"Removed {before - after} duplicate rows")
#         return combined.drop(columns=[y.name]), combined[y.name]
#     except Exception as e:
#         raise HeartdieseaseException(e, sys)
