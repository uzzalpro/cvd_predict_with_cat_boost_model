from heart_disease.pipline.training_pipeline import TrainingPipeline

pipeline=TrainingPipeline()
pipeline.run_pipeline()
# from heart_disease.cloud_storage.azure_blob_storage import SimpleStorageService
# import logging
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# pipeline=SimpleStorageService()
# pipeline.run_pipeline()