import os

from airflow.sdk import dag, task, param, get_current_context
import logging

logger = logging.getLogger(__name__)

class Config: new*
    BASE_DIR = os.getenv('PIPELINE_BASE_DIR', '/home/dataspiro/PycharmProject/yelpReviewsSelfHealingPipeline')
