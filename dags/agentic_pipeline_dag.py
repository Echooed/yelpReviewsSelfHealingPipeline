import itertools
import json
import os

from datetime import timedelta, datetime
from airflow.sdk import dag, task, param, get_current_context, Param
import logging

from huggingface_hub import model_info

logger = logging.getLogger(__name__)

class Config:
    BASE_DIR = os.getenv('PIPELINE_BASE_DIR', '/home/dataspiro/PycharmProject/yelpReviewsSelfHealingPipeline')
    INPUT_FILE = os.getenv('PIPELINE_INPUT_FILE',
                           f'{BASE_DIR}/input/yelpReviewsSelfHealingPipeline/input/yelp_academic_dataset_review.json')
    OUTPUT_FILE = os.getenv('PIPELINE_OUTPUT_DIR',
                            f'{BASE_DIR}/output/')

    MAX_TEXT_LENGTH = int(os.getenv('PIPELINE_MAX_TEXT_LENGTH', 3000))
    DEFAULT_BATCH_SIZE = 100
    DEFAULT_OFFSET = 0

    #ollama settings
    OLLAMA_HOST = os.getenv('OLLAMA_HOST', 'http://localhost:11434')
    OLLAMA_MODEL = os.getenv('OLLAMA_MODEL', 'llama3.2')
    OLLAMA_TIMEOUT = int(os.getenv('OLLAMA_TIMEOUT', 120))
    OLLAMA_RETRIES = int(os.getenv('OLLAMA_RETRIES', 5))

default_args = {
    'owner': 'Michael Akindele',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=30),
}

def _load_ollama_model(model_name: str):
    import ollama
    logger.info(f"Loading OLLAMA model: {model_name}")
    logger.info(f'OLLAMA Host: {Config.OLLAMA_HOST}')

    client = ollama.Client(host=Config.OLLAMA_HOST)

    try:
        client.show(model_name)
        logger.info(f'OLLAMA model {model_name} is available.')
    except ollama.ResponseError as e:
        logger.info('Model not found locally. Attempting to pull from remote repository...')
        try:
            client.pull(model_name)
            logger.info(f'OLLAMA model {model_name} pulled from remote repository successfully.')
        except ollama.ResponseError as pull_error:
            logger.error(f'Failed to pull OLLAMA model {model_name}: {pull_error}')
            raise

    test_response = client.chat(
        model=model_name,
        messages=[{
            "role": "user",
            "content": "classify the sentiment: 'This is a great product!' as positive, negative, or neutral."
        }]
    )
    test_result = test_response['messages']['content'].strip().upper()
    logger.info(f'OLLAMA model validation passed the test response: {test_result}')

    return {
        'backend': 'ollama',
        'model_name': model_name,
        'ollama_host': Config.OLLAMA_HOST,
        'max_length': Config.MAX_TEXT_LENGTH,
        'status': 'loaded',
        'validated_at': datetime.now().isoformat(),
    }

def _load_from_file(params: dict, batch_size: int, offset: int):
    input_file = params.get('input_file', Config.INPUT_FILE)

    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Input file not found: {input_file}")

    reviews = []

    with open(input_file, 'r', encoding='utf-8') as f:
        sliced = itertools.islice(f, offset, offset+batch_size)

        for line in sliced:
            try:
                review = json.loads(line.strip())
                reviews.append({
                    'review_id': review.get('review_id'),
                    'business_id': review.get('business_id'),
                    'user_id': review.get('user_id'),
                    'stars': review.get('stars', 0),
                    'text': review.get('text'),
                    'date': review.get('date'),
                    'useful': review.get('useful', 0),
                    'funny': review.get('funny', 0),
                    'cool': review.get('cool', 0),
                })
            except json.JSONDecodeError as e:
                logger.warning(f"Skipping invalid JSON line: {e}")
                continue

    logger.info(f'Loaded {len(reviews)} reviews from file starting at offset {offset}.')
    return reviews

def _parse_ollama_response(response_text: str):
    try:
        clean_text = response_text.strip()
        if clean_text.startswith('```'):
            lines = clean_text.split('\n')
            clean_text = '\n'.join(lines[1:-1]) if lines[-1].strip() == '```' else '\n'.join(lines[1:])


        parsed = json.loads(clean_text)
        sentiment = parsed.get('sentiment', 'NEUTRAL').upper()
        confidence = float(parsed.get('confidence', 0.0))

        if sentiment not in ['POSITIVE', 'NEGATIVE', 'NEUTRAL']:
            sentiment = 'NEUTRAL'

        return {
            'label': sentiment,
            'score': min(max(confidence, 0.0), 1.0)
    }
    except (json.JSONDecodeError, ValueError, KeyError, TypeError) as e:
        upper_text = response_text.strip().upper()
        if 'POSITIVE' in upper_text:
            return {'label': 'POSITIVE', 'score': 0.75}
        elif 'NEGATIVE' in upper_text:
            return {'label': 'NEGATIVE', 'score': 0.75}
        return {'label': 'NEUTRAL', 'score': 0.5}

@dag(
    dag_id = 'self_healing_pipeline',
    default_args = default_args,
    description = "pipeline for self-healing pipeline using OLLAMA model",
    schedule = None,
    start_date=datetime(2025, 12, 28),
    catchup = False,
    tags = ['self-healing', 'sentiment_analysis', 'ollama', 'pipeline', 'yelp_reviews', 'nlp'],
    params = {
        'input_file': Param(
                    default=Config.INPUT_FILE,
                    type='string',
                    description='Path to the input JSON file containing Yelp reviews.'
                ),
                'batch_size': Param(
                    default=Config.DEFAULT_BATCH_SIZE,
                    type='integer',
                    description='Number of reviews to process in each batch.'
                ),
                'offset': Param(
                    default=Config.DEFAULT_OFFSET,
                    type='integer',
                    description='Offset to start reading reviews from the input file.'
                ),
                'ollama_model': Param(
                    default=Config.OLLAMA_MODEL,
                    type='string',
                    description='Name of the OLLAMA model to use for sentiment analysis.'
                ),

    },
    render_template_as_native_obj = True,

)

def self_healing_pipeline():
    @task
    def load_model():
        context = get_current_context()
        params = get_current_context()
        model_name = params.get['ollama_model',Config.OLLAMA_MODEL]
        logger.info(f"Using OLLAMA model: {model_name}")
        return _load_ollama_model(model_name)

    @task
    def load_reviews():
        context = get_current_context()
        params = context['params']
        batch_size = params.get['batch_size', Config.DEFAULT_BATCH_SIZE]
        offset = params.get('offset', Config.DEFAULT_OFFSET)
        logger.info(f'loading reviews with batch_size: {batch_size} and offset: {offset}')
        return _load_from_file(params, batch_size, offset)

    @task
    def diagnose_and_heal_batch(reviews: list[dict]):
        healed_reviews = [_heal_review(review) for review in reviews]
        healed_count = sum(1 for r in healed_reviews if r.get('was_healed', True))
        logger.info(f'Healed{healed_count} reviews')
        return healed_reviews




