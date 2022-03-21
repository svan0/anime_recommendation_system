import os
from datetime import datetime

from dotenv import load_dotenv
load_dotenv()

import kfp
from kfp.v2 import dsl

from google.oauth2 import service_account
from google.cloud import aiplatform

from components.bq_components import gcs_to_bq_table_and_vertexai, run_query_save_to_bq_table_and_gcs_and_vertexai
from components.model_components import get_model_training_details

from anime_rec.data.bq_queries.common_data_queries import anime_list_query
from anime_rec.data.bq_queries.anime_anime_data_queries import user_last_anime_watched_query, user_last_anime_ranked_animes_query, anime_all_possible_anime_query
from anime_rec.data.bq_queries.anime_anime_ml_data_queries import anime_anime_retrieval_query
from anime_rec.data.bq_queries.anime_anime_ml_data_queries import anime_anime_pair_ranking_query

ANIME_AT_LEAST_RATED = 1000
USER_AT_LEAST_RATED = 50

train_anime_anime_retrieval_op = kfp.components.load_component_from_file(
    os.path.join(os.path.abspath(__file__ + "/../"), "components/anime_anime/train/retrieval/component.yaml")
)
infer_anime_anime_retrieval_op = kfp.components.load_component_from_file(
    os.path.join(os.path.abspath(__file__ + "/../"), "components/anime_anime/infer/retrieval/component.yaml")
)
train_anime_anime_ranking_op = kfp.components.load_component_from_file(
    os.path.join(os.path.abspath(__file__ + "/../"), "components/anime_anime/train/ranking/component.yaml")
)
infer_anime_anime_ranking_op = kfp.components.load_component_from_file(
    os.path.join(os.path.abspath(__file__ + "/../"), "components/anime_anime/infer/ranking/component.yaml")
)

def anime_anime_train_retrieval_steps(list_anime_data, project_id, dataset_id, data_format):
    """
        Data Load
    """
    train_retrieval_data = run_query_save_to_bq_table_and_gcs_and_vertexai(
        query = anime_anime_retrieval_query('TRAIN', anime_min_completed_and_rated = ANIME_AT_LEAST_RATED, user_min_completed_and_rated = USER_AT_LEAST_RATED),
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"retrieval_train",
        gcs_output_format=data_format
    )
    train_retrieval_data.set_display_name("DATA: train anime anime retrieval")

    val_retrieval_data = run_query_save_to_bq_table_and_gcs_and_vertexai(
        query = anime_anime_retrieval_query('VAL', anime_min_completed_and_rated = ANIME_AT_LEAST_RATED, user_min_completed_and_rated = USER_AT_LEAST_RATED),
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"retrieval_val",
        gcs_output_format=data_format
    )
    val_retrieval_data.set_display_name("DATA: val anime anime retrieval")

    test_retrieval_data = run_query_save_to_bq_table_and_gcs_and_vertexai(
        query = anime_anime_retrieval_query('TEST', anime_min_completed_and_rated = ANIME_AT_LEAST_RATED, user_min_completed_and_rated = USER_AT_LEAST_RATED),
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"retrieval_test",
        gcs_output_format=data_format
    )
    test_retrieval_data.set_display_name("DATA: test anime anime retrieval")

    """
        Model Training
    """
    train_retrieval_model = train_anime_anime_retrieval_op(
        data_format=data_format,
        train_data_path = train_retrieval_data.outputs['output_data_path'], 
        val_data_path = val_retrieval_data.outputs['output_data_path'],
        test_data_path = test_retrieval_data.outputs['output_data_path'],
        anime_data_path = list_anime_data.outputs['output_data_path'],
        anime_embedding_size = 128,
        learning_rate = 0.005,
        optimizer = 'adam',
        max_num_epochs = 5,
        early_stop_num_epochs = 1
    )
    train_retrieval_model.set_display_name("TRAIN: anime anime retrieval")
    train_retrieval_model.set_cpu_limit('16').set_memory_limit('32G')

    train_retrieval_model_details = get_model_training_details(
        project_id=project_id,
        input_model = train_retrieval_model.outputs['model_path'],
        model_name="anime_anime_retrieval_model",
        labels={
            "anime_embedding_size" : 128,
            "learning_rate" : 0.005,
            "optimizer" : 'adam',
            "max_num_epochs" : 5,
            "early_stop_num_epochs" : 1
        },
        input_metrics=train_retrieval_model.outputs['metrics_path']
    )
    train_retrieval_model_details.set_display_name("TRAIN: anime anime retrieval display")

    return train_retrieval_model

def anime_anime_infer_retrieval_steps(list_anime_data, train_retrieval_model, project_id, dataset_id, data_format):
    
    """
        Batch Inference
    """
    # Outputs anime_id, retrieved_anime_id to GCS
    infer_retrieval_model = infer_anime_anime_retrieval_op(
        data_format = data_format,
        model_path = train_retrieval_model.outputs['model_path'],
        input_data_path = list_anime_data.outputs['output_data_path']
    )
    infer_retrieval_model.set_display_name("INFER: anime anime retrieval")
    infer_retrieval_model = infer_retrieval_model.set_cpu_limit('16').set_memory_limit('32G')

    # Outputs anime_id, retrieved_anime_id to BQ
    infer_retrieval_to_bq = gcs_to_bq_table_and_vertexai(
        gcs_input_data=infer_retrieval_model.outputs['output_data_path'],
        gcs_input_data_format=data_format,
        gcs_input_data_schema=[('anime_id', 'STRING'), ('retrieved_anime_id', 'STRING')],
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"retrieval_infer"
    )
    infer_retrieval_to_bq.set_display_name("INFER: anime anime retrieval to BQ")
    
    return infer_retrieval_model

def anime_anime_train_ranking_steps(list_anime_data, project_id, dataset_id, data_format):
    """
        Data Loading
    """
    train_ranking_data = run_query_save_to_bq_table_and_gcs_and_vertexai(
        query = anime_anime_pair_ranking_query('TRAIN', anime_min_completed_and_rated = ANIME_AT_LEAST_RATED, user_min_completed_and_rated = USER_AT_LEAST_RATED),
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"ranking_train",
        gcs_output_format=data_format
    )
    train_ranking_data.set_display_name("DATA: train anime anime ranking")
    
    val_ranking_data = run_query_save_to_bq_table_and_gcs_and_vertexai(
        query = anime_anime_pair_ranking_query('VAL', anime_min_completed_and_rated = ANIME_AT_LEAST_RATED, user_min_completed_and_rated = USER_AT_LEAST_RATED),
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"ranking_val",
        gcs_output_format=data_format
    )
    val_ranking_data.set_display_name("DATA: validation anime anime ranking")

    test_ranking_data = run_query_save_to_bq_table_and_gcs_and_vertexai(
        query = anime_anime_pair_ranking_query('TEST', anime_min_completed_and_rated = ANIME_AT_LEAST_RATED, user_min_completed_and_rated = USER_AT_LEAST_RATED),
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"ranking_test",
        gcs_output_format=data_format
    )
    test_ranking_data.set_display_name("DATA: test anime anime ranking")
    
    """
        Model Training
    """
    train_ranking_model = train_anime_anime_ranking_op(
        data_format=data_format,
        train_data_path = train_ranking_data.outputs['output_data_path'], 
        val_data_path = val_ranking_data.outputs['output_data_path'],
        test_data_path = test_ranking_data.outputs['output_data_path'],
        anime_data_path = list_anime_data.outputs['output_data_path'],
        anime_embedding_size = 128,
        scoring_layer_size = 128,
        learning_rate = 0.005,
        optimizer = 'adam',
        max_num_epochs = 5,
        early_stop_num_epochs = 1
    )
    train_ranking_model.set_display_name("TRAIN: anime anime ranking")
    train_ranking_model.set_cpu_limit('16').set_memory_limit('64G')

    train_ranking_model_details = get_model_training_details(
        project_id=project_id,
        input_model = train_ranking_model.outputs['model_path'],
        model_name="anime_anime_ranking_model",
        labels={
            "anime_embedding_size" : 128,
            "scoring_layer_size" : 128,
            "learning_rate" : 0.005,
            "optimizer" : 'adam',
            "max_num_epochs" : 5,
            "early_stop_num_epochs" : 1
        },
        input_metrics=train_ranking_model.outputs['metrics_path']
    )
    train_ranking_model_details.set_display_name("TRAIN: anime anime ranking display")
    
    return train_ranking_model

def anime_anime_infer_ranking_steps(train_ranking_model, anime_anime_to_rank, project_id, dataset_id, data_format):
    """
        Batch Inference
    """
    # Outputs anime_id, retrieved_anime_id, score to GCS
    infer_ranking_model = infer_anime_anime_ranking_op(
        data_format = data_format,
        model_path = train_ranking_model.outputs['model_path'],
        input_data_path = anime_anime_to_rank.outputs['output_data_path']
    )
    infer_ranking_model.set_display_name("INFER: anime anime ranking")
    infer_ranking_model.set_cpu_limit('16').set_memory_limit('32G')

    # Outputs anime_id, retrieved_anime_id, score to BQ
    infer_ranking_to_bq = gcs_to_bq_table_and_vertexai(
        gcs_input_data=infer_ranking_model.outputs['output_data_path'],
        gcs_input_data_format=data_format,
        gcs_input_data_schema=[('anime_id', 'STRING'), ('retrieved_anime_id', 'STRING'), ("score", 'FLOAT')],
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"ranking_infer"
    )
    infer_ranking_to_bq.set_display_name("INFER: anime anime ranking to BQ")

    '''
        Get user recommendations based on last completed anime
    '''
    # Outputs user_id, anime_id to GCS and BQ
    user_last_watched = run_query_save_to_bq_table_and_gcs_and_vertexai(
        query = user_last_anime_watched_query(anime_min_completed_and_rated = ANIME_AT_LEAST_RATED),
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"user_last_watched",
        gcs_output_format=data_format
    )
    user_last_watched.set_display_name("INFER: user last watched")

    # Outputs user_id, anime_id, retrieved_anime_id, score to GCS and BQ
    user_ranked_recs = run_query_save_to_bq_table_and_gcs_and_vertexai(
        query = user_last_anime_ranked_animes_query(
            f"`{project_id}.{dataset_id}.user_last_watched`", 
            f"`{project_id}.{dataset_id}.ranking_infer`"
        ),
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"user_anime_ranked",
        gcs_output_format=data_format
    )
    user_ranked_recs.after(user_last_watched)
    user_ranked_recs.after(infer_ranking_to_bq)
    user_ranked_recs.set_display_name("INFER: user anime ranked")


@dsl.pipeline(
    name="anime-anime-recommendation-pipeline"
)
def anime_anime_recommendation_pipeline(
    project_id:str='anime-rec-dev', 
    dataset_id:str='ml_pipelines', 
    data_format:str='csv',
    run_retrieval:str='false'
):

    list_anime_data = run_query_save_to_bq_table_and_gcs_and_vertexai(
        query = anime_list_query(anime_min_completed_and_rated = ANIME_AT_LEAST_RATED),
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"list_anime",
        gcs_output_format=data_format
    )
    list_anime_data.set_display_name("DATA: list anime")

    with dsl.Condition(run_retrieval=='true', name='yes-run-retrieval'):
        
        train_retrieval_model = anime_anime_train_retrieval_steps(
            list_anime_data,
            project_id, 
            dataset_id, 
            data_format
        )

        train_ranking_model = anime_anime_train_ranking_steps(
            list_anime_data,
            project_id, 
            dataset_id, 
            data_format
        )

        # Outputs anime_id, retrieved_anime_id to GCS and BQ
        anime_anime_to_rank = anime_anime_infer_retrieval_steps(
            list_anime_data,
            train_retrieval_model,
            project_id, 
            dataset_id, 
            data_format
        )
        
        # Outputs user_id, anime_id, score to GCS and BQ
        _ = anime_anime_infer_ranking_steps(
            train_ranking_model, 
            anime_anime_to_rank,
            project_id, 
            dataset_id, 
            data_format
        )

    with dsl.Condition(run_retrieval=='false', name='no-run-retrieval'):
        
        # Outputs anime_id, retrieved_anime_id to GCS and BQ
        anime_anime_to_rank = run_query_save_to_bq_table_and_gcs_and_vertexai(
            query = anime_all_possible_anime_query(anime_min_completed_and_rated = ANIME_AT_LEAST_RATED),
            project_id=project_id,
            destination_dataset_id=dataset_id,
            destination_table_id=f"anime_cross_anime",
            gcs_output_format=data_format
        )
        anime_anime_to_rank.set_display_name("DATA: anime cross anime")

        train_ranking_model = anime_anime_train_ranking_steps(
            list_anime_data,
            project_id, 
            dataset_id, 
            data_format
        )

        # Outputs user_id, anime_id, score to GCS and BQ
        _ = anime_anime_infer_ranking_steps(
            train_ranking_model, 
            anime_anime_to_rank,
            project_id, 
            dataset_id, 
            data_format
        )


if __name__ == '__main__':
    
    current_time = datetime.now().strftime("%Y%m%d%H%M%S")

    PROJECT_ID = 'anime-rec-dev'
    DATA_FORMAT = 'csv'
    RUN_RETRIEVAL = 'true'
    DATASET_ID = f"ml_anime_anime_retrieval_{RUN_RETRIEVAL}_{current_time}"
    DISPLAY_NAME = "anime_anime_recommendation"
    JOB_ID = f"anime-anime-retrieval-{RUN_RETRIEVAL}-{current_time}"

    package_path = os.path.abspath(__file__ + "/../anime_anime_recommendation_pipeline.json")
    
    kfp.v2.compiler.Compiler().compile(
        pipeline_func=anime_anime_recommendation_pipeline,
        package_path=package_path
    )

    job = aiplatform.PipelineJob(
        display_name = DISPLAY_NAME,
        template_path = package_path,
        job_id = JOB_ID,
        pipeline_root = 'gs://anime-rec-dev-ml-pipelines/anime-anime-recommendation-pipeline',
        parameter_values={
            'project_id': PROJECT_ID,
            'dataset_id': DATASET_ID, 
            'data_format': DATA_FORMAT,
            'run_retrieval': RUN_RETRIEVAL
        },
        enable_caching = False,
        labels = {'run_retrieval': RUN_RETRIEVAL},
        credentials = service_account.Credentials.from_service_account_file(os.getenv('GOOGLE_APPLICATION_CREDENTIALS')),
        project = PROJECT_ID,
        location = 'us-central1'
    )
    job.submit(service_account = os.getenv('SERVICE_ACCOUNT'))