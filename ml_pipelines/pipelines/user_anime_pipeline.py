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

from anime_rec.data.bq_queries.common_data_queries import anime_list_query, user_list_query, filter_recommendations
from anime_rec.data.bq_queries.user_anime_data_queries import user_all_possible_animes_query
from anime_rec.data.bq_queries.user_anime_ml_data_queries import user_anime_retrieval_query
from anime_rec.data.bq_queries.user_anime_ml_data_queries import user_anime_ranking_query, user_anime_list_ranking_query

ANIME_AT_LEAST_RATED = 1000
USER_AT_LEAST_RATED = 50

train_user_anime_retrieval_op = kfp.components.load_component_from_file(
    os.path.join(os.path.abspath(__file__ + "/../"), "components/user_anime/train/retrieval/component.yaml")
)
infer_user_anime_retrieval_op = kfp.components.load_component_from_file(
    os.path.join(os.path.abspath(__file__ + "/../"), "components/user_anime/infer/retrieval/component.yaml")
)
train_user_anime_ranking_op = kfp.components.load_component_from_file(
    os.path.join(os.path.abspath(__file__ + "/../"), "components/user_anime/train/ranking/component.yaml")
)
infer_user_anime_ranking_op = kfp.components.load_component_from_file(
    os.path.join(os.path.abspath(__file__ + "/../"), "components/user_anime/infer/ranking/component.yaml")
)

"""
    Retrieval
"""
def user_anime_train_retrieval_step(list_anime_data, list_user_data, project_id, dataset_id, data_format):
    """
        Data Load
    """
    train_retrieval_data = run_query_save_to_bq_table_and_gcs_and_vertexai(
        query = user_anime_retrieval_query('TRAIN', anime_min_completed_and_rated = ANIME_AT_LEAST_RATED, user_min_completed_and_rated = USER_AT_LEAST_RATED),
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"retrieval_train",
        gcs_output_format=data_format
    )
    train_retrieval_data.set_display_name("DATA: train user anime retrieval")
    
    val_retrieval_data = run_query_save_to_bq_table_and_gcs_and_vertexai(
        query = user_anime_retrieval_query('VAL', anime_min_completed_and_rated = ANIME_AT_LEAST_RATED, user_min_completed_and_rated = USER_AT_LEAST_RATED),
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"retrieval_val",
        gcs_output_format=data_format
    )
    val_retrieval_data.set_display_name("DATA: val user anime retrieval")
    
    test_retrieval_data = run_query_save_to_bq_table_and_gcs_and_vertexai(
        query = user_anime_retrieval_query('TEST', anime_min_completed_and_rated = ANIME_AT_LEAST_RATED, user_min_completed_and_rated = USER_AT_LEAST_RATED),
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"retrieval_test",
        gcs_output_format=data_format
    )
    test_retrieval_data.set_display_name("DATA: test user anime retrieval")

    """
        Model Training
    """
    train_retrieval_model = train_user_anime_retrieval_op(
        data_format=data_format,
        train_data_path = train_retrieval_data.outputs['output_data_path'], 
        val_data_path = val_retrieval_data.outputs['output_data_path'],
        test_data_path = test_retrieval_data.outputs['output_data_path'],
        anime_data_path = list_anime_data.outputs['output_data_path'],
        user_data_path = list_user_data.outputs['output_data_path'],
        user_anime_embedding_size = 128,
        learning_rate = 0.005,
        optimizer = 'adam',
        max_num_epochs = 5,
        early_stop_num_epochs = 1
    )
    train_retrieval_model.set_cpu_limit('16').set_memory_limit('32G')
    train_retrieval_model.set_display_name("TRAIN: user anime retrieval")

    train_retrieval_model_details = get_model_training_details(
        project_id=project_id,
        input_model = train_retrieval_model.outputs['model_path'],
        model_name="user_anime_retrieval_model",
        labels={
            "user_anime_embedding_size" : 128,
            "learning_rate" : 0.005,
            "optimizer" : 'adam',
            "max_num_epochs" : 5,
            "early_stop_num_epochs" : 1
        },
        input_metrics=train_retrieval_model.outputs['metrics_path']
    )
    train_retrieval_model_details.set_display_name("TRAIN: user anime retrieval display")
    
    return train_retrieval_model

def user_anime_infer_retrieval_step(list_user_data, train_retrieval_model, project_id, dataset_id, data_format):
    """
        Batch Inference
    """
    # Outputs user_id, anime_id to GCS
    infer_retrieval_model = infer_user_anime_retrieval_op(
        data_format = data_format,
        model_path = train_retrieval_model.outputs['model_path'],
        input_data_path = list_user_data.outputs['output_data_path']
    )
    infer_retrieval_model = infer_retrieval_model.set_cpu_limit('16').set_memory_limit('32G')
    infer_retrieval_model.set_display_name("INFER: user anime retrieval")

    # Outputs user_id, anime_id to BQ
    infer_retrieval_to_bq = gcs_to_bq_table_and_vertexai(
        gcs_input_data=infer_retrieval_model.outputs['output_data_path'],
        gcs_input_data_format=data_format,
        gcs_input_data_schema=[('user_id', 'STRING'), ('anime_id', 'STRING')],
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"retrieval_infer"
    )
    infer_retrieval_to_bq.set_display_name("INFER: user anime retrieval to BQ")

    return infer_retrieval_model

"""
    Ranking
"""
def user_anime_train_ranking_steps(list_anime_data, list_user_data, project_id, dataset_id, data_format):
    """
        Data Loading
    """
    train_ranking_data = run_query_save_to_bq_table_and_gcs_and_vertexai(
        query = user_anime_ranking_query('TRAIN', anime_min_completed_and_rated = ANIME_AT_LEAST_RATED, user_min_completed_and_rated = USER_AT_LEAST_RATED),
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"ranking_train",
        gcs_output_format=data_format
    )
    train_ranking_data.set_display_name("DATA: train user anime ranking")
    
    val_ranking_data = run_query_save_to_bq_table_and_gcs_and_vertexai(
        query = user_anime_ranking_query('VAL', anime_min_completed_and_rated = ANIME_AT_LEAST_RATED, user_min_completed_and_rated = USER_AT_LEAST_RATED),
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"ranking_val",
        gcs_output_format=data_format
    )
    val_ranking_data.set_display_name("DATA: validation user anime ranking")

    test_ranking_data = run_query_save_to_bq_table_and_gcs_and_vertexai(
        query = user_anime_ranking_query('TEST', anime_min_completed_and_rated = ANIME_AT_LEAST_RATED, user_min_completed_and_rated = USER_AT_LEAST_RATED),
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"ranking_test",
        gcs_output_format=data_format
    )
    test_ranking_data.set_display_name("DATA: test user anime ranking")
    
    """
        Model Training
    """
    train_ranking_model = train_user_anime_ranking_op(
        model_type = 'ranking',
        data_format=data_format,
        train_data_path = train_ranking_data.outputs['output_data_path'], 
        val_data_path = val_ranking_data.outputs['output_data_path'],
        test_data_path = test_ranking_data.outputs['output_data_path'],
        anime_data_path = list_anime_data.outputs['output_data_path'],
        user_data_path = list_user_data.outputs['output_data_path'],
        anime_embedding_size = 128,
        user_embedding_size = 256,
        scoring_layer_size = 128,
        learning_rate = 0.005,
        optimizer = 'adam',
        max_num_epochs = 5,
        early_stop_num_epochs = 1
    )
    train_ranking_model.set_cpu_limit('16').set_memory_limit('32G')
    train_ranking_model.set_display_name("TRAIN: user anime ranking")

    train_ranking_model_details = get_model_training_details(
        project_id=project_id,
        input_model = train_ranking_model.outputs['model_path'],
        model_name="user_anime_ranking_model",
        labels={
            "anime_embedding_size" : 128,
            "user_embedding_size": 128,
            "scoring_layer_size" : 128,
            "learning_rate" : 0.005,
            "optimizer" : 'adam',
            "max_num_epochs" : 5,
            "early_stop_num_epochs" : 1
        },
        input_metrics=train_ranking_model.outputs['metrics_path']
    )
    train_ranking_model_details.set_display_name("TRAIN: user anime ranking display")
    
    return train_ranking_model

def user_anime_infer_ranking_steps(train_ranking_model, user_anime_to_rank, project_id, dataset_id, data_format):
    """
        Batch Inference
    """
    infer_ranking_model = infer_user_anime_ranking_op(
        model_type = 'ranking',
        data_format = data_format,
        model_path = train_ranking_model.outputs['model_path'],
        input_data_path = user_anime_to_rank.outputs['output_data_path']
    )
    infer_ranking_model.set_cpu_limit('16').set_memory_limit('32G')
    infer_ranking_model.set_display_name("INFER: user anime ranking")
    
    infer_ranking_to_bq = gcs_to_bq_table_and_vertexai(
        gcs_input_data=infer_ranking_model.outputs['output_data_path'],
        gcs_input_data_format=data_format,
        gcs_input_data_schema=[('user_id', 'STRING'), ('anime_id', 'STRING'), ("score", 'FLOAT')],
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"ranking_infer"
    )
    infer_ranking_to_bq.set_display_name("INFER: user anime ranking to BQ")

    filter_infer_ranking = run_query_save_to_bq_table_and_gcs_and_vertexai(
        query = filter_recommendations(f"{project_id}.{dataset_id}.ranking_infer"),
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"ranking_infer_filter",
        gcs_output_format=data_format
    )
    filter_infer_ranking.after(infer_ranking_to_bq)
    filter_infer_ranking.set_display_name("INFER: user anime ranking filter")

"""
    List Ranking
"""
def user_anime_train_list_ranking_steps(list_anime_data, list_user_data, project_id, dataset_id, data_format):
    """
        Data Loading
    """
    train_list_ranking_data = run_query_save_to_bq_table_and_gcs_and_vertexai(
        query = user_anime_list_ranking_query('TRAIN', anime_min_completed_and_rated = ANIME_AT_LEAST_RATED, user_min_completed_and_rated = USER_AT_LEAST_RATED),
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"list_ranking_train",
        gcs_output_format=data_format
    )
    train_list_ranking_data.set_display_name("DATA: train user anime list ranking")
    
    val_list_ranking_data = run_query_save_to_bq_table_and_gcs_and_vertexai(
        query = user_anime_list_ranking_query('VAL', anime_min_completed_and_rated = ANIME_AT_LEAST_RATED, user_min_completed_and_rated = USER_AT_LEAST_RATED),
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"list_ranking_val",
        gcs_output_format=data_format
    )
    val_list_ranking_data.set_display_name("DATA: validation user anime list ranking")

    test_list_ranking_data = run_query_save_to_bq_table_and_gcs_and_vertexai(
        query = user_anime_list_ranking_query('TEST', anime_min_completed_and_rated = ANIME_AT_LEAST_RATED, user_min_completed_and_rated = USER_AT_LEAST_RATED),
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"list_ranking_test",
        gcs_output_format=data_format
    )
    test_list_ranking_data.set_display_name("DATA: test user anime list ranking")
    
    """
        Model Training
    """
    train_list_ranking_model = train_user_anime_ranking_op(
        model_type = 'list_ranking',
        data_format=data_format,
        train_data_path = train_list_ranking_data.outputs['output_data_path'], 
        val_data_path = val_list_ranking_data.outputs['output_data_path'],
        test_data_path = test_list_ranking_data.outputs['output_data_path'],
        anime_data_path = list_anime_data.outputs['output_data_path'],
        user_data_path = list_user_data.outputs['output_data_path'],
        anime_embedding_size = 128,
        user_embedding_size = 256,
        scoring_layer_size = 128,
        learning_rate = 0.005,
        optimizer = 'adam',
        max_num_epochs = 5,
        early_stop_num_epochs = 1
    )
    train_list_ranking_model.set_cpu_limit('16').set_memory_limit('32G')
    train_list_ranking_model.set_display_name("TRAIN: user anime list ranking")

    train_list_ranking_model_details = get_model_training_details(
        project_id=project_id,
        input_model = train_list_ranking_model.outputs['model_path'],
        model_name="user_anime_list_ranking_model",
        labels={
            "anime_embedding_size" : 128,
            "user_embedding_size": 128,
            "scoring_layer_size" : 128,
            "learning_rate" : 0.005,
            "optimizer" : 'adam',
            "max_num_epochs" : 5,
            "early_stop_num_epochs" : 1
        },
        input_metrics=train_list_ranking_model.outputs['metrics_path']
    )
    train_list_ranking_model_details.set_display_name("TRAIN: user anime list ranking display")

    return train_list_ranking_model

def user_anime_infer_list_ranking_steps(train_list_ranking_model, user_anime_to_rank, project_id, dataset_id, data_format):
    """
        Batch Inference
    """
    infer_list_ranking_model = infer_user_anime_ranking_op(
        model_type = 'list_ranking',
        data_format = data_format,
        model_path = train_list_ranking_model.outputs['model_path'],
        input_data_path = user_anime_to_rank.outputs['output_data_path']
    )
    infer_list_ranking_model.set_cpu_limit('16').set_memory_limit('32G')
    infer_list_ranking_model.set_display_name("INFER: user anime list ranking")
    
    infer_list_ranking_to_bq = gcs_to_bq_table_and_vertexai(
        gcs_input_data=infer_list_ranking_model.outputs['output_data_path'],
        gcs_input_data_format=data_format,
        gcs_input_data_schema=[('user_id', 'STRING'), ('anime_id', 'STRING'), ("score", 'FLOAT')],
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"list_ranking_infer"
    )
    infer_list_ranking_to_bq.set_display_name("INFER: user anime list ranking to BQ")

    filter_infer_list_ranking = run_query_save_to_bq_table_and_gcs_and_vertexai(
        query = filter_recommendations(f"{project_id}.{dataset_id}.list_ranking_infer"),
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"list_ranking_infer_filter",
        gcs_output_format=data_format
    )
    filter_infer_list_ranking.after(infer_list_ranking_to_bq)
    filter_infer_list_ranking.set_display_name("INFER: user anime list ranking filter")

@dsl.pipeline(
    name="user-anime-recommendation-pipeline"
)
def user_anime_recommendation_pipeline(
    project_id:str='anime-rec-dev', 
    dataset_id:str='ml_pipelines', 
    data_format:str='csv',
    run_retrieval:str='false', 
    list_ranking:str='false'
):

    list_anime_data = run_query_save_to_bq_table_and_gcs_and_vertexai(
        query = anime_list_query(anime_min_completed_and_rated = ANIME_AT_LEAST_RATED),
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"list_anime",
        gcs_output_format=data_format
    )
    list_anime_data.set_display_name("DATA: list anime")

    list_user_data = run_query_save_to_bq_table_and_gcs_and_vertexai(
        query = user_list_query(anime_min_completed_and_rated = ANIME_AT_LEAST_RATED, user_min_completed_and_rated = USER_AT_LEAST_RATED),
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"list_user",
        gcs_output_format=data_format
    )
    list_user_data.set_display_name("DATA: list user")

    with dsl.Condition(run_retrieval=='true', name='yes-run-retrieval'):
        with dsl.Condition(list_ranking=='true', name='list-ranking'):
            train_retrieval_model = user_anime_train_retrieval_step(
                list_anime_data, 
                list_user_data,
                project_id, 
                dataset_id, 
                data_format
            )
            train_list_ranking_model = user_anime_train_list_ranking_steps(
                list_anime_data, 
                list_user_data,
                project_id, 
                dataset_id, 
                data_format
            )
            infer_list_ranking_model = user_anime_infer_retrieval_step(
                list_user_data,
                train_retrieval_model,
                project_id, 
                dataset_id, 
                data_format
            )
            _ = user_anime_infer_list_ranking_steps(
                train_list_ranking_model,
                infer_list_ranking_model,
                project_id, 
                dataset_id, 
                data_format
            )
        with dsl.Condition(list_ranking=='false', name='ranking'):
            train_retrieval_model = user_anime_train_retrieval_step(
                list_anime_data, 
                list_user_data,
                project_id, 
                dataset_id, 
                data_format
            )
            train_ranking_model = user_anime_train_ranking_steps(
                list_anime_data, 
                list_user_data,
                project_id, 
                dataset_id, 
                data_format
            )
            infer_ranking_model = user_anime_infer_retrieval_step(
                list_user_data,
                train_retrieval_model,
                project_id, 
                dataset_id, 
                data_format
            )
            _ = user_anime_infer_ranking_steps(
                train_ranking_model,
                infer_ranking_model,
                project_id, 
                dataset_id, 
                data_format
            )

    with dsl.Condition(run_retrieval=='false', name='no-run-retrieval'):
        with dsl.Condition(list_ranking=='true', name='list-ranking'):
            # Outputs user_id, anime_id to GCS and BQ
            user_cross_anime = run_query_save_to_bq_table_and_gcs_and_vertexai(
                query = user_all_possible_animes_query(anime_min_completed_and_rated = ANIME_AT_LEAST_RATED, user_min_completed_and_rated = USER_AT_LEAST_RATED),
                project_id=project_id,
                destination_dataset_id=dataset_id,
                destination_table_id=f"user_cross_anime",
                gcs_output_format=data_format
            )
            user_cross_anime.set_display_name("DATA: user cross anime")
            
            train_list_ranking_model = user_anime_train_list_ranking_steps(
                list_anime_data, 
                list_user_data,
                project_id, 
                dataset_id, 
                data_format
            )

            _ = user_anime_infer_list_ranking_steps(
                train_list_ranking_model,
                user_cross_anime,
                project_id, 
                dataset_id, 
                data_format
            )
        with dsl.Condition(list_ranking=='false', name='ranking'):
           
            user_cross_anime = run_query_save_to_bq_table_and_gcs_and_vertexai(
                query = user_all_possible_animes_query(anime_min_completed_and_rated = ANIME_AT_LEAST_RATED, user_min_completed_and_rated = USER_AT_LEAST_RATED),
                project_id=project_id,
                destination_dataset_id=dataset_id,
                destination_table_id=f"user_cross_anime",
                gcs_output_format=data_format
            )
            user_cross_anime.set_display_name("DATA: user cross anime")
            
            train_ranking_model = user_anime_train_ranking_steps(
                list_anime_data, 
                list_user_data,
                project_id, 
                dataset_id, 
                data_format
            )

            _ = user_anime_infer_ranking_steps(
                train_ranking_model,
                user_cross_anime,
                project_id, 
                dataset_id, 
                data_format
            )


if __name__ == '__main__':
    
    current_time = datetime.now().strftime("%Y%m%d%H%M%S")

    PROJECT_ID = 'anime-rec-dev'
    DATA_FORMAT = 'csv'
    RUN_RETRIEVAL = 'true'
    LIST_RANKING = 'true'
    DATASET_ID = f"ml_user_anime_retrieval_{RUN_RETRIEVAL}_list_ranking_{LIST_RANKING}_{current_time}"
    DISPLAY_NAME = "user_anime_recommendation"
    JOB_ID = f"user-anime-retrieval-{RUN_RETRIEVAL}-list-ranking-{LIST_RANKING}-{current_time}"

    package_path = os.path.abspath(__file__ + "/../user_anime_recommendation_pipeline.json")
    
    kfp.v2.compiler.Compiler().compile(
        pipeline_func=user_anime_recommendation_pipeline,
        package_path=package_path
    )

    job = aiplatform.PipelineJob(
        display_name = DISPLAY_NAME,
        template_path = package_path,
        job_id = JOB_ID,
        pipeline_root = 'gs://anime-rec-dev-ml-pipelines/user-anime-recommendation-pipeline',
        parameter_values={
            'project_id': PROJECT_ID,
            'dataset_id': DATASET_ID, 
            'data_format': DATA_FORMAT,
            'run_retrieval': RUN_RETRIEVAL,
            'list_ranking': LIST_RANKING
        },
        enable_caching = False,
        labels = {'run_retrieval': RUN_RETRIEVAL, 'list-ranking': LIST_RANKING},
        credentials = service_account.Credentials.from_service_account_file(os.getenv('GOOGLE_APPLICATION_CREDENTIALS')),
        project = PROJECT_ID,
        location = 'us-central1'
    )
    job.submit(service_account = os.getenv('SERVICE_ACCOUNT'))