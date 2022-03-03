import os
from datetime import datetime

import kfp
from kfp.v2 import dsl
from kfp.v2.dsl import component

from kfp.v2.google.client import AIPlatformClient

from components.bq_components import gcs_to_bq_table, run_query_save_to_bq_table_and_gcs

from anime_rec.data.bq_queries.common_data_queries import anime_list_query, user_list_query
from anime_rec.data.bq_queries.user_anime_data_queries import user_retrieved_animes_query, user_all_possible_animes_query
from anime_rec.data.bq_queries.user_anime_ml_data_queries import user_anime_retrieval_query
from anime_rec.data.bq_queries.user_anime_ml_data_queries import user_anime_ranking_query, user_anime_list_ranking_query

ANIME_AT_LEAST_RATED = 10000
USER_AT_LEAST_RATED = 500

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

def user_anime_retrieval_step(list_anime_data, list_user_data, project_id, dataset_id, data_format):
    """
        Data Load
    """
    train_retrieval_data = run_query_save_to_bq_table_and_gcs(
        query = user_anime_retrieval_query('TRAIN', anime_min_completed_and_rated = ANIME_AT_LEAST_RATED, user_min_completed_and_rated = USER_AT_LEAST_RATED),
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"user_anime_retrieval_train",
        gcs_output_format=data_format
    )
    train_retrieval_data.set_display_name("DATA: train user anime retrieval")
    
    val_retrieval_data = run_query_save_to_bq_table_and_gcs(
        query = user_anime_retrieval_query('VAL', anime_min_completed_and_rated = ANIME_AT_LEAST_RATED, user_min_completed_and_rated = USER_AT_LEAST_RATED),
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"user_anime_retrieval_val",
        gcs_output_format=data_format
    )
    val_retrieval_data.set_display_name("DATA: val user anime retrieval")
    
    test_retrieval_data = run_query_save_to_bq_table_and_gcs(
        query = user_anime_retrieval_query('TEST', anime_min_completed_and_rated = ANIME_AT_LEAST_RATED, user_min_completed_and_rated = USER_AT_LEAST_RATED),
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"user_anime_retrieval_test",
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
    infer_retrieval_to_bq = gcs_to_bq_table(
        gcs_input_data=infer_retrieval_model.outputs['output_data_path'],
        gcs_input_data_format=data_format,
        gcs_input_data_schema=[('user_id', 'STRING'), ('anime_id', 'STRING')],
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"user_anime_retrieval_infer"
    )
    infer_retrieval_to_bq.set_display_name("INFER: user anime retrieval to BQ")
    
    # Outputs user_id, anime_id to GCS and BQ (filter animes that user has watched)
    user_anime_to_rank = run_query_save_to_bq_table_and_gcs(
        query = user_retrieved_animes_query(f"{project_id}.{dataset_id}.user_anime_retrieval_infer", anime_min_completed_and_rated = ANIME_AT_LEAST_RATED, user_min_completed_and_rated = USER_AT_LEAST_RATED),
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"user_anime_retrieval_to_rank",
        gcs_output_format=data_format
    )
    user_anime_to_rank.after(infer_retrieval_to_bq)
    user_anime_to_rank.set_display_name("INFER: user anime retrieval to be ranked")

    return user_anime_to_rank

def user_anime_ranking_steps(list_anime_data, list_user_data, user_anime_to_rank, project_id, dataset_id, data_format):
    """
        Data Loading
    """
    train_ranking_data = run_query_save_to_bq_table_and_gcs(
        query = user_anime_ranking_query('TRAIN', anime_min_completed_and_rated = ANIME_AT_LEAST_RATED, user_min_completed_and_rated = USER_AT_LEAST_RATED),
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"user_anime_ranking_train",
        gcs_output_format=data_format
    )
    train_ranking_data.set_display_name("DATA: train user anime ranking")
    
    val_ranking_data = run_query_save_to_bq_table_and_gcs(
        query = user_anime_ranking_query('VAL', anime_min_completed_and_rated = ANIME_AT_LEAST_RATED, user_min_completed_and_rated = USER_AT_LEAST_RATED),
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"user_anime_ranking_val",
        gcs_output_format=data_format
    )
    val_ranking_data.set_display_name("DATA: validation user anime ranking")

    test_ranking_data = run_query_save_to_bq_table_and_gcs(
        query = user_anime_ranking_query('TEST', anime_min_completed_and_rated = ANIME_AT_LEAST_RATED, user_min_completed_and_rated = USER_AT_LEAST_RATED),
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"user_anime_ranking_test",
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
    
    infer_ranking_to_bq = gcs_to_bq_table(
        gcs_input_data=infer_ranking_model.outputs['output_data_path'],
        gcs_input_data_format=data_format,
        gcs_input_data_schema=[('user_id', 'STRING'), ('anime_id', 'STRING'), ("score", 'FLOAT')],
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"user_anime_ranking_infer"
    )
    infer_ranking_to_bq.set_display_name("INFER: user anime ranking to BQ")

def user_anime_list_ranking_steps(list_anime_data, list_user_data, user_anime_to_rank, project_id, dataset_id, data_format):
    """
        Data Loading
    """
    train_list_ranking_data = run_query_save_to_bq_table_and_gcs(
        query = user_anime_list_ranking_query('TRAIN', anime_min_completed_and_rated = ANIME_AT_LEAST_RATED, user_min_completed_and_rated = USER_AT_LEAST_RATED),
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"user_anime_list_ranking_train",
        gcs_output_format=data_format
    )
    train_list_ranking_data.set_display_name("DATA: train user anime list ranking")
    
    val_list_ranking_data = run_query_save_to_bq_table_and_gcs(
        query = user_anime_list_ranking_query('VAL', anime_min_completed_and_rated = ANIME_AT_LEAST_RATED, user_min_completed_and_rated = USER_AT_LEAST_RATED),
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"user_anime_list_ranking_val",
        gcs_output_format=data_format
    )
    val_list_ranking_data.set_display_name("DATA: validation user anime list ranking")

    test_list_ranking_data = run_query_save_to_bq_table_and_gcs(
        query = user_anime_list_ranking_query('TEST', anime_min_completed_and_rated = ANIME_AT_LEAST_RATED, user_min_completed_and_rated = USER_AT_LEAST_RATED),
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"user_anime_list_ranking_test",
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
    
    infer_list_ranking_to_bq = gcs_to_bq_table(
        gcs_input_data=infer_list_ranking_model.outputs['output_data_path'],
        gcs_input_data_format=data_format,
        gcs_input_data_schema=[('user_id', 'STRING'), ('anime_id', 'STRING'), ("score", 'FLOAT')],
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"user_anime_list_ranking_infer"
    )
    infer_list_ranking_to_bq.set_display_name("INFER: user anime list ranking to BQ")

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

    list_anime_data = run_query_save_to_bq_table_and_gcs(
        query = anime_list_query(anime_min_completed_and_rated = ANIME_AT_LEAST_RATED),
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"list_anime",
        gcs_output_format=data_format
    )
    list_anime_data.set_display_name("DATA: list anime")

    list_user_data = run_query_save_to_bq_table_and_gcs(
        query = user_list_query(anime_min_completed_and_rated = ANIME_AT_LEAST_RATED, user_min_completed_and_rated = USER_AT_LEAST_RATED),
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"list_user",
        gcs_output_format=data_format
    )
    list_user_data.set_display_name("DATA: list user")

    with dsl.Condition(run_retrieval=='true', name='yes-run-retrieval'):

        user_anime_to_rank = user_anime_retrieval_step(
            list_anime_data, 
            list_user_data,
            project_id, 
            dataset_id, 
            data_format
        )

        with dsl.Condition(list_ranking=='true', name='list-ranking'):
            _ = user_anime_list_ranking_steps(
                list_anime_data, 
                list_user_data, 
                user_anime_to_rank,
                project_id, 
                dataset_id, 
                data_format
            )
        
        with dsl.Condition(list_ranking=='false', name='ranking'):
            _ = user_anime_ranking_steps(
                list_anime_data, 
                list_user_data, 
                user_anime_to_rank, 
                project_id, 
                dataset_id, 
                data_format
            )

    with dsl.Condition(run_retrieval=='false', name='no-run-retrieval'):
        
        # Outputs user_id, anime_id to GCS and BQ
        user_cross_anime = run_query_save_to_bq_table_and_gcs(
            query = user_all_possible_animes_query(anime_min_completed_and_rated = ANIME_AT_LEAST_RATED, user_min_completed_and_rated = USER_AT_LEAST_RATED),
            project_id=project_id,
            destination_dataset_id=dataset_id,
            destination_table_id=f"user_cross_anime",
            gcs_output_format=data_format
        )
        user_cross_anime.set_display_name("DATA: user cross anime")

        # Outputs user_id, anime_id to GCS and BQ (filter anime user has watched)
        user_anime_to_rank = run_query_save_to_bq_table_and_gcs(
            query = user_retrieved_animes_query(
                f"{project_id}.{dataset_id}.user_cross_anime", 
                anime_min_completed_and_rated = ANIME_AT_LEAST_RATED, 
                user_min_completed_and_rated = USER_AT_LEAST_RATED
            ),
            project_id=project_id,
            destination_dataset_id=dataset_id,
            destination_table_id=f"user_cross_anime_to_rank",
            gcs_output_format=data_format
        )
        user_anime_to_rank.after(user_cross_anime)
        user_anime_to_rank.set_display_name("DATA: user cross anime to be ranked")
    
        with dsl.Condition(list_ranking=='true', name='list-ranking'):
            _ = user_anime_list_ranking_steps(
                list_anime_data, 
                list_user_data, 
                user_anime_to_rank,
                project_id, 
                dataset_id, 
                data_format
            )
        
        with dsl.Condition(list_ranking=='false', name='ranking'):
            _ = user_anime_ranking_steps(
                list_anime_data, 
                list_user_data, 
                user_anime_to_rank,
                project_id, 
                dataset_id, 
                data_format
            )


if __name__ == '__main__':
    
    PROJECT_ID = 'anime-rec-dev'
    DATA_FORMAT = 'csv'
    RUN_RETRIEVAL = 'true'
    LIST_RANKING = 'true'
    current_time = datetime.now().strftime("%Y%m%d%H%M%S")
    DATASET_ID = f"ml_pipelines_user_anime_retrieval_{RUN_RETRIEVAL}_list_ranking_{LIST_RANKING}_{current_time}"

    package_path = os.path.abspath(__file__ + "/../user_anime_recommendation_pipeline.json")
    
    kfp.v2.compiler.Compiler().compile(
        pipeline_func=user_anime_recommendation_pipeline,
        package_path=package_path
    )
    api_client = AIPlatformClient(
        project_id=PROJECT_ID,
        region='us-central1',
    )

    response = api_client.create_run_from_job_spec(
        job_spec_path=package_path,
        pipeline_root='gs://anime-rec-dev-ml-pipelines/user-anime-recommendation-pipeline',
        enable_caching=False,
        parameter_values={
            'project_id': PROJECT_ID,
            'dataset_id': DATASET_ID, 
            'data_format': DATA_FORMAT,
            'run_retrieval': RUN_RETRIEVAL, 
            'list_ranking': LIST_RANKING
        }
    )