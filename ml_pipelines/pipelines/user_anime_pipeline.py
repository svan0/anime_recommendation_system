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

def user_anime_retrieval_step(list_anime_data, list_user_data, current_time, project_id, dataset_id, data_format):
    """
        Data Load
    """
    train_retrieval_data = run_query_save_to_bq_table_and_gcs(
        query = user_anime_retrieval_query('TRAIN'),
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"user_anime_retrieval_train_{current_time}",
        gcs_output_format=data_format
    )
    train_retrieval_data.set_display_name("DATA: train user anime retrieval")
    
    val_retrieval_data = run_query_save_to_bq_table_and_gcs(
        query = user_anime_retrieval_query('VAL'),
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"user_anime_retrieval_val_{current_time}",
        gcs_output_format=data_format
    )
    val_retrieval_data.set_display_name("DATA: val user anime retrieval")
    
    test_retrieval_data = run_query_save_to_bq_table_and_gcs(
        query = user_anime_retrieval_query('TEST'),
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"user_anime_retrieval_test_{current_time}",
        gcs_output_format=data_format
    )
    test_retrieval_data.set_display_name("DATA: test user anime retrieval")

    """
        Model Training
    """
    train_retrieval_model = train_user_anime_retrieval_op(
        data_format=data_format,
        train_data_path = train_retrieval_data.outputs['gcs_output_data'], 
        val_data_path = val_retrieval_data.outputs['gcs_output_data'],
        test_data_path = test_retrieval_data.outputs['gcs_output_data'],
        anime_data_path = list_anime_data.outputs['gcs_output_data'],
        user_data_path = list_user_data.outputs['gcs_output_data'],
        user_anime_embedding_size = 128,
        learning_rate = 0.005,
        optimizer = 'adam',
        max_num_epochs = 30,
        early_stop_num_epochs = 5
    )
    train_retrieval_model.set_display_name("TRAIN: user anime retrieval")
    train_retrieval_model.set_cpu_limit('16').set_memory_limit('32G')

    """
        Batch Inference
    """
    infer_retrieval_model = infer_user_anime_retrieval_op(
        model_path = train_retrieval_model.outputs['model_path'],
        input_data_path = list_user_data.outputs['gcs_output_data']
    )
    infer_retrieval_model = infer_retrieval_model.set_cpu_limit('16').set_memory_limit('32G')
    infer_retrieval_model.set_display_name("INFER: user anime retrieval")

    infer_retrieval_to_bq = gcs_to_bq_table(
        gcs_input_data=infer_retrieval_model.outputs['output_data_path'],
        gcs_input_data_format=data_format,
        gcs_input_data_schema=[('user_id', 'STRING'), ('anime_id', 'STRING')],
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"user_anime_retrieval_infer_{current_time}"
    )
    infer_retrieval_to_bq.set_display_name("INFER: user anime retrieval to BQ")
    
    user_anime_to_rank = run_query_save_to_bq_table_and_gcs(
        query = user_retrieved_animes_query("{project_id}.{dataset_id}.user_anime_retrieval_infer_{current_time}"),
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"user_anime_retrieval_to_rank_{current_time}",
        gcs_output_format=data_format
    )
    user_anime_to_rank.after(infer_retrieval_to_bq)
    user_anime_to_rank.set_display_name("INFER: user anime retrieval to be ranked")

    return user_anime_to_rank

def user_anime_ranking_steps(list_anime_data, list_user_data, user_anime_to_rank, current_time, project_id, dataset_id, data_format):
    """
        Data Loading
    """
    train_ranking_data = run_query_save_to_bq_table_and_gcs(
        query = user_anime_ranking_query('TRAIN'),
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"user_anime_ranking_train_{current_time}",
        gcs_output_format=data_format
    )
    train_ranking_data.set_display_name("DATA: train user anime ranking")
    
    val_ranking_data = run_query_save_to_bq_table_and_gcs(
        query = user_anime_ranking_query('VAL'),
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"user_anime_ranking_val_{current_time}",
        gcs_output_format=data_format
    )
    val_ranking_data.set_display_name("DATA: validation user anime ranking")

    test_ranking_data = run_query_save_to_bq_table_and_gcs(
        query = user_anime_ranking_query('TEST'),
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"user_anime_ranking_test_{current_time}",
        gcs_output_format=data_format
    )
    test_ranking_data.set_display_name("DATA: test user anime ranking")
    
    """
        Model Training
    """
    train_ranking_model = train_user_anime_ranking_op(
        model_type = 'ranking',
        data_format=data_format,
        train_data_path = train_ranking_data.outputs['gcs_output_data'], 
        val_data_path = val_ranking_data.outputs['gcs_output_data'],
        test_data_path = test_ranking_data.outputs['gcs_output_data'],
        anime_data_path = list_anime_data.outputs['gcs_output_data'],
        user_data_path = list_user_data.outputs['gcs_output_data'],
        anime_embedding_size = 128,
        user_embedding_size = 256,
        scoring_layer_size = 128,
        learning_rate = 0.005,
        optimizer = 'adam',
        max_num_epochs = 30,
        early_stop_num_epochs = 5
    )
    train_ranking_model.set_display_name("TRAIN: user anime ranking")
    train_ranking_model.set_cpu_limit('8').set_memory_limit('32G')

    """
        Batch Inference
    """
    infer_ranking_model = infer_user_anime_ranking_op(
        model_type = 'ranking',
        model_path = train_ranking_model.outputs['model_path'],
        input_data_path = user_anime_to_rank.outputs['gcs_output_data']
    )
    infer_ranking_model.set_display_name("INFER: user anime ranking")
    
    infer_ranking_to_bq = gcs_to_bq_table(
        gcs_input_data=infer_ranking_model.outputs['output_data_path'],
        gcs_input_data_format=data_format,
        gcs_input_data_schema=[('user_id', 'STRING'), ('anime_id', 'STRING'), ("score", 'FLOAT')],
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"user_anime_ranking_infer_{current_time}"
    )
    infer_ranking_to_bq.set_display_name("INFER: user anime ranking to BQ")

def user_anime_list_ranking_steps(list_anime_data, list_user_data, user_anime_to_rank, current_time, project_id, dataset_id, data_format):
    """
        Data Loading
    """
    train_list_ranking_data = run_query_save_to_bq_table_and_gcs(
        query = user_anime_list_ranking_query('TRAIN'),
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"user_anime_list_ranking_train_{current_time}",
        gcs_output_format=data_format
    )
    train_list_ranking_data.set_display_name("DATA: train user anime list ranking")
    
    val_list_ranking_data = run_query_save_to_bq_table_and_gcs(
        query = user_anime_list_ranking_query('VAL'),
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"user_anime_list_ranking_val_{current_time}",
        gcs_output_format=data_format
    )
    val_list_ranking_data.set_display_name("DATA: validation user anime list ranking")

    test_list_ranking_data = run_query_save_to_bq_table_and_gcs(
        query = user_anime_list_ranking_query('TEST'),
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"user_anime_list_ranking_test_{current_time}",
        gcs_output_format=data_format
    )
    test_list_ranking_data.set_display_name("DATA: test user anime list ranking")
    
    """
        Model Training
    """
    train_list_ranking_model = train_user_anime_ranking_op(
        model_type = 'list_ranking',
        data_format=data_format,
        train_data_path = train_list_ranking_data.outputs['gcs_output_data'], 
        val_data_path = val_list_ranking_data.outputs['gcs_output_data'],
        test_data_path = test_list_ranking_data.outputs['gcs_output_data'],
        anime_data_path = list_anime_data.outputs['gcs_output_data'],
        user_data_path = list_user_data.outputs['gcs_output_data'],
        anime_embedding_size = 128,
        user_embedding_size = 256,
        scoring_layer_size = 128,
        learning_rate = 0.005,
        optimizer = 'adam',
        max_num_epochs = 30,
        early_stop_num_epochs = 5
    )
    train_list_ranking_model.set_display_name("TRAIN: user anime list ranking")
    train_list_ranking_model.set_cpu_limit('8').set_memory_limit('32G')

    """
        Batch Inference
    """
    infer_list_ranking_model = infer_user_anime_ranking_op(
        model_type = 'list_ranking',
        model_path = train_list_ranking_model.outputs['model_path'],
        input_data_path = user_anime_to_rank.outputs['gcs_output_data']
    )
    infer_list_ranking_model.set_display_name("INFER: user anime list ranking")
    
    infer_list_ranking_to_bq = gcs_to_bq_table(
        gcs_input_data=infer_list_ranking_model.outputs['output_data_path'],
        gcs_input_data_format=data_format,
        gcs_input_data_schema=[('user_id', 'STRING'), ('anime_id', 'STRING'), ("score", 'FLOAT')],
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"user_anime_list_ranking_infer_{current_time}"
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

    current_time = datetime.now().strftime("%Y%m%d%H%M%S")

    list_anime_data = run_query_save_to_bq_table_and_gcs(
        query = anime_list_query(),
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"list_anime_{current_time}",
        gcs_output_format=data_format
    )
    list_anime_data.set_display_name("DATA: list anime")

    list_user_data = run_query_save_to_bq_table_and_gcs(
        query = user_list_query(),
        project_id=project_id,
        destination_dataset_id=dataset_id,
        destination_table_id=f"list_user_{current_time}",
        gcs_output_format=data_format
    )
    list_user_data.set_display_name("DATA: list user")

    with dsl.Condition(run_retrieval=='true', name='yes-run-retrieval'):

        user_anime_to_rank = user_anime_retrieval_step(
            list_anime_data, 
            list_user_data,
            current_time, 
            project_id, 
            dataset_id, 
            data_format
        )

        with dsl.Condition(list_ranking=='true', name='list-ranking'):
            _ = user_anime_list_ranking_steps(
                list_anime_data, 
                list_user_data, 
                user_anime_to_rank, 
                current_time, 
                project_id, 
                dataset_id, 
                data_format
            )
        
        with dsl.Condition(list_ranking=='false', name='ranking'):
            _ = user_anime_ranking_steps(
                list_anime_data, 
                list_user_data, 
                user_anime_to_rank, 
                current_time, 
                project_id, 
                dataset_id, 
                data_format
            )

    with dsl.Condition(run_retrieval=='false', name='no-run-retrieval'):

        user_anime_to_rank = run_query_save_to_bq_table_and_gcs(
            query = user_all_possible_animes_query(),
            project_id=project_id,
            destination_dataset_id=dataset_id,
            destination_table_id=f"user_cross_anime_{current_time}",
            gcs_output_format=data_format
        )
        user_anime_to_rank.set_display_name("DATA: user cross anime not interacted with data")
    
        with dsl.Condition(list_ranking=='true', name='list-ranking'):
            _ = user_anime_list_ranking_steps(
                list_anime_data, 
                list_user_data, 
                user_anime_to_rank, 
                current_time, 
                project_id, 
                dataset_id, 
                data_format
            )
        
        with dsl.Condition(list_ranking=='false', name='ranking'):
            _ = user_anime_ranking_steps(
                list_anime_data, 
                list_user_data, 
                user_anime_to_rank, 
                current_time, 
                project_id, 
                dataset_id, 
                data_format
            )


if __name__ == '__main__':
    
    package_path = os.path.abspath(__file__ + "/../user_anime_recommendation_pipeline.json")
    
    kfp.v2.compiler.Compiler().compile(
        pipeline_func=user_anime_recommendation_pipeline,
        package_path=package_path
    )
    api_client = AIPlatformClient(
        project_id='anime-rec-dev',
        region='us-central1',
    )

    response = api_client.create_run_from_job_spec(
        job_spec_path=package_path,
        pipeline_root='gs://anime-rec-dev-ml-pipelines/user-anime-ranking-pipeline',
        enable_caching=False,
        parameter_values={
            'project_id': 'anime-rec-dev',
            'dataset_id': 'ml_pipelines', 
            'data_format': 'csv',
            'run_retrieval': 'true', 
            'list_ranking':'true'
        }
    )