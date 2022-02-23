import os
import kfp
from kfp.v2 import dsl
from kfp.v2.dsl import component

from kfp.v2.google.client import AIPlatformClient

from components.bq_components import load_big_query_data, load_big_query_external_data

from ml_package.utils.bq_queries.user_anime_ml_data_queries import user_anime_retrieval_query
from ml_package.utils.bq_queries.user_anime_ml_data_queries import user_anime_ranking_query
from ml_package.utils.bq_queries.user_anime_ml_data_queries import user_anime_list_ranking_query
from ml_package.utils.bq_queries.common_data_queries import anime_list_query
from ml_package.utils.bq_queries.user_anime_data_queries import user_list_query
from ml_package.utils.bq_queries.user_anime_data_queries import user_retrieved_animes_query
from ml_package.utils.bq_queries.user_anime_data_queries import user_all_possible_animes_query


train_user_anime_retrieval = kfp.components.load_component_from_file(
    os.path.join(os.path.abspath(__file__ + "/../"), "components/user_anime/train_retrieval/component.yaml")
)
infer_user_anime_retrieval = kfp.components.load_component_from_file(
    os.path.join(os.path.abspath(__file__ + "/../"), "components/user_anime/infer_retrieval/component.yaml")
)
train_user_anime_ranking = kfp.components.load_component_from_file(
    os.path.join(os.path.abspath(__file__ + "/../"), "components/user_anime/train_ranking/component.yaml")
)
infer_user_anime_ranking = kfp.components.load_component_from_file(
    os.path.join(os.path.abspath(__file__ + "/../"), "components/user_anime/infer_ranking/component.yaml")
)

def retrieval_model_steps(all_user_data, all_anime_data):

    train_retrieval_data = load_big_query_data(query = user_anime_retrieval_query('TRAIN'))
    train_retrieval_data.set_display_name("train data : user anime retrieval")
    
    val_retrieval_data = load_big_query_data(query = user_anime_retrieval_query('VAL'))
    val_retrieval_data.set_display_name("val data : user anime retrieval")
    
    test_retrieval_data = load_big_query_data(query = user_anime_retrieval_query('TEST'))
    test_retrieval_data.set_display_name("test data : user anime retrieval")

    train_retrieval_model = train_user_anime_retrieval(
        train_data_path = train_retrieval_data.outputs['output_csv'], 
        val_data_path = val_retrieval_data.outputs['output_csv'],
        test_data_path = test_retrieval_data.outputs['output_csv'],
        all_anime_data_path = all_anime_data.outputs['output_csv'],
        all_user_data_path = all_user_data.outputs['output_csv'],
        user_anime_embedding_size = 128,
        learning_rate = 0.005,
        optimizer = 'adam',
        max_num_epochs = 30,
        early_stop_num_epochs = 5
    )
    train_retrieval_model.set_display_name("train retrieval model")
    train_retrieval_model.set_cpu_limit('16').set_memory_limit('32G')

    infer_retrieval_model = infer_user_anime_retrieval(
        model_path = train_retrieval_model.outputs['output_model_path'],
        input_data_path = all_user_data.outputs['output_csv']
    )
    infer_retrieval_model.set_display_name("infer retrieval model")

    unknown_retrieved = load_big_query_external_data(
        external_table_uri = infer_retrieval_model.outputs['output_data_path'],
        external_table_schema = [
            ("user_id", "STRING"),
            ("retrieved_anime_id", "STRING")
        ],
        external_table_id = 'unknown_retrieved',
        query = user_retrieved_animes_query('unknown_retrieved')
    )
    unknown_retrieved.set_display_name("remove known retrieved animes")

    return unknown_retrieved

def ranking_model_steps(all_user_data, all_anime_data, unknown_retrieved):

    train_ranking_data = load_big_query_data(query = user_anime_ranking_query('TRAIN'))
    train_ranking_data.set_display_name("train data : user anime ranking")
    
    val_ranking_data = load_big_query_data(query = user_anime_ranking_query('VAL'))
    val_ranking_data.set_display_name("validation data : user anime ranking")
    
    test_ranking_data = load_big_query_data(query = user_anime_ranking_query('TEST'))
    test_ranking_data.set_display_name("test data : user anime ranking")
    
    train_ranking_model = train_user_anime_ranking(
        model_type = 'ranking',
        train_data_path = train_ranking_data.outputs['output_csv'], 
        val_data_path = val_ranking_data.outputs['output_csv'],
        test_data_path = test_ranking_data.outputs['output_csv'],
        all_anime_data_path = all_anime_data.outputs['output_csv'],
        all_user_data_path = all_user_data.outputs['output_csv'],
        anime_embedding_size = 128,
        user_embedding_size = 256,
        scoring_layer_size = 128,
        learning_rate = 0.005,
        optimizer = 'adam',
        max_num_epochs = 30,
        early_stop_num_epochs = 5
    )
    train_ranking_model.set_display_name("train ranking model")
    train_ranking_model.set_cpu_limit('16').set_memory_limit('32G')
    
    infer_ranking_model = infer_user_anime_ranking(
        model_type = 'ranking',
        model_path = train_ranking_model.outputs['output_model_path'],
        input_data_path = unknown_retrieved.outputs['output_csv']
    )
    infer_ranking_model.set_display_name("infer ranking model")

    return infer_ranking_model

def list_ranking_model_steps(all_user_data, all_anime_data, unknown_retrieved):
    
    train_ranking_data = load_big_query_data(query = user_anime_list_ranking_query('TRAIN'))
    train_ranking_data.set_display_name("train data : user anime list ranking")
    
    val_ranking_data = load_big_query_data(query = user_anime_list_ranking_query('VAL'))
    val_ranking_data.set_display_name("validation data : user anime list ranking")
    
    test_ranking_data = load_big_query_data(query = user_anime_list_ranking_query('TEST'))
    test_ranking_data.set_display_name("test data : user anime list ranking")
    
    train_ranking_model = train_user_anime_ranking(
        model_type = 'list_ranking',
        train_data_path = train_ranking_data.outputs['output_csv'], 
        val_data_path = val_ranking_data.outputs['output_csv'],
        test_data_path = test_ranking_data.outputs['output_csv'],
        all_anime_data_path = all_anime_data.outputs['output_csv'],
        all_user_data_path = all_user_data.outputs['output_csv'],
        anime_embedding_size = 128,
        user_embedding_size = 256,
        scoring_layer_size = 128,
        learning_rate = 0.005,
        optimizer = 'adam',
        max_num_epochs = 30,
        early_stop_num_epochs = 5
    )
    train_ranking_model.set_display_name("train list ranking model")
    train_ranking_model.set_cpu_limit('16').set_memory_limit('32G')
    
    infer_ranking_model = infer_user_anime_ranking(
        model_type = 'list_ranking',
        model_path = train_ranking_model.outputs['output_model_path'],
        input_data_path = unknown_retrieved.outputs['output_csv']
    )
    infer_ranking_model.set_display_name("infer list ranking model")

    return infer_ranking_model


@dsl.pipeline(
    name="user-anime-recommendation-pipeline"
)
def user_anime_recommendation_pipeline(run_retrieval: str = 'false', list_ranking: str = 'false'):

    all_anime_data = load_big_query_data(query = anime_list_query())
    all_anime_data.set_display_name("all anime data")
    
    all_user_data = load_big_query_data(query = user_list_query())
    all_user_data.set_display_name("all user data")

    with dsl.Condition(run_retrieval=='true'):
        unknown_retrieved = retrieval_model_steps(all_user_data, all_anime_data)

        with dsl.Condition(list_ranking=='true'):
            _ = list_ranking_model_steps(all_user_data, all_anime_data, unknown_retrieved)
        
        with dsl.Condition(list_ranking=='false'):
            _ = ranking_model_steps(all_user_data, all_anime_data, unknown_retrieved)
    
    with dsl.Condition(run_retrieval=='false'):
        unknown_retrieved = load_big_query_data(query = user_all_possible_animes_query())
        unknown_retrieved.set_display_name("all anime not interacted with data")
    
        with dsl.Condition(list_ranking=='true'):
            _ = list_ranking_model_steps(all_user_data, all_anime_data, unknown_retrieved)
        
        with dsl.Condition(list_ranking=='false'):
            _ = ranking_model_steps(all_user_data, all_anime_data, unknown_retrieved)


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
        pipeline_root='gs://anime-rec-dev-ml-pipelines/user-anime-rec-pipeline',
        enable_caching=False,
        parameter_values={
            'run_retrieval' : 'true',
            'list_ranking' : 'true'
        }
    )

    response = api_client.create_run_from_job_spec(
        job_spec_path=package_path,
        pipeline_root='gs://anime-rec-dev-ml-pipelines/user-anime-rec-pipeline',
        enable_caching=False,
        parameter_values={
            'run_retrieval' : 'true',
            'list_ranking' : 'false'
        }
    )
    response = api_client.create_run_from_job_spec(
        job_spec_path=package_path,
        pipeline_root='gs://anime-rec-dev-ml-pipelines/user-anime-rec-pipeline',
        enable_caching=False,
        parameter_values={
            'run_retrieval' : 'false',
            'list_ranking' : 'true'
        }
    )
    response = api_client.create_run_from_job_spec(
        job_spec_path=package_path,
        pipeline_root='gs://anime-rec-dev-ml-pipelines/user-anime-rec-pipeline',
        enable_caching=False,
        parameter_values={
            'run_retrieval' : 'true',
            'list_ranking' : 'true'
        }
    )

    
