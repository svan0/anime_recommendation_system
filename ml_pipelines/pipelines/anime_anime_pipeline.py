import os
import sys
import kfp
from kfp.v2 import dsl

from kfp.v2.google.client import AIPlatformClient

from components.bq_components import load_big_query_data, load_big_query_external_data

from ml_package.utils.bq_queries.anime_anime_ml_data_queries import anime_anime_retrieval_query
from ml_package.utils.bq_queries.anime_anime_ml_data_queries import anime_anime_pair_ranking_query
from ml_package.utils.bq_queries.common_data_queries import anime_list_query
from ml_package.utils.bq_queries.anime_anime_data_queries import user_last_anime_watched_query
from ml_package.utils.bq_queries.anime_anime_data_queries import user_last_anime_retrieved_animes_query
from ml_package.utils.bq_queries.user_anime_data_queries import user_all_possible_animes_query


train_anime_anime_retrieval = kfp.components.load_component_from_file(
    os.path.join(os.path.abspath(__file__ + "/../"), "components/anime_anime/train_retrieval/component.yaml")
)
infer_anime_anime_retrieval = kfp.components.load_component_from_file(
    os.path.join(os.path.abspath(__file__ + "/../"), "components/anime_anime/infer_retrieval/component.yaml")
)
train_anime_anime_ranking = kfp.components.load_component_from_file(
    os.path.join(os.path.abspath(__file__ + "/../"), "components/anime_anime/train_ranking/component.yaml")
)
infer_anime_anime_ranking = kfp.components.load_component_from_file(
    os.path.join(os.path.abspath(__file__ + "/../"), "components/anime_anime/infer_ranking/component.yaml")
)

def retrieval_model_steps(all_anime_data):
    
    train_retrieval_data = load_big_query_data(query = anime_anime_retrieval_query('TRAIN'))
    train_retrieval_data.set_display_name("train data : anime anime retrieval")
    
    val_retrieval_data = load_big_query_data(query = anime_anime_retrieval_query('VAL'))
    val_retrieval_data.set_display_name("validation data : anime anime retrieval")
    
    test_retrieval_data = load_big_query_data(query = anime_anime_retrieval_query('TEST'))
    test_retrieval_data.set_display_name("test data : anime anime retrieval")

    train_retrieval_model = train_anime_anime_retrieval(
        train_data_path = train_retrieval_data.outputs['output_csv'], 
        val_data_path = val_retrieval_data.outputs['output_csv'],
        test_data_path = test_retrieval_data.outputs['output_csv'],
        all_anime_data_path = all_anime_data.outputs['output_csv'],
        anime_embedding_size = 256,
        learning_rate = 0.05,
        optimizer = 'adagrad',
        max_num_epochs = 30,
        early_stop_num_epochs = 5
    )
    train_retrieval_model.set_display_name("train retrieval model")
    train_retrieval_model.set_cpu_limit('16').set_memory_limit('32G')
    # Quotas !
    # train_retrieval_model.add_node_selector_constraint('cloud.google.com/gke-accelerator', 'NVIDIA_TESLA_K80').set_gpu_limit(2)

    last_watched_anime = load_big_query_data(query = user_last_anime_watched_query())
    last_watched_anime.set_display_name("last watched anime data")
    
    infer_retrieval = infer_anime_anime_retrieval(model_path = train_retrieval_model.outputs['output_model_path'],
                                                  input_data_path = last_watched_anime.outputs['output_csv'],
                                                 )
    infer_retrieval.set_display_name("inference retrieval model")
    
    unknown_retrieved = load_big_query_external_data(
        external_table_uri = infer_retrieval.outputs['output_data_path'],
        external_table_schema = [
            ("user_id", "STRING"), 
            ("anime_id", "STRING"), 
            ("retrieved_anime_id", "STRING")
        ],
        external_table_id = 'unknown_retrieved',
        query = user_last_anime_retrieved_animes_query('unknown_retrieved')
    )
    unknown_retrieved.set_display_name("remove known retrieved animes")
    
    return unknown_retrieved

def pair_ranking_model_steps(all_anime_data, unknown_retrieved):
    
    train_pair_ranking_data = load_big_query_data(query = anime_anime_pair_ranking_query('TRAIN'))
    train_pair_ranking_data.set_display_name("train data : anime anime pair ranking")
    
    val_pair_ranking_data = load_big_query_data(query = anime_anime_pair_ranking_query('VAL'))
    val_pair_ranking_data.set_display_name("validation data : anime anime pair ranking")
    
    test_pair_ranking_data = load_big_query_data(query = anime_anime_pair_ranking_query('TEST'))
    test_pair_ranking_data.set_display_name("test data : anime anime pair ranking")
    
    
    train_ranking_model = train_anime_anime_ranking(train_data_path = train_pair_ranking_data.outputs['output_csv'], 
                                                    val_data_path = val_pair_ranking_data.outputs['output_csv'],
                                                    test_data_path = test_pair_ranking_data.outputs['output_csv'],
                                                    all_anime_data_path = all_anime_data.outputs['output_csv'],
                                                    anime_embedding_size = 256,
                                                    scoring_layer_size = 256,
                                                    learning_rate = 0.05,
                                                    optimizer = 'adam',
                                                    max_num_epochs = 30,
                                                    early_stop_num_epochs = 5
                                                    )
    train_ranking_model.set_display_name("train ranking model")
    train_ranking_model.set_cpu_limit('8').set_memory_limit('32G')
    # Quotas !
    # train_ranking_model.add_node_selector_constraint('cloud.google.com/gke-accelerator', 'NVIDIA_TESLA_K80').set_gpu_limit(1)

    infer_ranking = infer_anime_anime_ranking(model_path = train_ranking_model.outputs['output_model_path'],
                                              input_data_path = unknown_retrieved.outputs['output_csv']
    )
    infer_ranking.set_display_name("inference ranking model")
    return infer_ranking

@dsl.pipeline(
    name="anime-anime-recommendation-pipeline"
)
def anime_anime_recommendation_pipeline(run_retrieval: str = 'false'):
    
    all_anime_data = load_big_query_data(query = anime_list_query())
    all_anime_data.set_display_name("all anime data")
    
    with dsl.Condition(run_retrieval=='true'):
        unknown_retrieved = retrieval_model_steps(all_anime_data)
        _ = pair_ranking_model_steps(all_anime_data, unknown_retrieved)
    
    with dsl.Condition(run_retrieval=='false'):
        unknown_retrieved = load_big_query_data(query = user_all_possible_animes_query())
        unknown_retrieved.set_display_name("all anime not interacted with data")
        _ = pair_ranking_model_steps(all_anime_data, unknown_retrieved)

if __name__ == '__main__':
    
    package_path = os.path.abspath(__file__ + "/../anime_anime_recommendation_pipeline.json")
    
    kfp.v2.compiler.Compiler().compile(
        pipeline_func=anime_anime_recommendation_pipeline,
        package_path=package_path
    )
    api_client = AIPlatformClient(
        project_id='anime-rec-dev',
        region='us-central1',
    )
    rresponse = api_client.create_run_from_job_spec(
        job_spec_path=package_path,
        pipeline_root='gs://anime-rec-dev-ml-pipelines/user-anime-rec-pipeline',
        enable_caching=False,
        parameter_values={
            'run_retrieval' : 'true'
        }
    )

    response = api_client.create_run_from_job_spec(
        job_spec_path=package_path,
        pipeline_root='gs://anime-rec-dev-ml-pipelines/user-anime-rec-pipeline',
        enable_caching=False,
        parameter_values={
            'run_retrieval' : 'false'
        }
    )

