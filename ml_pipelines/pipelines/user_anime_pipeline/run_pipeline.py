import os
import sys
import kfp
from kfp.v2 import dsl
from kfp.v2.dsl import component

from kfp.v2.google.client import AIPlatformClient

sys.path.append(os.path.abspath(__file__ + "/../../../"))
print(os.path.abspath(__file__ + "/../../../"))

from components.bq_components import load_big_query_data, load_big_query_external_data

from utils.bq_queries.user_anime_data_queries import sample_user_anime_retrieval_query as user_anime_retrieval_query
from utils.bq_queries.user_anime_data_queries import sample_user_anime_list_ranking_query as user_anime_list_ranking_query
from utils.bq_queries.user_anime_data_queries import sample_all_anime_query as all_anime_query
from utils.bq_queries.user_anime_data_queries import sample_all_user_query as all_user_query
from utils.bq_queries.user_anime_data_queries import user_retrieved_animes


train_user_anime_retrieval = kfp.components.load_component_from_file(
    os.path.join(os.path.abspath(__file__ + "/../../../"), "components/user_anime/train_retrieval/component.yaml")
)
infer_user_anime_retrieval = kfp.components.load_component_from_file(
    os.path.join(os.path.abspath(__file__ + "/../../../"), "components/user_anime/infer_retrieval/component.yaml")
)
train_user_anime_ranking = kfp.components.load_component_from_file(
    os.path.join(os.path.abspath(__file__ + "/../../../"), "components/user_anime/train_ranking/component.yaml")
)
infer_user_anime_ranking = kfp.components.load_component_from_file(
    os.path.join(os.path.abspath(__file__ + "/../../../"), "components/user_anime/infer_ranking/component.yaml")
)

@dsl.pipeline(
    name="user-anime-recommendation-pipeline"
)
def user_anime_recommendation_pipeline():
    
    train_retrieval_data = load_big_query_data(query = user_anime_retrieval_query('TRAIN'))
    train_retrieval_data.set_display_name("train data : user anime retrieval")
    
    val_retrieval_data = load_big_query_data(query = user_anime_retrieval_query('VAL'))
    val_retrieval_data.set_display_name("val data : user anime retrieval")
    
    test_retrieval_data = load_big_query_data(query = user_anime_retrieval_query('TEST'))
    test_retrieval_data.set_display_name("test data : user anime retrieval")
    
    train_ranking_data = load_big_query_data(query = user_anime_list_ranking_query('TRAIN'))
    train_ranking_data.set_display_name("train data : user anime list ranking")
    
    val_ranking_data = load_big_query_data(query = user_anime_list_ranking_query('VAL'))
    val_ranking_data.set_display_name("validation data : user anime list ranking")
    
    test_ranking_data = load_big_query_data(query = user_anime_list_ranking_query('TEST'))
    test_ranking_data.set_display_name("test data : user anime list ranking")
    
    all_anime_data = load_big_query_data(query = all_anime_query())
    all_anime_data.set_display_name("all anime data")
    
    all_user_data = load_big_query_data(query = all_user_query())
    all_user_data.set_display_name("all user data")
    
    
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
        query = user_retrieved_animes('unknown_retrieved')
    )
    unknown_retrieved.set_display_name("remove known retrieved animes")
    
    
    
    train_ranking_model = train_user_anime_ranking(
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
    
    infer_ranking_model = infer_user_anime_ranking(
        model_path = train_ranking_model.outputs['output_model_path'],
        input_data_path = unknown_retrieved.outputs['output_csv']
    )
    infer_ranking_model.set_display_name("infer ranking model")


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
        pipeline_root='gs://anime-rec-dev-ml-pipelines/anime-anime-rec-pipeline'
    )
    
