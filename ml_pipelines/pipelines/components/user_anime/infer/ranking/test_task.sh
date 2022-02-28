export PYTHONPATH=/Users/marouenesfargandoura/Desktop/personal_projects/anime_recommendation_system/ml_pipelines/anime_rec_pkg

python3 task.py --model-type ranking \
    --model-path gs://anime-rec-dev-ml-pipelines/sample_data/user_anime/ranking/model \
    --input-data-path gs://anime-rec-dev-ml-pipelines/sample_data/user_anime/ranking/infer \
    --output-data-path gs://anime-rec-dev-ml-pipelines/sample_data/user_anime/ranking/infer_result

python3 task.py --model-type list_ranking \
    --model-path gs://anime-rec-dev-ml-pipelines/sample_data/user_anime/list_ranking/model \
    --input-data-path gs://anime-rec-dev-ml-pipelines/sample_data/user_anime/list_ranking/infer \
    --output-data-path gs://anime-rec-dev-ml-pipelines/sample_data/user_anime/list_ranking/infer_result