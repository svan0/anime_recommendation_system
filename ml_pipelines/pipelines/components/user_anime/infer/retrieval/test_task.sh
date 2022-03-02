export PYTHONPATH=/Users/marouenesfargandoura/Desktop/personal_projects/anime_recommendation_system/ml_pipelines/anime_rec_pkg

python3 task.py --data-format csv \
    --model-path gs://anime-rec-dev-ml-pipelines/sample_data/user_anime/retrieval/model \
    --input-data-path gs://anime-rec-dev-ml-pipelines/sample_data/user_anime/retrieval/infer \
    --output-data-path gs://anime-rec-dev-ml-pipelines/sample_data/user_anime/retrieval/result
