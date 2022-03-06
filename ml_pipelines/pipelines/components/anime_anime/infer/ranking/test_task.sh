export PYTHONPATH=/Users/marouenesfargandoura/Desktop/personal_projects/anime_recommendation_system/ml_pipelines/anime_rec_pkg

python3 task.py --data-format csv \
    --model-path gs://anime-rec-dev-ml-pipelines/sample_data/anime_anime/ranking/model \
    --input-data-path gs://anime-rec-dev-ml-pipelines/sample_data/anime_anime/ranking/infer \
    --output-data-path gs://anime-rec-dev-ml-pipelines/sample_data/anime_anime/ranking/result