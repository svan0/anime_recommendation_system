export PYTHONPATH=/Users/marouenesfargandoura/Desktop/personal_projects/anime_recommendation_system/ml_pipelines/anime_rec_pkg

python3 task.py --data-format csv \
    --train-data-path gs://anime-rec-dev-ml-pipelines/sample_data/anime_anime/retrieval/train \
    --val-data-path gs://anime-rec-dev-ml-pipelines/sample_data/anime_anime/retrieval/val \
    --test-data-path gs://anime-rec-dev-ml-pipelines/sample_data/anime_anime/retrieval/test \
    --anime-data-path gs://anime-rec-dev-ml-pipelines/sample_data/list_anime \
    --model-path gs://anime-rec-dev-ml-pipelines/sample_data/anime_anime/retrieval/model \
    --metrics-path gs://anime-rec-dev-ml-pipelines/sample_data/anime_anime/retrieval/metrics \
    --anime-embedding-size 128 \
    --learning-rate 128 \
    --optimizer adam \
    --max-num-epochs 5 \
    --early-stop-num-epochs 1