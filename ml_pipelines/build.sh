cd ..
docker build -t ml_image -f ml_pipelines/Dockerfile .
docker tag ml_image gcr.io/anime-rec-dev/ml_image
docker push gcr.io/anime-rec-dev/ml_image
