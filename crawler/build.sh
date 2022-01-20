docker build -t crawler_image .
docker tag crawler_image gcr.io/anime-rec-dev/crawler_image
docker push gcr.io/anime-rec-dev/crawler_image
