export TAG=$(git log -1 --pretty=%H)
docker build -t crawler_image .
docker tag crawler_image gcr.io/anime-rec-dev/crawler_image:$TAG
docker push gcr.io/anime-rec-dev/crawler_image:$TAG
