docker build -t crawler_image .
docker tag crawler_image gcr.io/anime-rec-dev/crawler_image
docker push gcr.io/anime-rec-dev/crawler_image
kubectl create -f crawler_anime_deployment.yaml
kubectl create -f crawler_profile_deployment.yaml
kubectl create -f crawler_web_deployment.yaml
kubectl create -f crawler_web_service.yaml