PROJECT_PATH=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_PATH=$(dirname "$PROJECT_PATH")
PROJECT_PATH=$(dirname "$PROJECT_PATH")

cd $PROJECT_PATH/crawler

kubectl create -f manifests/crawler_anime_deployment.yaml
kubectl create -f manifests/crawler_profile_deployment.yaml
kubectl create -f manifests/crawler_web_deployment.yaml
kubectl create -f manifests/crawler_web_service.yaml