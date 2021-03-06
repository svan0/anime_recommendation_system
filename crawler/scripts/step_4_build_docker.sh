PROJECT_PATH=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_PATH=$(dirname "$PROJECT_PATH")
PROJECT_PATH=$(dirname "$PROJECT_PATH")

cd $PROJECT_PATH/crawler

docker build -t crawler_image .
docker tag crawler_image gcr.io/$PROJECT_ID/crawler_image
docker push gcr.io/$PROJECT_ID/crawler_image
