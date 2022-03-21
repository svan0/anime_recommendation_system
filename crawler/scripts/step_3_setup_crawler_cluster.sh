PROJECT_PATH=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_PATH=$(dirname "$PROJECT_PATH")
PROJECT_PATH=$(dirname "$PROJECT_PATH")

source $PROJECT_PATH/.env

gcloud container clusters create $CRAWLER_CLUSTER_NAME\
    --zone $CRAWLER_CLUSTER_ZONE\
    --machine-type=n1-standard-1\
    --max-nodes=10\
    --min-nodes=3\
    --spot

gcloud container clusters get-credentials $CRAWLER_CLUSTER_NAME\
    --zone $CRAWLER_CLUSTER_ZONE

kubectl create secret generic gcp-credentials\
    --from-file=gcp_credentials.json=$GOOGLE_APPLICATION_CREDENTIALS

kubectl create secret generic scheduler-db-credentials\
    --from-literal=dbinstance=$SCHEDULER_DB_INSTANCE\
    --from-literal=dbhost=$SCHEDULER_DB_HOST\
    --from-literal=user=$SCHEDULER_DB_USER\
    --from-literal=password=$SCHEDULER_DB_PASSWORD\
    --from-literal=dbname=$SCHEDULER_DB
