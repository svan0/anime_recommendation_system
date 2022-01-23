source .env

gcloud container clusters create $CLUSTER_NAME\
    --zone $CLUSTER_ZONE\
    --machine-type=n1-standard-1\
    --max-nodes=10\
    --min-nodes=3

gcloud container clusters get-credentials $CLUSTER_NAME\
    --zone $CLUSTER_ZONE

kubectl create secret generic gcp-credentials\
    --from-file=gcp_credentials.json=$CREDENTIALS_FILE_PATH

kubectl create secret generic scheduler-db-credentials\
    --from-literal=dbinstance=$SCHEDULER_DB_INSTANCE\
    --from-literal=dbhost=$SCHEDULER_DB_HOST\
    --from-literal=user=$SCHEDULER_DB_USER\
    --from-literal=password=$SCHEDULER_DB_PASSWORD\
    --from-literal=dbname=$SCHEDULER_DB
