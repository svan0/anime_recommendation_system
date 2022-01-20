gcloud container clusters create crawler-cluster\
    --zone us-central1-a\
    --machine-type=n1-standard-1\
    --max-nodes=10\
    --min-nodes=1

gcloud container clusters get-credentials crawler-cluster\
    --zone us-central1-a

kubectl create secret generic scheduler-db-instance-credentials\
    --from-file=scheduler_db_instance_credentials.json=/Users/marouenesfargandoura/Downloads/anime-rec-dev-2a0411323a4f.json

kubectl create secret generic scheduler-db-credentials\
    --from-literal=user=postgres\
    --from-literal=password=1123581321\
    --from-literal=dbname=scheduler_db
