PROJECT_PATH=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_PATH=$(dirname "$PROJECT_PATH")
PROJECT_PATH=$(dirname "$PROJECT_PATH")

source $PROJECT_PATH/.env

cd $PROJECT_PATH/web_app

# Create redis instance
echo "****** Start creating Redis instance ******"
gcloud redis instances create $WEB_APP_REDIS_INSTANCE_ID --size=2 --region=$WEB_APP_REGION
echo "****** Finish creating Redis instance ******"

# Connect to redis and load data (too slow when run from local)
echo "****** Start connecting and loading to Redis instance ******"

# Approach 1 (run script on VM instance)
WEB_APP_REDIS_INSTANCE_HOST=$(gcloud redis instances describe $WEB_APP_REDIS_INSTANCE_ID --region=$WEB_APP_REGION | grep host: | awk '{print $2}')
COMPUTE_NAME="redis-upload-compute"
gcloud compute instances create $COMPUTE_NAME --machine-type=e2-standard-2 --zone=$WEB_APP_REGION-a --scopes=bigquery
sleep 180
gcloud compute scp load_data_to_redis.py $COMPUTE_NAME:~
gcloud compute ssh $COMPUTE_NAME --zone=$WEB_APP_REGION-a -- 'export REDIS_HOST='$WEB_APP_REDIS_INSTANCE_HOST' && sudo apt-get -y install python3-pip && pip3 install redis==4.1.4 google-cloud-bigquery==2.31.0 tqdm==4.63.0 && python3 load_data_to_redis.py'
gcloud compute instances delete $COMPUTE_NAME --quiet

# Approach 2 (run script locally and port forward to Redis instance through VM)
#WEB_APP_REDIS_INSTANCE_HOST=$(gcloud redis instances describe $WEB_APP_REDIS_INSTANCE_ID --region=$WEB_APP_REGION | grep host: | awk '{print $2}')
#TUNNEL_COMPUTE_NAME="redis-compute-tunnel"

#gcloud compute instances create $TUNNEL_COMPUTE_NAME --machine-type=f1-micro --zone=$WEB_APP_REGION-a
#sleep 180

#gcloud compute ssh $TUNNEL_COMPUTE_NAME --zone=$WEB_APP_REGION-a -- -N -L 6379:$WEB_APP_REDIS_INSTANCE_HOST:6379 &
#sleep 180

#python3 load_data_to_redis.py

#gcloud compute instances delete $TUNNEL_COMPUTE_NAME --quiet
echo "****** Finish connecting and loading to Redis instance ******"

# Set up Redis VPC connector
echo "****** Start setting up serverless VPC connector to Redis instance ******"
gcloud compute networks vpc-access connectors create $WEB_APP_REDIS_VPC_CONNECTOR \
    --network default \
    --region $WEB_APP_REGION \
    --range 10.8.0.0/28 \
    --min-instances 2 \
    --max-instances 3 \
    --machine-type f1-micro
echo "****** Finish setting up serverless VPC connector to Redis instance ******"