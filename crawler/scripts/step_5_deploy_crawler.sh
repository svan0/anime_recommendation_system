export TAG=$(git log -1 --pretty=%H)
#sed -e "s|TAG|$TAG|g" manifests/crawler_anime_no_db_deployment.yaml | kubectl create -f -
sed -e "s|TAG|$TAG|g" manifests/crawler_profile_no_db_deployment.yaml | kubectl create -f -
#sed -e "s|TAG|$TAG|g" manifests/crawler_anime_deployment.yaml | kubectl create -f -
#sed -e "s|TAG|$TAG|g" manifests/crawler_profile_deployment.yaml | kubectl create -f -
#sed -e "s|TAG|$TAG|g" manifests/crawler_web_deployment.yaml | kubectl create -f -
#kubectl create -f manifests/crawler_web_service.yaml