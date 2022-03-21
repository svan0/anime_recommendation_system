PROJECT_PATH=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_PATH=$(dirname "$PROJECT_PATH")
PROJECT_PATH=$(dirname "$PROJECT_PATH")

source $PROJECT_PATH/.env

cd $PROJECT_PATH/etl

bq --location us-central1 mk --dataset $PROJECT_ID:landing_area
bq --location us-central1 mk --dataset $PROJECT_ID:staging_area
bq --location us-central1 mk --dataset $PROJECT_ID:processed_area

bq mk --table $PROJECT_ID:landing_area.activity_item landing_area_schemas/activity_item.json
bq mk --table $PROJECT_ID:landing_area.anime_item landing_area_schemas/anime_item.json
bq mk --table $PROJECT_ID:landing_area.favorite_item landing_area_schemas/favorite_item.json
bq mk --table $PROJECT_ID:landing_area.friends_item landing_area_schemas/friends_item.json
bq mk --table $PROJECT_ID:landing_area.profile_item landing_area_schemas/profile_item.json
bq mk --table $PROJECT_ID:landing_area.recommendation_item landing_area_schemas/recommendation_item.json
bq mk --table $PROJECT_ID:landing_area.related_anime_item landing_area_schemas/related_anime_item.json
bq mk --table $PROJECT_ID:landing_area.review_item landing_area_schemas/review_item.json
bq mk --table $PROJECT_ID:landing_area.watch_status_item landing_area_schemas/watch_status_item.json


    