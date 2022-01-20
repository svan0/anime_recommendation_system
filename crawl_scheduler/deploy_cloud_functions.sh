gcloud functions deploy anime_crawl_scheduler\
    --entry-point=schedule_anime \
    --runtime=python38 \
    --source=. \
    --timeout=300 \
    --env-vars-file=env.yaml \
    --trigger-http

gcloud functions deploy profile_crawl_scheduler\
    --entry-point=schedule_profile \
    --runtime=python38 \
    --source=. \
    --timeout=300 \
    --env-vars-file=env.yaml \
    --trigger-http