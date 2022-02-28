PROJECT_PATH=pwd
# Base image
docker build -t anime_rec_ml_base_image .
docker tag anime_rec_ml_base_image gcr.io/anime-rec-dev/anime_rec_ml_base_image
docker push gcr.io/anime-rec-dev/anime_rec_ml_base_image

# user anime train retrieval
cd $PROJECT_PATH/pipelines/components/user_anime/train/retrieval
docker build -t user_anime_train_retrieval .
docker tag user_anime_train_retrieval gcr.io/anime-rec-dev/user_anime_train_retrieval
docker push gcr.io/anime-rec-dev/user_anime_train_retrieval

# user anime infer retrieval
cd $PROJECT_PATH/pipelines/components/user_anime/infer/retrieval
docker build -t user_anime_infer_retrieval .
docker tag user_anime_infer_retrieval gcr.io/anime-rec-dev/user_anime_infer_retrieval
docker push gcr.io/anime-rec-dev/user_anime_infer_retrieval

# user anime train ranking
cd $PROJECT_PATH/pipelines/components/user_anime/train/retrieval
docker build -t user_anime_train_retrieval .
docker tag user_anime_train_retrieval gcr.io/anime-rec-dev/user_anime_train_retrieval
docker push gcr.io/anime-rec-dev/user_anime_train_retrieval

# user anime infer ranking
cd $PROJECT_PATH/pipelines/components/user_anime/train/ranking
docker build -t user_anime_train_ranking .
docker tag user_anime_train_ranking gcr.io/anime-rec-dev/user_anime_train_ranking
docker push gcr.io/anime-rec-dev/user_anime_train_ranking

