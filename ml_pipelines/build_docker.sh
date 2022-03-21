PROJECT_PATH=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_PATH=$(dirname "$PROJECT_PATH")
PROJECT_PATH=$(dirname "$PROJECT_PATH")

cd $PROJECT_PATH/ml_pipelines

# Base image
docker build -t anime_rec_ml_base_image .
docker tag anime_rec_ml_base_image gcr.io/anime-rec-dev/anime_rec_ml_base_image
docker push gcr.io/anime-rec-dev/anime_rec_ml_base_image


# anime anime train retrieval
cd $PROJECT_PATH/ml_pipelines/pipelines/components/anime_anime/train/retrieval
docker build -t anime_anime_train_retrieval .
docker tag anime_anime_train_retrieval gcr.io/anime-rec-dev/anime_anime_train_retrieval
docker push gcr.io/anime-rec-dev/anime_anime_train_retrieval

# anime anime infer retrieval
cd $PROJECT_PATH/ml_pipelines/pipelines/components/anime_anime/infer/retrieval
docker build -t anime_anime_infer_retrieval .
docker tag anime_anime_infer_retrieval gcr.io/anime-rec-dev/anime_anime_infer_retrieval
docker push gcr.io/anime-rec-dev/anime_anime_infer_retrieval

# anime anime train ranking
cd $PROJECT_PATH/ml_pipelines/pipelines/components/anime_anime/train/ranking
docker build -t anime_anime_train_ranking .
docker tag anime_anime_train_ranking gcr.io/anime-rec-dev/anime_anime_train_ranking
docker push gcr.io/anime-rec-dev/anime_anime_train_ranking

# anime anime infer ranking
cd $PROJECT_PATH/ml_pipelines/pipelines/components/anime_anime/infer/ranking
docker build -t anime_anime_infer_ranking .
docker tag anime_anime_infer_ranking gcr.io/anime-rec-dev/anime_anime_infer_ranking
docker push gcr.io/anime-rec-dev/anime_anime_infer_ranking


# user anime train retrieval
cd $PROJECT_PATH/ml_pipelines/pipelines/components/user_anime/train/retrieval
docker build -t user_anime_train_retrieval .
docker tag user_anime_train_retrieval gcr.io/anime-rec-dev/user_anime_train_retrieval
docker push gcr.io/anime-rec-dev/user_anime_train_retrieval

# user anime infer retrieval
cd $PROJECT_PATH/ml_pipelines/pipelines/components/user_anime/infer/retrieval
docker build -t user_anime_infer_retrieval .
docker tag user_anime_infer_retrieval gcr.io/anime-rec-dev/user_anime_infer_retrieval
docker push gcr.io/anime-rec-dev/user_anime_infer_retrieval

# user anime train ranking
cd $PROJECT_PATH/ml_pipelines/pipelines/components/user_anime/train/ranking
docker build -t user_anime_train_ranking .
docker tag user_anime_train_ranking gcr.io/anime-rec-dev/user_anime_train_ranking
docker push gcr.io/anime-rec-dev/user_anime_train_ranking

# user anime infer ranking
cd $PROJECT_PATH/ml_pipelines/pipelines/components/user_anime/infer/ranking
docker build -t user_anime_infer_ranking .
docker tag user_anime_infer_ranking gcr.io/anime-rec-dev/user_anime_infer_ranking
docker push gcr.io/anime-rec-dev/user_anime_infer_ranking

