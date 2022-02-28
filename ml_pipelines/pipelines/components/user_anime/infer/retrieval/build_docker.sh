docker build -t user_anime_infer_retrieval .
docker tag user_anime_infer_retrieval gcr.io/anime-rec-dev/user_anime_infer_retrieval
docker push gcr.io/anime-rec-dev/user_anime_infer_retrieval