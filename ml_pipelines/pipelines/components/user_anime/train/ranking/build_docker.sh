docker build -t user_anime_train_ranking .
docker tag user_anime_train_ranking gcr.io/anime-rec-dev/user_anime_train_ranking
docker push gcr.io/anime-rec-dev/user_anime_train_ranking