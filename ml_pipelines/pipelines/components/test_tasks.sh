COMPONENT_PATH=$PWD

echo "Anime Anime train retrieval"
cd $COMPONENT_PATH/anime_anime/train/retrieval
./test_task.sh

echo "Anime Anime train ranking"
cd $COMPONENT_PATH/anime_anime/train/ranking
./test_task.sh

echo "Anime Anime infer retrieval"
cd $COMPONENT_PATH/anime_anime/infer/retrieval
./test_task.sh

echo "Anime Anime infer ranking"
cd $COMPONENT_PATH/anime_anime/infer/ranking
./test_task.sh


echo "User Anime train retrieval"
cd $COMPONENT_PATH/user_anime/train/retrieval
./test_task.sh

echo "User Anime train ranking"
cd $COMPONENT_PATH/user_anime/train/ranking
./test_task.sh

echo "User Anime infer retrieval"
cd $COMPONENT_PATH/user_anime/infer/retrieval
./test_task.sh

echo "User Anime infer ranking"
cd $COMPONENT_PATH/user_anime/infer/ranking
./test_task.sh