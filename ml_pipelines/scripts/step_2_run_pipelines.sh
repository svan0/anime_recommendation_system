PROJECT_PATH=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_PATH=$(dirname "$PROJECT_PATH")
PROJECT_PATH=$(dirname "$PROJECT_PATH")

export PYTHONPATH=$PROJECT_PATH/ml_pipelines/anime_rec_pkg

cd $PROJECT_PATH/ml_pipelines/pipelines
python3 anime_anime_pipeline.py
python3 user_anime_pipeline.py