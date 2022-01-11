'''
    User Anime Ranking Training module
'''
import tensorflow as tf
import tensorflow_ranking as tfr

from models.user_models import SimpleUserModel
from models.anime_models import SimpleAnimeModel
from models.user_anime_ranking_models import UserAnimeListRankingModel
from models.user_anime_ranking_models import mse_and_list_mle_loss

def get_user_anime_list_ranking_model(
    list_anime,
    list_user,
    anime_embedding_size,
    user_embedding_size,
    scoring_layer_size,
    learning_rate,
    optimizer
):
    '''
        Get User Anime List Ranking model
    '''
    anime_model = SimpleAnimeModel(list_anime, anime_embedding_size)
    user_model = SimpleUserModel(list_user, user_embedding_size)
    ranking_model = UserAnimeListRankingModel(user_model, anime_model, scoring_layer_size, topn=3, positive_threshold=9)

    train_optimizer = tf.keras.optimizers.get(optimizer)
    train_optimizer.learning_rate = learning_rate

    ranking_model.compile(
        loss = mse_and_list_mle_loss,
        optimizer = train_optimizer
    )

    return ranking_model

def train_user_anime_list_ranking_model(list_anime,
                list_user,
                train_data,
                validation_data,
                hyperparameters
):
    '''
        Train User Anime List Ranking model
    '''
    model = get_user_anime_list_ranking_model(
        list_anime,
        list_user,
        hyperparameters.get('anime_embedding_size'),
        hyperparameters.get('user_embedding_size'),
        hyperparameters.get('scoring_layer_size'),
        hyperparameters.get('learning_rate'),
        hyperparameters.get('optimizer')
    )

    early_stop_num_epochs = hyperparameters.get('early_stop_num_epochs')
    max_num_epochs = hyperparameters.get('max_num_epochs')

    early_stop_callback = tf.keras.callbacks.EarlyStopping(
        monitor='val_Precision@3',
        patience=early_stop_num_epochs,
        mode='max',
        restore_best_weights=True
    )

    callbacks = [early_stop_callback]

    model.fit(
        train_data,
        epochs=max_num_epochs,
        validation_data=validation_data,
        callbacks=callbacks,
        verbose=2
    )

    return model
