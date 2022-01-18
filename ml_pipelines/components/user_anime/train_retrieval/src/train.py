import tensorflow as tf
import tensorflow_recommenders as tfrs

from models.anime_models import SimpleAnimeModel
from models.user_models import SimpleUserModel
from models.user_anime_retrieval_models import UserAnimeRetrievalModel

def get_user_anime_retrieval_model(
    list_anime,
    list_user,
    user_anime_embedding_size,
    learning_rate,
    optimizer
):
    anime_model = SimpleAnimeModel(list_anime, user_anime_embedding_size)
    anime_model.build(input_shape=(1,))

    user_model = SimpleUserModel(list_user, user_anime_embedding_size)
    user_model.build(input_shape=(1,))

    retrieval_model = UserAnimeRetrievalModel(user_model, anime_model, list_anime)

    train_optimizer = tf.keras.optimizers.get(optimizer)
    train_optimizer.learning_rate = learning_rate

    retrieval_model.compile(optimizer = train_optimizer)

    return retrieval_model

def train_user_anime_retrieval_model(
    list_anime,
    list_user,
    train_data,
    validation_data,
    hyperparameters
):
    model = get_user_anime_retrieval_model(
        list_anime,
        list_user,
        hyperparameters.get('user_anime_embedding_size'),
        hyperparameters.get('learning_rate'),
        hyperparameters.get('optimizer')
    )

    early_stop_num_epochs = hyperparameters.get('early_stop_num_epochs')
    max_num_epochs = hyperparameters.get('max_num_epochs')

    early_stop_callback = tf.keras.callbacks.EarlyStopping(
        monitor='val_factorized_top_k/top_100_categorical_accuracy', 
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

def get_user_anime_retrieval_index(list_anime, retrieval_model, k=100):

    list_anime = tf.data.Dataset.from_tensor_slices(list_anime)
    retrieval_index = tfrs.layers.factorized_top_k.BruteForce(retrieval_model.user_model, k=k)
    retrieval_index.index_from_dataset(
        tf.data.Dataset.zip((
            list_anime.batch(100), 
            list_anime.batch(100).map(retrieval_model.anime_model)
        ))
    )
    return retrieval_index
