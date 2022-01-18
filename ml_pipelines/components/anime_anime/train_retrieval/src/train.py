import tensorflow as tf
import tensorflow_recommenders as tfrs

from models.anime_models import SimpleAnimeModel
from models.anime_anime_retrieval_models import AnimeAnimeRetrievalModel

def get_anime_anime_retrieval_model(
    list_anime, 
    anime_embedding_size,
    learning_rate,
    optimizer
):
    anime_model = SimpleAnimeModel(list_anime, anime_embedding_size)
    anime_model.build(input_shape=(1,))

    retrieval_model = AnimeAnimeRetrievalModel(anime_model, list_anime)

    train_optimizer = tf.keras.optimizers.get(optimizer)
    train_optimizer.learning_rate = learning_rate

    retrieval_model.compile(optimizer = train_optimizer)

    return retrieval_model

def train_anime_anime_retrieval_model(
    list_anime, 
    train_data, 
    validation_data, 
    hyperparameters
):
    model = get_anime_anime_retrieval_model(
        list_anime, 
        hyperparameters.get('anime_embedding_size'),
        hyperparameters.get('learning_rate'),
        hyperparameters.get('optimizer')
    )

    max_num_epochs = hyperparameters.get('max_num_epochs')
    early_stop_num_epochs = hyperparameters.get('early_stop_num_epochs')

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

def get_anime_anime_retrieval_index(list_anime, retrieval_model, k=100):
    
    list_anime = tf.data.Dataset.from_tensor_slices(list_anime)
    retrieval_index = tfrs.layers.factorized_top_k.BruteForce(retrieval_model.anime_model, k=k)
    retrieval_index.index_from_dataset(
        tf.data.Dataset.zip((
            list_anime.batch(100), 
            list_anime.batch(100).map(retrieval_model.anime_model)
        ))
    )
    return retrieval_index
