'''
    Train anime anime ranking models
'''
import tensorflow as tf
import tensorflow_recommenders as tfrs

from anime_rec.models.anime_models import SimpleAnimeModel
from anime_rec.models.anime_anime_retrieval_models import AnimeAnimeRetrievalModel
from anime_rec.models.anime_anime_ranking_models import AnimeAnimeScoringModel
from anime_rec.models.anime_anime_ranking_models import AnimeAnimePairClassificationModel

'''
    Anime Anime Retrieval
'''
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

def evaluate_anime_anime_retrieval_model(model, train_dataset, val_dataset, test_dataset):
    def evaluate_model(model, dataset, data_set_type = 'train'):
        metrics = model.evaluate(dataset, return_dict = True)
        list_metrics = []
        for metric in metrics:
            if 'loss' in metric:
                list_metrics.append({
                    'name' : f"{data_set_type}_{metric}",
                    'number_value' : float(metrics[metric]),
                    'format' : "RAW"
                })
            else:
                list_metrics.append({
                    'name' : f"{data_set_type}_{metric}",
                    'number_value' : float(metrics[metric]),
                    'format' : "PERCENTAGE"
                })
        return list_metrics
    
    train_metrics = evaluate_model(model, train_dataset, 'train')
    val_metrics = evaluate_model(model, val_dataset, 'val')
    test_metrics = evaluate_model(model, test_dataset, 'test')
    
    return {
        'metrics' : train_metrics + val_metrics + test_metrics
    }

'''
    Anime Anime Ranking
'''
def get_anime_anime_pair_ranking_classification_model(
        list_anime,
        anime_embedding_size,
        scoring_layer_size,
        learning_rate,
        optimizer
):
    '''
        Creates the anime pair ranking classification model
    '''
    anime_model = SimpleAnimeModel(list_anime, anime_embedding_size)

    anime_pair_scoring_model = AnimeAnimeScoringModel(anime_model, scoring_layer_size)
    anime_anime_pair_classification_model = AnimeAnimePairClassificationModel(anime_pair_scoring_model)
    train_optimizer = tf.keras.optimizers.get(optimizer)
    train_optimizer.learning_rate = learning_rate

    anime_anime_pair_classification_model.compile(
        optimizer=train_optimizer,
        loss='binary_crossentropy',
        metrics=[
            tf.keras.metrics.BinaryAccuracy(),
            tf.keras.metrics.Precision(),
            tf.keras.metrics.Recall(),
            tf.keras.metrics.AUC()
        ]
    )
    return anime_anime_pair_classification_model

def train_anime_anime_pair_ranking_classification_model(
        list_anime,
        train_data,
        validation_data,
        hyperparameters
):
    '''
        Trains the anime pair ranking classification model
    '''
    model = get_anime_anime_pair_ranking_classification_model(
        list_anime,
        hyperparameters.get('anime_embedding_size'),
        hyperparameters.get('scoring_layer_size'),
        hyperparameters.get('learning_rate'),
        hyperparameters.get('optimizer')
    )

    max_num_epochs = hyperparameters.get('max_num_epochs')
    early_stop_num_epochs = hyperparameters.get('early_stop_num_epochs')

    early_stop_callback = tf.keras.callbacks.EarlyStopping(
        monitor='val_binary_accuracy',
        patience=early_stop_num_epochs,
        mode='max'
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

def evaluate_anime_anime_pair_ranking_classification_model(model, train_dataset, val_dataset, test_dataset):
    def evaluate_model(model, dataset, data_set_type = 'train'):
        metrics = model.evaluate(dataset, return_dict = True)
        list_metrics = []
        for metric in metrics:
            if 'loss' in metric:
                list_metrics.append({
                    'name' : f"{data_set_type}_{metric}",
                    'number_value' : float(metrics[metric]),
                    'format' : "RAW"
                })
            else:
                list_metrics.append({
                    'name' : f"{data_set_type}_{metric}",
                    'number_value' : float(metrics[metric]),
                    'format' : "PERCENTAGE"
                })
        return list_metrics
    
    train_metrics = evaluate_model(model, train_dataset, 'train')
    val_metrics = evaluate_model(model, val_dataset, 'val')
    test_metrics = evaluate_model(model, test_dataset, 'test')
    
    return {
        'metrics' : train_metrics + val_metrics + test_metrics
    }
