'''
    User Anime Ranking Training module
'''
import tensorflow as tf
import tensorflow_ranking as tfr
import tensorflow_recommenders as tfrs

from anime_rec.models.user_models import SimpleUserModel
from anime_rec.models.anime_models import SimpleAnimeModel
from anime_rec.models.user_anime_retrieval_models import UserAnimeRetrievalModel
from anime_rec.models.user_anime_ranking_models import UserAnimeRankingModel
from anime_rec.models.user_anime_ranking_models import UserAnimeListRankingModel

'''
    User Anime Retrieval
'''
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

def evaluate_user_anime_retrieval_model(model, train_dataset, val_dataset, test_dataset):
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
    User Anime Ranking
'''
def get_user_anime_ranking_model(
    list_anime,
    list_user,
    anime_embedding_size,
    user_embedding_size,
    scoring_layer_size,
    learning_rate,
    optimizer
):
    '''
        Get User Anime Rating Prediction model
    '''
    anime_model = SimpleAnimeModel(list_anime, anime_embedding_size)
    user_model = SimpleUserModel(list_user, user_embedding_size)
    ranking_model = UserAnimeRankingModel(user_model, anime_model, scoring_layer_size)

    train_optimizer = tf.keras.optimizers.get(optimizer)
    train_optimizer.learning_rate = learning_rate

    ranking_model.compile(
        loss = tf.keras.losses.MeanSquaredError(),
        optimizer = train_optimizer
    )

    return ranking_model

def train_user_anime_ranking_model(
    list_anime,
    list_user,
    train_data,
    validation_data,
    hyperparameters
):
    '''
        Train User Anime Rating Prediction model
    '''
    model = get_user_anime_ranking_model(
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
        monitor='val_loss',
        patience=early_stop_num_epochs,
        mode='min',
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

def evaluate_user_anime_ranking_model(model, train_dataset, val_dataset, test_dataset):
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
    User Anime List Ranking
'''
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
        loss = tfr.keras.losses.ListMLELoss(),
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
        monitor='val_NDCG',
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

def evaluate_user_anime_list_ranking_model(model, train_dataset, val_dataset, test_dataset):
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

