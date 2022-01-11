'''
    Common data loading helper function
'''
import pandas as pd

def load_list_anime_pd(file_path):
    '''
        Load list of all anime from CSV to pandas DataFrame
    '''
    list_anime = pd.read_csv(file_path)
    list_anime['anime_id'] = list_anime['anime_id'].apply(str)
    return list_anime

def load_list_anime_np(file_path):
    '''
        Load list of all anime from CSV to numpy array
    '''
    list_anime = load_list_anime_pd(file_path)
    list_anime = list_anime['anime_id'].values
    return list_anime

def load_list_user_pd(file_path):
    '''
        Load list of all users from CSV to pandas DataFrame
    '''
    list_user = pd.read_csv(file_path, keep_default_na=False)
    return list_user

def load_list_user_np(file_path):
    '''
        Load list of all users from CSV to numpy array
    '''
    list_user = load_list_user_pd(file_path)
    list_user = list_user['user_id'].values
    return list_user
