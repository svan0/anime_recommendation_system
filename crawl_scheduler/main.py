import os
import json

from google.cloud import pubsub
from google.cloud.sql.connector import connector

def get_top_priority_animes(max_num_urls):
    db_conn = connector.connect(
            os.getenv("SCHEDULER_DB_INSTANCE"),
            "pymysql",
            user=os.getenv("SCHEDULER_DB_USER"),
            password=os.getenv("SCHEDULER_DB_PASSWORD"),
            db=os.getenv("SCHEDULER_DB")
    )
    cursor = db_conn.cursor()
    
    list_urls = cursor.execute(
        f"""
            SELECT url 
            FROM anime_schedule 
            ORDER BY (last_crawl_date IS NULL) ASC, 
                      DATEDIFF(last_inspect_date, last_crawl_date) DESC,
                      end_date DESC, 
                      num_watching DESC
            LIMIT {max_num_urls}
        """
    )
    return list_urls

def get_top_priority_profiles(max_num_urls):
    db_conn = connector.connect(
            os.getenv("SCHEDULER_DB_INSTANCE"),
            "pymysql",
            user=os.getenv("SCHEDULER_DB_USER"),
            password=os.getenv("SCHEDULER_DB_PASSWORD"),
            db=os.getenv("SCHEDULER_DB")
    )
    cursor = db_conn.cursor()
    
    list_urls = cursor.execute(
        f"""
            SELECT url 
            FROM profile_schedule 
            ORDER BY (last_crawl_date IS NULL) ASC,
                     DATEDIFF(last_crawl_date, last_inspect_date) DESC,
                     DATEDIFF(last_inspect_date, last_online) DESC 
            LIMIT {max_num_urls}
        """
    )
    return list_urls


def schedule_anime(request):
    publish_client = pubsub.PublisherClient()
    project = os.getenv('PROJECT_ID')
    topic = os.getenv('SCHEDULE_PUBSUB_TOPIC')
    topic_path = publish_client.topic_path(project, topic)

    request_json = request.get_json(silent=True)
    request_args = request.args
    
    if request_json and 'max_num_urls' in request_json:
        max_num_urls = request_json['max_num_urls']
    elif request_args and 'max_num_urls' in request_args:
        max_num_urls = request_args['max_num_urls']
    else:
        max_num_urls = 1000
    
    try:
        max_num_urls = int(max_num_urls)
    except:
        max_num_urls = 1000
    
    list_urls = get_top_priority_animes(1000)
    for anime_url in list_urls:
        message = {
            'type' : 'anime',
            'url' : anime_url
        }
        message = json.dumps(message).encode("utf-8")
        publish_client.publish(topic_path, message)

def schedule_profile(request):
    publish_client = pubsub.PublisherClient()
    project = os.getenv('PROJECT_ID')
    topic = os.getenv('SCHEDULE_PUBSUB_TOPIC')
    topic_path = publish_client.topic_path(project, topic)

    request_json = request.get_json(silent=True)
    request_args = request.args
    
    if request_json and 'max_num_urls' in request_json:
        max_num_urls = request_json['max_num_urls']
    elif request_args and 'max_num_urls' in request_args:
        max_num_urls = request_args['max_num_urls']
    else:
        max_num_urls = 1000
    
    try:
        max_num_urls = int(max_num_urls)
    except:
        max_num_urls = 1000
    
    list_urls = get_top_priority_profiles(max_num_urls)
    for anime_url in list_urls:
        message = {
            'type' : 'anime',
            'url' : anime_url
        }
        message = json.dumps(message).encode("utf-8")
        publish_client.publish(topic_path, message)
