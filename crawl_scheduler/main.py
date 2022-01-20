import os
import logging
from concurrent import futures
import threading
from datetime import datetime

from dotenv import load_dotenv

from google.cloud import pubsub
from google.cloud.sql.connector import connector

lock = threading.Lock()
count_published = 0

def get_publish_callback(publish_future, url):
    def callback(publish_future):
        global count_published
        try:
            publish_future.result(timeout=60)
            with lock:
                count_published += 1
            print(f"Published {url}")
        except:
            print(f"Publishing {url} timedout after 60 seconds")
    return callback

def get_top_priority_animes(max_num_urls):
    db_conn = connector.connect(
            os.getenv("SCHEDULER_DB_INSTANCE"),
            "pg8000",
            user=os.getenv("SCHEDULER_DB_USER"),
            password=os.getenv("SCHEDULER_DB_PASSWORD"),
            db=os.getenv("SCHEDULER_DB"),
            port='5432'
    )
    cursor = db_conn.cursor()
    try:
        cursor.execute(
            f"""
                SELECT url 
                FROM anime_schedule 
                ORDER BY (last_crawl_date IS NULL) DESC, 
                        last_inspect_date ASC,
                        end_date DESC, 
                        watching_count DESC
                LIMIT {max_num_urls}
            """
        )
        list_urls = cursor.fetchall()
        list_urls = list([x[0] for x in list_urls])
        
        for url in list_urls:

            url_string = f"'{url}'"
            last_inspect_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
            last_inspect_date = f"'{last_inspect_date}'"
            last_crawl_date = last_inspect_date
            end_date = "NULL"
            watching_count = "NULL"

            insert_query = f"""
                INSERT INTO anime_schedule (
                    url, 
                    end_date,
                    watching_count,
                    last_crawl_date, 
                    last_inspect_date
                )
                VALUES (
                    {url_string},
                    {end_date},
                    {watching_count},
                    {last_crawl_date},
                    {last_inspect_date}
                )
                ON CONFLICT(url) DO UPDATE
                    SET end_date = 
                            CASE WHEN anime_schedule.last_crawl_date IS NULL THEN excluded.end_date
                            ELSE (
                                CASE WHEN excluded.last_crawl_date IS NULL THEN anime_schedule.end_date
                                ELSE (
                                    CASE WHEN excluded.last_crawl_date > anime_schedule.last_crawl_date THEN excluded.end_date
                                    ELSE anime_schedule.end_date
                                    END
                                )
                                END
                            )
                            END,
                        watching_count = 
                            CASE WHEN anime_schedule.last_crawl_date IS NULL THEN excluded.watching_count
                            ELSE (
                                CASE WHEN excluded.last_crawl_date IS NULL THEN anime_schedule.watching_count
                                ELSE (
                                    CASE WHEN excluded.last_crawl_date > anime_schedule.last_crawl_date THEN excluded.watching_count
                                    ELSE anime_schedule.watching_count
                                    END
                                )
                                END
                            )
                            END,
                        last_crawl_date  = 
                            CASE WHEN anime_schedule.last_crawl_date IS NULL THEN excluded.last_crawl_date
                            ELSE (
                                CASE WHEN excluded.last_crawl_date IS NULL THEN anime_schedule.last_crawl_date
                                ELSE (
                                    CASE WHEN excluded.last_crawl_date > anime_schedule.last_crawl_date THEN excluded.last_crawl_date
                                    ELSE anime_schedule.last_crawl_date
                                    END
                                )
                                END
                            )
                            END,
                        last_inspect_date = 
                            CASE WHEN anime_schedule.last_inspect_date > excluded.last_inspect_date THEN anime_schedule.last_inspect_date
                            ELSE excluded.last_inspect_date
                            END
                ;
            """
            try:
                cursor.execute(insert_query)
            except:
                db_conn.rollback()
        db_conn.commit()
    except Exception as e:
        print(e)
        list_urls = []
    finally:
        cursor.close()
        db_conn.close()

    print(f"Top {len(list_urls)} animes to crawl fetched from scheduler DB")
    
    return list_urls

def get_top_priority_profiles(max_num_urls):
    db_conn = connector.connect(
            os.getenv("SCHEDULER_DB_INSTANCE"),
            "pg8000",
            user=os.getenv("SCHEDULER_DB_USER"),
            password=os.getenv("SCHEDULER_DB_PASSWORD"),
            db=os.getenv("SCHEDULER_DB"),
            port='5432'
    )
    cursor = db_conn.cursor()
    try:
        cursor.execute(
            f"""
                SELECT url 
                FROM profile_schedule 
                ORDER BY (last_crawl_date IS NULL) DESC,
                        last_inspect_date ASC,
                        last_online_date ASC 
                LIMIT {max_num_urls}
            """
        )
        list_urls = cursor.fetchall()
        list_urls = list([x[0] for x in list_urls])

        for url in list_urls:
            url_string = f"'{url}'"
            last_inspect_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
            last_inspect_date = f"'{last_inspect_date}'"
            last_crawl_date = last_inspect_date
            last_online_date = "NULL"
            
            insert_query = f"""
                INSERT INTO profile_schedule 
                    (url, last_online_date, last_crawl_date, last_inspect_date)
                VALUES (
                    {url_string},
                    {last_online_date},
                    {last_crawl_date},
                    {last_inspect_date}
                )
                ON CONFLICT(url) DO UPDATE
                    SET last_online_date = 
                            CASE WHEN profile_schedule.last_crawl_date IS NULL THEN excluded.last_online_date
                            ELSE (
                                CASE WHEN excluded.last_crawl_date IS NULL THEN profile_schedule.last_online_date
                                ELSE (
                                    CASE WHEN excluded.last_crawl_date > profile_schedule.last_crawl_date THEN excluded.last_online_date
                                    ELSE profile_schedule.last_online_date
                                    END
                                )
                                END
                            )
                            END,
                        last_crawl_date = 
                            CASE WHEN profile_schedule.last_crawl_date IS NULL THEN excluded.last_crawl_date
                            ELSE (
                                CASE WHEN excluded.last_crawl_date IS NULL THEN profile_schedule.last_crawl_date
                                ELSE (
                                    CASE WHEN excluded.last_crawl_date > profile_schedule.last_crawl_date THEN excluded.last_crawl_date
                                    ELSE profile_schedule.last_crawl_date
                                    END
                                )
                                END
                            )
                            END,
                        last_inspect_date = 
                            CASE WHEN profile_schedule.last_inspect_date > excluded.last_inspect_date THEN profile_schedule.last_inspect_date
                            ELSE excluded.last_inspect_date
                            END
                ;
            """
            try:
                cursor.execute(insert_query)
            except:
                db_conn.rollback()
        db_conn.commit()

    except Exception as e:
        print(e)
        list_urls = []
    finally:
        cursor.close()
        db_conn.close()

    print(f"Top {len(list_urls)} profiles to crawl fetched from scheduler DB")

    return list_urls


def schedule_anime(request):
    global count_published
    count_published = 0

    publish_client = pubsub.PublisherClient()
    project = os.getenv('PROJECT_ID')
    topic = os.getenv('SCHEDULE_ANIME_PUBSUB_TOPIC')
    topic_path = publish_client.topic_path(project, topic)
    pub_futures = []

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
    
    print(f"Fetching top {max_num_urls} animes to crawl from scheduler DB")
    list_urls = get_top_priority_animes(max_num_urls)
    print(f"Fetched top {len(list_urls)} animes to crawl from scheduler DB")
    
    for anime_url in list_urls:
        message = anime_url.encode("utf-8")
        pub_future = publish_client.publish(topic_path, message)
        pub_future.add_done_callback(get_publish_callback(pub_future, anime_url))
        pub_futures.append(pub_future)
    
    futures.wait(pub_futures, return_when=futures.ALL_COMPLETED)

    return f"{count_published} animes pushed to PubSub {topic}"

def schedule_profile(request):
    global count_published
    count_published = 0

    publish_client = pubsub.PublisherClient()
    project = os.getenv('PROJECT_ID')
    topic = os.getenv('SCHEDULE_PROFILE_PUBSUB_TOPIC')
    topic_path = publish_client.topic_path(project, topic)
    pub_futures = []

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
    
    print(f"Fetching top {max_num_urls} profiles to crawl from scheduler DB")
    list_urls = get_top_priority_profiles(max_num_urls)
    print(f"Fetched top {len(list_urls)} profiles to crawl from scheduler DB")

    for profile_url in list_urls:
        message = profile_url.encode("utf-8")
        pub_future = publish_client.publish(topic_path, message)
        pub_future.add_done_callback(get_publish_callback(pub_future, profile_url))
        pub_futures.append(pub_future)
    
    futures.wait(pub_futures, return_when=futures.ALL_COMPLETED)
    
    return f"{count_published} profiles pushed to PubSub {topic}"

if __name__ == '__main__':
    load_dotenv()
    get_top_priority_profiles(500)
    get_top_priority_animes(500)