import os
import logging
from concurrent import futures
import sqlite3
import threading
from datetime import datetime
import psycopg2

import argparse
from dotenv import load_dotenv

from google.cloud import pubsub
from google.cloud.sql.connector import connector

logging.getLogger().setLevel(logging.INFO)

lock = threading.Lock()
count_published = 0

ANIME_SCHEDULE_TABLE_NAME = 'anime_schedule'
PROFILE_SCHEDULE_TABLE_NAME = 'profile_schedule'

def get_publish_callback(publish_future, url):
    def callback(publish_future):
        global count_published
        try:
            publish_future.result(timeout=60)
            with lock:
                count_published += 1
            logging.debug(f"Published {url}")
        except:
            logging.warning(f"Publishing {url} timedout after 60 seconds")
    return callback

def get_db_connection(type='postgres'):
    if type == 'postgres':
        try:
            return connector.connect(
                os.getenv("SCHEDULER_DB_INSTANCE"),
                "pg8000",
                user=os.getenv("SCHEDULER_DB_USER"),
                password=os.getenv("SCHEDULER_DB_PASSWORD"),
                db=os.getenv("SCHEDULER_DB"),
                port='5432'
            )
        except:
            logging.info("Could not connect to CloudSQL instance with instance name")
            logging.info("Trying to connect with instance host")
            return psycopg2.connect(
                host = os.getenv("SCHEDULER_DB_HOST"),
                user=os.getenv("SCHEDULER_DB_USER"),
                password=os.getenv("SCHEDULER_DB_PASSWORD"),
                port=5432,
                database=os.getenv("SCHEDULER_DB"),
            )
    elif type == 'sqlite':
        return sqlite3.connect(os.getenv("SCHEDULER_DB_FILE"))
    else:
        raise Exception("Type of db connection should be either 'postgres' or 'sqlite'")

def get_scheduled_update_query(table_name, url, scheduled_date):
    url = f"'{url}'"
    scheduled_date = f"'{scheduled_date}'"
    sql_query = f"""
    INSERT INTO {table_name} (
        url, 
        last_scheduled_date,
        scheduled_count,
        last_crawled_date, 
        crawled_count,
        last_inspected_date,
        inspected_count
    )
    VALUES (
        {url},
        {scheduled_date},
        1,
        NULL,
        0,
        NULL,
        0
    )
    ON CONFLICT(url) DO UPDATE
        SET last_scheduled_date = 
                CASE WHEN {table_name}.last_scheduled_date IS NULL THEN excluded.last_scheduled_date
                ELSE (
                    CASE WHEN excluded.last_scheduled_date > {table_name}.last_scheduled_date THEN excluded.last_scheduled_date
                    ELSE {table_name}.last_scheduled_date
                    END
                )
                END,
            scheduled_count = {table_name}.scheduled_count + 1,
            last_crawled_date = {table_name}.last_crawled_date,
            crawled_count = {table_name}.crawled_count,
            last_inspected_date = {table_name}.last_inspected_date,
            inspected_count = {table_name}.inspected_count
    ;       
    """
    return sql_query

def get_postgres_priority_query(table_name, max_num_urls):
    query = f"""
    SELECT url FROM (
        SELECT  url, 
                (scheduled_count = 0) AS never_scheduled,
                ((scheduled_count > crawled_count) AND ((EXTRACT(epoch from age(last_scheduled_date, now())) / 86400)::int < 7)) AS failed_not_long_ago,
                (EXTRACT(epoch from age(last_crawled_date, now())) / 3600)::int  AS num_hours_since_last_crawled,
                (EXTRACT(epoch from age(last_inspected_date, now())) / 3600)::int AS num_hours_since_last_inspected
        FROM {table_name} 
        ORDER BY
            never_scheduled DESC,
            failed_not_long_ago ASC,
            num_hours_since_last_crawled DESC,
            num_hours_since_last_inspected ASC,
            inspected_count DESC
    )
    AS x
    LIMIT {max_num_urls}
    """
    return query

def get_sqlite_priority_query(table_name, max_num_urls):
    query = f"""
    SELECT url FROM (
        SELECT url, 
               (scheduled_count = 0) AS never_scheduled,
               ((scheduled_count > crawled_count) AND (ROUND(JULIANDAY(CURRENT_DATE) - JULIANDAY(last_scheduled_date)) < 7)) AS failed_not_long_ago,
               ROUND((JULIANDAY(CURRENT_DATE) - JULIANDAY(last_crawled_date)) * 24)  AS num_hours_since_last_crawled,
               ROUND((JULIANDAY(CURRENT_DATE) - JULIANDAY(last_inspected_date)) * 24) AS num_hours_since_last_inspected
        FROM {table_name} 
        ORDER BY
            never_scheduled DESC,
            failed_not_long_ago ASC,
            num_hours_since_last_crawled DESC,
            num_hours_since_last_inspected ASC,
            inspected_count DESC
    )
    LIMIT {max_num_urls}
    """
    return query

def get_top_priority(db_conn, table_name, max_num_urls):
    cursor = db_conn.cursor()
    try:
        if type(db_conn) == sqlite3.Connection:
            priority_query = get_sqlite_priority_query(table_name, max_num_urls)
        else:
            priority_query = get_postgres_priority_query(table_name, max_num_urls)

        cursor.execute(priority_query)
        list_urls = cursor.fetchall()
        list_urls = list([x[0] for x in list_urls])
        
        for url in list_urls:
            insert_query = get_scheduled_update_query(table_name, url, datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))
            try:
                cursor.execute(insert_query)
            except:
                db_conn.rollback()
        db_conn.commit()
    
    except Exception as e:
        logging.warning(f"{table_name} error {e}")
        list_urls = []
    finally:
        cursor.close()

    logging.info(f"Fetched from table {table_name} top {len(list_urls)} entries")
    
    return list_urls

def schedule(table_name, max_num_urls, db_type = 'postgres'):
    global count_published
    count_published = 0

    publish_client = pubsub.PublisherClient()
    project = os.getenv('PROJECT_ID')
    if table_name == ANIME_SCHEDULE_TABLE_NAME:
        topic = os.getenv('SCHEDULE_ANIME_PUBSUB_TOPIC')
    else:
        topic = os.getenv('SCHEDULE_PROFILE_PUBSUB_TOPIC')
    
    logging.debug(f"Topic {topic}")

    topic_path = publish_client.topic_path(project, topic)
    pub_futures = []

    db_conn = get_db_connection(db_type)
    try:
        list_urls = get_top_priority(db_conn, table_name, max_num_urls)
        for url in list_urls:
            message = url.encode("utf-8")
            pub_future = publish_client.publish(topic_path, message)
            pub_future.add_done_callback(get_publish_callback(pub_future, url))
            pub_futures.append(pub_future)
        
        futures.wait(pub_futures, return_when=futures.ALL_COMPLETED)
    finally:
        db_conn.close()
    
    logging.info(f"{count_published} {table_name} pushed to PubSub {topic}")

    return f"{count_published} {table_name} pushed to PubSub {topic}"

"""
    Cloud functions
"""
def schedule_anime(request):

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
    
    return schedule(ANIME_SCHEDULE_TABLE_NAME, max_num_urls, 'postgres')

def schedule_profile(request):

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
    
    return schedule(PROFILE_SCHEDULE_TABLE_NAME, max_num_urls, 'postgres')

if __name__ == '__main__':
    
    load_dotenv()

    parser = argparse.ArgumentParser()
    parser.add_argument('--anime', dest='anime', action='store_true')
    parser.add_argument('--profile', dest='profile', action='store_true')
    parser.add_argument('--scheduler_db_type', type=str)
    parser.add_argument('--max_urls', type=int)
    parser.set_defaults(anime = False)
    parser.set_defaults(profile = False)
    parser.set_defaults(scheduler_db_type = 'sqlite')
    parser.set_defaults(max_urls = 100)
    args = parser.parse_args()

    if args.anime:
        schedule(ANIME_SCHEDULE_TABLE_NAME, args.max_urls, db_type=args.scheduler_db_type) 
    if args.profile:
        schedule(PROFILE_SCHEDULE_TABLE_NAME, args.max_urls, db_type=args.scheduler_db_type) 