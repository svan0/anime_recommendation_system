import os
import json
from datetime import datetime

from google.cloud import pubsub

from scrapy.loader import ItemLoader

from crawler.items.scheduler_items.anime_item import AnimeSchedulerItem
from crawler.items.scheduler_items.profile_item import ProfileSchedulerItem

from dotenv import load_dotenv

from google.cloud.sql.connector import connector

class CloudSQLPipeline:
    def __init__(self):
        self.db_conn = None
        self.cursor = None

    def insert_anime(self, item):
        url = f"'{item['url']}'"
        end_date = f"'{item['end_date']}'" if 'end_date' in item else "NULL"
        watching_count = item['watching_count'] if 'watching_count' in item else "NULL"
        last_crawl_date = f"'{item['last_crawl_date']}'" if 'last_crawl_date' in item else "NULL"
        last_inspect_date = f"'{item['last_inspect_date']}'"
        
        sql_query = f"""
            INSERT INTO anime_schedule (
                url, 
                end_date,
                watching_count,
                last_crawl_date, 
                last_inspect_date
            )
            VALUES (
                {url},
                {end_date},
                {watching_count},
                {last_crawl_date},
                {last_inspect_date}
            )
            ON DUPLICATE KEY UPDATE
                end_date = COALESCE({end_date}, watching_count),
                watching_count = COALESCE({watching_count}, watching_count),
                last_crawl_date  = COALESCE({last_crawl_date}, last_inspect_date),
                last_inspect_date = {last_inspect_date}
            ;
        """
        self.cursor.execute(sql_query)
        self.db_conn.commit()
    
    def insert_profile(self, item):
        url = f"'{item['url']}'"
        last_online_date = f"'{item['last_online_date']}'" if 'last_online_date' in item else "NULL"
        last_crawl_date = f"'{item['last_crawl_date']}'" if 'last_crawl_date' in item else "NULL"
        last_inspect_date = f"'{item['last_inspect_date']}'"

        sql_query = f"""
            INSERT INTO profile_schedule 
                (url, last_online_date, last_crawl_date, last_inspect_date)
            VALUES (
                {url},
                {last_online_date},
                {last_crawl_date},
                {last_inspect_date}
            )
            ON DUPLICATE KEY UPDATE
                last_online_date = COALESCE({last_online_date}, last_online_date),
                last_crawl_date = COALESCE({last_crawl_date}, last_crawl_date),
                last_inspect_date = {last_inspect_date}
            ;
        """
        self.cursor.execute(sql_query)
        self.db_conn.commit()
    
    def open_spider(self, spider):

        self.db_conn = connector.connect(
            os.getenv("SCHEDULER_DB_INSTANCE"),
            "pymysql",
            user=os.getenv("SCHEDULER_DB_USER"),
            password=os.getenv("SCHEDULER_DB_PASSWORD"),
            db=os.getenv("SCHEDULER_DB")
        )
        self.cursor = self.db_conn.cursor()
        
        self.cursor.execute(
            """
                CREATE TABLE IF NOT EXISTS anime_schedule (
                    url VARCHAR(255) PRIMARY KEY NOT NULL,
                    end_date DATETIME,
                    watching_count INT,
                    last_crawl_date DATETIME,
                    last_inspect_date DATETIME
                )
            """
        )
        self.cursor.execute(
            """
                CREATE TABLE IF NOT EXISTS profile_schedule (
                    url VARCHAR(255) PRIMARY KEY NOT NULL,
                    last_online_date DATETIME,
                    last_crawl_date DATETIME,
                    last_inspect_date DATETIME
                )
            """
        )
        self.db_conn.commit()

    def process_item(self, item, spider):

        if isinstance(item, AnimeSchedulerItem):
            self.insert_anime(item)
        
        if isinstance(item, ProfileSchedulerItem):
            self.insert_profile(item)
        
        return item


    
