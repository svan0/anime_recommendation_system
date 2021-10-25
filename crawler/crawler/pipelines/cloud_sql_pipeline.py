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
        status = f"'{item['status']}'" if 'status' in item else "NULL"
        num_watch = item['num_watch'] if 'num_watch' in item else "NULL"
        num_completed = item['num_completed'] if 'num_completed' in item else "NULL"
        num_dropped = item['num_dropped'] if 'num_dropped' in item else "NULL"
        last_crawl_date = f"'{item['last_crawl_date']}'" if 'last_crawl_date' in item else "NULL"
        last_inspect_date = f"'{item['last_inspect_date']}'"

        sql_query = f"""
            INSERT INTO anime_schedule (
                url, 
                status,
                num_watch,
                num_completed,
                num_dropped,
                last_crawl_date, 
                last_inspect_date
            )
            VALUES (
                {url},
                {status},
                {num_watch},
                {num_completed},
                {num_dropped},
                {last_crawl_date},
                {last_inspect_date}
            )
            ON DUPLICATE KEY UPDATE
                status = COALESCE({status}, status),
                num_watch = COALESCE({num_watch}, num_watch),
                num_completed = COALESCE({num_completed}, num_completed),
                num_dropped = COALESCE({num_dropped}, num_dropped),
                last_crawl_date  = 
                    CASE WHEN last_crawl_date IS NULL THEN {last_crawl_date}
                    ELSE (
                        CASE WHEN {last_crawl_date} IS NULL THEN last_crawl_date
                        ELSE GREATEST({last_crawl_date}, last_crawl_date)
                        END
                    )
                    END,
                last_inspect_date  = 
                    CASE WHEN last_inspect_date IS NULL THEN {last_inspect_date}
                    ELSE (
                        CASE WHEN {last_inspect_date} IS NULL THEN last_inspect_date
                        ELSE GREATEST({last_inspect_date}, last_inspect_date)
                        END 
                    )
                    END
            ;
        """
        self.cursor.execute(sql_query)
        self.db_conn.commit()
    
    def insert_profile(self, item):
        url = f"'{item['url']}'"
        last_crawl_date = f"'{item['last_crawl_date']}'" if 'last_crawl_date' in item else "NULL"
        last_inspect_date = f"'{item['last_inspect_date']}'"

        sql_query = f"""
            INSERT INTO profile_schedule 
                (url, last_crawl_date, last_inspect_date)
            VALUES (
                {url},
                {last_crawl_date},
                {last_inspect_date}
            )
            ON DUPLICATE KEY UPDATE
                last_crawl_date  = 
                    CASE WHEN last_crawl_date IS NULL THEN {last_crawl_date}
                    ELSE (
                        CASE WHEN {last_crawl_date} IS NULL THEN last_crawl_date
                        ELSE GREATEST({last_crawl_date}, last_crawl_date)
                        END
                    )
                    END,
                last_inspect_date  = 
                    CASE WHEN last_inspect_date IS NULL THEN {last_inspect_date}
                    ELSE (
                        CASE WHEN {last_inspect_date} IS NULL THEN last_inspect_date
                        ELSE GREATEST({last_inspect_date}, last_inspect_date)
                        END 
                    )
                    END
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
                    status VARCHAR(255),
                    num_watch INT,
                    num_completed INT,
                    num_dropped INT,
                    last_crawl_date DATETIME,
                    last_inspect_date DATETIME
                )
            """
        )
        self.cursor.execute(
            """
                CREATE TABLE IF NOT EXISTS profile_schedule (
                    url VARCHAR(255) PRIMARY KEY NOT NULL,
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


    
