import os
import logging
import time
from collections import defaultdict

import sqlite3
import psycopg2

from crawler.items.scheduler_items.anime_item import AnimeSchedulerItem
from crawler.items.scheduler_items.profile_item import ProfileSchedulerItem

from crawler.utils import crawler_stats_timing

ANIME_SCHEDULE_TABLE_NAME = 'anime_schedule'
PROFILE_SCHEDULE_TABLE_NAME = 'profile_schedule'

class SchedulerDBPipeline:
    """
        Scrapy Item Pipeline that processes schedule items (not data items)
        by inserting new anime/profile url to the schedule database or updating
        last_inspectted_date/inspected_count or last_crawled_date/crawled_counts
    """
    def __init__(self, db_conn):
        self.db_conn = db_conn
        self.cursor = self.db_conn.cursor()
        
    @staticmethod
    def create_schedule_table_query(table_name):
        query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                url VARCHAR(255) PRIMARY KEY NOT NULL,
                last_scheduled_date TIMESTAMP,
                scheduled_count INT,
                last_crawled_date TIMESTAMP,
                crawled_count INT,
                last_inspected_date TIMESTAMP,
                inspected_count INT
            )
        """
        return query
    
    @staticmethod
    def drop_schedule_table_query(table_name):
        query = f"""
            DROP TABLE IF EXISTS {table_name}
        """
        return query
    
    @staticmethod
    def update_inspected_query(table_name, url, inspected_date):
        url = f"'{url}'"
        inspected_date = f"'{inspected_date}'"
        query = f"""
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
            NULL,
            0,
            NULL,
            0,
            {inspected_date},
            1
        )
        ON CONFLICT(url) DO UPDATE
            SET last_scheduled_date = {table_name}.last_scheduled_date,
                scheduled_count = {table_name}.scheduled_count,
                last_crawled_date = {table_name}.last_crawled_date,
                crawled_count = {table_name}.crawled_count,
                last_inspected_date = 
                    CASE WHEN {table_name}.last_inspected_date IS NULL THEN excluded.last_inspected_date
                    ELSE (
                        CASE WHEN excluded.last_inspected_date > {table_name}.last_inspected_date THEN excluded.last_inspected_date
                        ELSE {table_name}.last_inspected_date
                        END
                    )
                    END,
                inspected_count = {table_name}.inspected_count + 1
        ;       
        """
        return query
    
    @staticmethod
    def update_crawled_query(table_name, url, crawled_date):
        url = f"'{url}'"
        crawled_date = f"'{crawled_date}'"
        query = f"""
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
            NULL,
            0,
            {crawled_date},
            1,
            NULL,
            0
        )
        ON CONFLICT(url) DO UPDATE
            SET last_scheduled_date = {table_name}.last_scheduled_date,
                scheduled_count = {table_name}.scheduled_count,
                last_crawled_date = 
                    CASE WHEN {table_name}.last_crawled_date IS NULL THEN excluded.last_crawled_date
                    ELSE (
                        CASE WHEN excluded.last_crawled_date > {table_name}.last_crawled_date THEN excluded.last_crawled_date
                        ELSE {table_name}.last_crawled_date
                        END
                    )
                    END,
                crawled_count = {table_name}.crawled_count + 1,
                last_inspected_date = {table_name}.last_inspected_date,
                inspected_count = {table_name}.inspected_count
        ;       
        """
        return query
    
    def create_tables(self):
        self.cursor.execute(self.create_schedule_table_query(ANIME_SCHEDULE_TABLE_NAME))
        self.cursor.execute(self.create_schedule_table_query(PROFILE_SCHEDULE_TABLE_NAME))
        self.db_conn.commit()
        logging.debug("create data tables if not exist")
    
    def drop_tables(self):
        self.cursor.execute(self.drop_schedule_table_query(ANIME_SCHEDULE_TABLE_NAME))
        self.cursor.execute(self.drop_schedule_table_query(PROFILE_SCHEDULE_TABLE_NAME))
        self.db_conn.commit()
        logging.debug("drop data tables if exist")
    
    def update(self, table_name, url, inspected_date = None, crawled_date = None):
        
        assert((inspected_date is not None) or (crawled_date is not None))
        assert(not ((inspected_date is not None) and (crawled_date is not None)))
        
        if inspected_date:
            query = self.update_inspected_query(table_name, url, inspected_date)
        else:
            query = self.update_crawled_query(table_name, url, crawled_date)
        try:
            self.cursor.execute(query)
        except:
            self.db_conn.rollback()
        
        logging.debug(f"insert {table_name} {url} for scheduling")

    @crawler_stats_timing
    def process_item(self, item, spider):
        
        if isinstance(item, AnimeSchedulerItem):
            if 'last_crawl_date' in item:
                self.update(ANIME_SCHEDULE_TABLE_NAME, item['url'], crawled_date=item['last_crawl_date'])
                if self.stats:
                    self.stats.inc_value(f'{self.__class__.__name__}_processed_{item.__class__.__name__}_crawled', count = 1)
            else:
                self.update(ANIME_SCHEDULE_TABLE_NAME, item['url'], inspected_date=item['last_inspect_date'])
                if self.stats:
                    self.stats.inc_value(f'{self.__class__.__name__}_processed_{item.__class__.__name__}_inspected', count = 1)
        
        if isinstance(item, ProfileSchedulerItem):
            if 'last_crawl_date' in item:
                self.update(PROFILE_SCHEDULE_TABLE_NAME, item['url'], crawled_date=item['last_crawl_date'])
                if self.stats:
                    self.stats.inc_value(f'{self.__class__.__name__}_processed_{item.__class__.__name__}_crawled', count = 1)
            else:
                self.update(PROFILE_SCHEDULE_TABLE_NAME, item['url'], inspected_date=item['last_inspect_date'])
                if self.stats:
                    self.stats.inc_value(f'{self.__class__.__name__}_processed_{item.__class__.__name__}_inspected', count = 1)

        return item


    def open_spider(self, spider):
        self.create_tables()

    @crawler_stats_timing
    def close_spider(self, spider):
        self.db_conn.commit()
        self.cursor.close()
        self.db_conn.close()

class SchedulerSQLitePipeline(SchedulerDBPipeline):
    """
        SchedulerDBPipeline specific to SQLite
    """
    def __init__(self, stats = None):
        db_conn = sqlite3.connect(os.getenv("SCHEDULER_DB_FILE"))
        super().__init__(db_conn=db_conn)
        self.stats = stats
    
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.stats)

class SchedulerPostgresPipeline(SchedulerDBPipeline):
    """
        SchedulerDBPipeline specific to Postgres
    """
    def __init__(self, stats = None):
        db_conn = psycopg2.connect(
            host = os.getenv("SCHEDULER_DB_HOST"),
            user=os.getenv("SCHEDULER_DB_USER"),
            password=os.getenv("SCHEDULER_DB_PASSWORD"),
            port=5432,
            database=os.getenv("SCHEDULER_DB"),
        )
        super().__init__(db_conn=db_conn)
        self.stats = stats
    
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.stats)
