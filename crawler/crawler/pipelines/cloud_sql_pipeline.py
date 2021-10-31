import os

from crawler.items.scheduler_items.anime_item import AnimeSchedulerItem
from crawler.items.scheduler_items.profile_item import ProfileSchedulerItem

from google.cloud.sql.connector import connector

class CloudSQLPipeline:
    def __init__(self):
        self.db_conn = None
        self.cursor = None

    def create_tables(self):
        self.cursor.execute(
            """
                CREATE TABLE IF NOT EXISTS anime_schedule (
                    url VARCHAR(255) PRIMARY KEY NOT NULL,
                    end_date TIMESTAMP,
                    watching_count INT,
                    last_crawl_date TIMESTAMP,
                    last_inspect_date TIMESTAMP
                )
            """
        )
        self.db_conn.commit()
        
        self.cursor.execute(
            """
                CREATE TABLE IF NOT EXISTS profile_schedule (
                    url VARCHAR(255) PRIMARY KEY NOT NULL,
                    last_online_date TIMESTAMP,
                    last_crawl_date TIMESTAMP,
                    last_inspect_date TIMESTAMP
                )
            """
        )
        self.db_conn.commit()
    
    def drop_tables(self):
        self.cursor.execute(
            """
                DROP TABLE IF EXISTS anime_schedule
            """
        )
        self.cursor.execute(
            """
                DROP TABLE IF EXISTS profile_schedule 
            """
        )
        self.db_conn.commit()

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
                                CASE WHEN last_crawl_date > profile_schedule.last_crawl_date THEN last_crawl_date
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
        self.cursor.execute(sql_query)
        self.db_conn.commit()
    
    def open_spider(self, spider):

        self.db_conn = connector.connect(
            os.getenv("SCHEDULER_DB_INSTANCE"),
            "pg8000",
            user=os.getenv("SCHEDULER_DB_USER"),
            password=os.getenv("SCHEDULER_DB_PASSWORD"),
            db=os.getenv("SCHEDULER_DB"),
            port='5432'
        )
        self.cursor = self.db_conn.cursor()
        self.create_tables()
        self.cursor.execute("SELECT * from anime_schedule")
        result = self.cursor.fetchall()
        for row in result:
            print(row)

    def process_item(self, item, spider):
        if isinstance(item, AnimeSchedulerItem):
            self.insert_anime(item)
        
        if isinstance(item, ProfileSchedulerItem):
            self.insert_profile(item)
        return item


    
