import unittest
import sqlite3
from crawler.pipelines.cloud_sql_pipeline import CloudSQLPipeline

class ScheduleDBAnimeTest(unittest.TestCase):
    
    def setUp(self):
        conn = sqlite3.connect('schedule-db-test.db')
        cursor = conn.cursor()
        
        self.pipeline = CloudSQLPipeline()
        self.pipeline.db_conn = conn
        self.pipeline.cursor = cursor
    
    def tearDown(self):
        self.pipeline.cursor.close()
        self.pipeline.db_conn.close()
    
    def test_multiple_animes_inserted(self):
        self.pipeline.drop_tables()
        self.pipeline.create_tables()

        expected_results = [
            ('anime1_url', None, 5, '2021-10-29T22:10:00', '2021-10-29T22:10:00'),
            ('anime2_url', None, None, None, '2021-10-29T22:20:00')
        ]
        records_to_insert = [
            {
                'url' : 'anime1_url',
                'watching_count' : 5,
                'last_crawl_date' : '2021-10-29T22:10:00',
                'last_inspect_date' : '2021-10-29T22:10:00'
            },
            {
                'url' : 'anime2_url',
                'last_inspect_date' : '2021-10-29T22:20:00'
            }
        ]
        for anime_schedule in records_to_insert:
            self.pipeline.insert_anime(anime_schedule)
        
        result = []
        for row in self.pipeline.cursor.execute("SELECT * FROM anime_schedule"):
            result.append(row)

        self.assertCountEqual(
            result, 
            expected_results
        )
    
    def test_only_inspect_date_changed(self):

        self.pipeline.drop_tables()
        self.pipeline.create_tables()

        expected_results = [
            ('anime1_url', None, 5, '2021-10-29T22:10:00', '2021-10-29T22:20:00')
        ]
        records_to_insert = [
            {
                'url' : 'anime1_url',
                'watching_count' : 5,
                'last_crawl_date' : '2021-10-29T22:10:00',
                'last_inspect_date' : '2021-10-29T22:10:00'
            },
            {
                'url' : 'anime1_url',
                'last_inspect_date' : '2021-10-29T22:20:00'
            }
        ]
        for anime_schedule in records_to_insert:
            self.pipeline.insert_anime(anime_schedule)
        
        result = []
        for row in self.pipeline.cursor.execute("SELECT * FROM anime_schedule"):
            result.append(row)

        self.assertCountEqual(
            result, 
            expected_results
        )

    def test_insert_animes_order(self):
        
        self.pipeline.drop_tables()
        self.pipeline.create_tables()

        expected_results = [
            ('anime1_url', None, 6, '2021-10-29T22:20:00', '2021-10-29T22:20:00')
        ]
        
        records_to_insert = [
            {
                'url' : 'anime1_url',
                'watching_count' : 6,
                'last_crawl_date' : '2021-10-29T22:20:00',
                'last_inspect_date' : '2021-10-29T22:20:00'
            },
            {
                'url' : 'anime1_url',
                'watching_count' : 5,
                'last_crawl_date' : '2021-10-29T22:10:00',
                'last_inspect_date' : '2021-10-29T22:10:00'
            },
        ]
        for anime_schedule in records_to_insert:
            self.pipeline.insert_anime(anime_schedule)
        
        result = []
        for row in self.pipeline.cursor.execute("SELECT * FROM anime_schedule"):
            result.append(row)
        
        self.assertCountEqual(
            result, 
            expected_results
        )
        


