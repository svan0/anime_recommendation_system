import unittest
import sqlite3
from crawler.pipelines.cloud_sql_pipeline import CloudSQLPipeline

class ScheduleDBProfileTest(unittest.TestCase):
    
    def setUp(self):
        conn = sqlite3.connect('schedule-db-test.db')
        cursor = conn.cursor()
        
        self.pipeline = CloudSQLPipeline()
        self.pipeline.db_conn = conn
        self.pipeline.cursor = cursor
    
    def tearDown(self):
        self.pipeline.cursor.close()
        self.pipeline.db_conn.close()
    
    def test_multiple_profiles_inserted(self):
        self.pipeline.drop_tables()
        self.pipeline.create_tables()

        expected_results = [
            ('profile1_url', '2021-10-28T22:10:00', '2021-10-29T22:10:00', '2021-10-29T22:10:00'),
            ('profile2_url', None, None, '2021-10-29T22:20:00')
        ]
        records_to_insert = [
            {
                'url' : 'profile1_url',
                'last_online_date' : '2021-10-28T22:10:00',
                'last_crawl_date' : '2021-10-29T22:10:00',
                'last_inspect_date' : '2021-10-29T22:10:00'
            },
            {
                'url' : 'profile2_url',
                'last_inspect_date' : '2021-10-29T22:20:00'
            }
        ]
        for profile_schedule in records_to_insert:
            self.pipeline.insert_profile(profile_schedule)
        
        result = []
        for row in self.pipeline.cursor.execute("SELECT * FROM profile_schedule"):
            result.append(row)

        self.assertCountEqual(
            result, 
            expected_results
        )
    
    def test_only_inspect_date_changed(self):

        self.pipeline.drop_tables()
        self.pipeline.create_tables()

        expected_results = [
            ('profile1_url', '2021-10-28T22:10:00', '2021-10-29T22:10:00', '2021-10-29T22:20:00')
        ]
        records_to_insert = [
            {
                'url' : 'profile1_url',
                'last_online_date' : '2021-10-28T22:10:00',
                'last_crawl_date' : '2021-10-29T22:10:00',
                'last_inspect_date' : '2021-10-29T22:10:00'
            },
            {
                'url' : 'profile1_url',
                'last_inspect_date' : '2021-10-29T22:20:00'
            }
        ]
        for profile_schedule in records_to_insert:
            self.pipeline.insert_profile(profile_schedule)
        
        result = []
        for row in self.pipeline.cursor.execute("SELECT * FROM profile_schedule"):
            result.append(row)

        self.assertCountEqual(
            result, 
            expected_results
        )

    def test_insert_profile_order(self):
        
        self.pipeline.drop_tables()
        self.pipeline.create_tables()

        expected_results = [
            ('profile1_url', '2021-10-29T22:20:00', '2021-10-29T22:20:00', '2021-10-29T22:20:00')
        ]
        
        records_to_insert = [
            {
                'url' : 'profile1_url',
                'last_online_date' : '2021-10-29T22:20:00',
                'last_crawl_date' : '2021-10-29T22:20:00',
                'last_inspect_date' : '2021-10-29T22:20:00'
            },
            {
                'url' : 'profile1_url',
                'last_online_date' : '2021-10-29T22:10:00',
                'last_crawl_date' : '2021-10-29T22:10:00',
                'last_inspect_date' : '2021-10-29T22:10:00'
            },
        ]
        for profile_schedule in records_to_insert:
            self.pipeline.insert_profile(profile_schedule)
        
        result = []
        for row in self.pipeline.cursor.execute("SELECT * FROM profile_schedule"):
            result.append(row)
        
        self.assertCountEqual(
            result, 
            expected_results
        )
        


