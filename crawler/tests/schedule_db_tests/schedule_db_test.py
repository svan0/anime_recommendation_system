import unittest
import sqlite3
from dotenv import load_dotenv
load_dotenv()
from crawler.pipelines.scheduler_db_pipeline import SchedulerSQLitePipeline

class ScheduleDBTest(unittest.TestCase):
    
    def setUp(self):
        conn = sqlite3.connect('schedule-db-test.db')
        cursor = conn.cursor()
        
        self.pipeline = SchedulerSQLitePipeline()
        self.pipeline.db_conn = conn
        self.pipeline.cursor = cursor
    
    def tearDown(self):
        self.pipeline.cursor.close()
        self.pipeline.db_conn.close()
    
    def test1(self):
        self.pipeline.drop_tables()
        self.pipeline.create_tables()

        expected_results = [
            ('profile1_url', None, 0, '2021-10-30T22:10:00', 1, '2021-10-29T22:10:00', 1),
            ('profile2_url', None, 0, None, 0, '2021-10-29T22:10:00', 1)
        ]
        
        self.pipeline.update("profile_schedule", 'profile1_url', inspected_date='2021-10-29T22:10:00')
        self.pipeline.update("profile_schedule", 'profile1_url', crawled_date='2021-10-30T22:10:00')
        self.pipeline.update("profile_schedule", 'profile2_url', inspected_date='2021-10-29T22:10:00')
        self.pipeline.db_conn.commit()

        result = []
        for row in self.pipeline.cursor.execute("SELECT * FROM profile_schedule"):
            result.append(row)

        self.assertCountEqual(
            result, 
            expected_results
        )
    
    def test_2(self):

        self.pipeline.drop_tables()
        self.pipeline.create_tables()

        expected_results = [
            ('profile1_url', None, 0, '2021-10-29T22:10:00', 2, '2021-10-29T22:20:00', 1)
        ]
        self.pipeline.update("profile_schedule", 'profile1_url', inspected_date='2021-10-29T22:20:00')
        self.pipeline.update("profile_schedule", 'profile1_url', crawled_date='2021-10-29T22:10:00')
        self.pipeline.update("profile_schedule", 'profile1_url', crawled_date='2021-10-29T22:00:00')
        self.pipeline.db_conn.commit()

        result = []
        for row in self.pipeline.cursor.execute("SELECT * FROM profile_schedule"):
            result.append(row)

        self.assertCountEqual(
            result, 
            expected_results
        )