import unittest

from crawler.spiders.profile_spider import ProfileSpider

from tests.utils import fake_xml_response_from_file

class ActivityTest(unittest.TestCase):
    
    def setUp(self):
        self.spider = ProfileSpider()
    
    def test(self):
        
        file_path = 'fake_responses/profile/activity_page/rss.xml'
        url = 'https://myanimelist.net/rss.php?type=rw&u=tazillo'

        expected_results = [
            {
                'activity_type': 'watching',
                'anime_id': '46095',
                'date': '2021-10-14T14:22:54',
                'user_id': 'tazillo'
            }, 
            {   
                'activity_type': 'completed',
                'anime_id': '40456',
                'date': '2021-06-10T01:44:12',
                'user_id': 'tazillo'
            }, 
            {
                'activity_type': 'completed',
                'anime_id': '42203',
                'date': '2021-04-07T13:39:33',
                'user_id': 'tazillo'
            }, 
            {
                'activity_type': 'completed',
                'anime_id': '35848',
                'date': '2021-01-28T04:59:14',
                'user_id': 'tazillo'
            }, 
            {   
                'activity_type': 'completed',
                'anime_id': '38000',
                'date': '2021-01-28T04:57:27',
                'user_id': 'tazillo'
            }, 
            {
                'activity_type': 'completed',
                'anime_id': '39587',
                'date': '2021-01-21T23:31:28',
                'user_id': 'tazillo'
            }, 
            {
                'activity_type': 'completed',
                'anime_id': '38414',
                'date': '2021-01-20T02:47:55',
                'user_id': 'tazillo'
            }, 
            {
                'activity_type': 'completed',
                'anime_id': '31240',
                'date': '2021-01-20T00:30:41',
                'user_id': 'tazillo'
            }, 
            {
                'activity_type': 'completed',
                'anime_id': '38145',
                'date': '2020-05-08T16:37:22',
                'user_id': 'tazillo'
            }, 
            {
                'activity_type': 'completed',
                'anime_id': '35363',
                'date': '2020-05-08T16:36:53',
                'user_id': 'tazillo'
            }, 
            {
                'activity_type': 'completed',
                'anime_id': '33206',
                'date': '2020-05-08T16:36:30',
                'user_id': 'tazillo'
            }, 
            {
                'activity_type': 'watching',
                'anime_id': '40815',
                'date': '2020-04-22T18:33:56',
                'user_id': 'tazillo'
            }, 
            {
                'activity_type': 'completed',
                'anime_id': '40841',
                'date': '2020-04-22T00:07:55',
                'user_id': 'tazillo'
            }, 
            {
                'activity_type': 'completed',
                'anime_id': '39468',
                'date': '2020-04-21T23:49:19',
                'user_id': 'tazillo'
            }, 
            {
                'activity_type': 'watching',
                'anime_id': '40417',
                'date': '2020-04-20T20:12:45',
                'user_id': 'tazillo'
            }
        ]
        response = fake_xml_response_from_file(file_path, url)
        result = []
        for activity in self.spider.parse_activity_page_for_activity(response):
            del activity['crawl_date']
            result.append(activity)
        
        self.assertCountEqual(
            result,
            expected_results
        )