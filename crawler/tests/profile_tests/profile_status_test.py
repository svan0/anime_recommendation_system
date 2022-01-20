import unittest

from crawler.spiders.profile_spider import ProfileSpider

from tests.utils import fake_html_response_from_file

class ProfileStatusTest(unittest.TestCase):
    
    def setUp(self):
        self.spider = ProfileSpider()
    
    def test_completed_page_type_1(self):

        file_path = "fake_responses/profile/watch_status_page/tazillo's Completed Anime List - MyAnimeList.net.html"
        url = 'https://myanimelist.net/animelist/tazillo?status=2'

        response = fake_html_response_from_file(file_path, url)
        result = []
        for completed_anime in self.spider.parse_status_page_for_anime_status(response):
            del completed_anime['crawl_date']
            result.append(completed_anime)
        
        self.assertEqual(
            len(result),
            266
        )

        self.assertDictEqual(
            dict(result[0]),
            {
                'anime_id': '50',
                'progress': 24,
                'score': 8,
                'status': 'completed',
                'user_id': 'tazillo'
            }
        )

        self.assertDictEqual(
            dict(result[-1]),
            {
                'anime_id': '23847',
                'progress': 13,
                'score': 8,
                'status': 'completed',
                'user_id': 'tazillo'
            }
        )
    
    def test_completed_page_type_2(self):

        file_path = "fake_responses/profile/watch_status_page/literaturenerd's Completed Anime List - MyAnimeList.net.html"
        url = 'https://myanimelist.net/animelist/literaturenerd?status=2'

        response = fake_html_response_from_file(file_path, url)
        result = []
        for completed_anime in self.spider.parse_status_page_for_anime_status(response):
            del completed_anime['crawl_date']
            result.append(completed_anime)

        self.assertEqual(
            len(result),
            1170
        )

        self.assertDictEqual(
            dict(result[0]),
            {
                'anime_id': '48',
                'progress': 26,
                'score': 6,
                'status': 'completed',
                'user_id': 'literaturenerd'
            }
        )
        self.assertDictEqual(
            dict(result[-1]),
            {
                'anime_id': '764',
                'progress' : 26,
                'score': 6,
                'status': 'completed',
                'user_id': 'literaturenerd'
            }
        )
       
    def test_dropped_page_type_1(self):

        file_path = "fake_responses/profile/watch_status_page/tazillo's Dropped Anime List - MyAnimeList.net.html"
        url = 'https://myanimelist.net/animelist/tazillo?status=4'

        response = fake_html_response_from_file(file_path, url)
        result = []
        for completed_anime in self.spider.parse_status_page_for_anime_status(response):
            del completed_anime['crawl_date']
            result.append(completed_anime)
        
        self.assertEqual(
            len(result),
            13
        )

        self.assertDictEqual(
            dict(result[0]),
            {
                'anime_id': '7647',
                'progress': 9,
                'score': 7,
                'status': 'dropped',
                'user_id': 'tazillo'
            }
        )

        self.assertDictEqual(
            dict(result[-1]),
            {
                'anime_id': '481',
                'progress': 100,
                'score': 5,
                'status': 'dropped',
                'user_id': 'tazillo'
            }
        )
    
    def test_dropped_page_type_2(self):

        file_path = "fake_responses/profile/watch_status_page/literaturenerd's Dropped Anime List - MyAnimeList.net.html"
        url = 'https://myanimelist.net/animelist/literaturenerd?status=4'

        response = fake_html_response_from_file(file_path, url)
        result = []
        for completed_anime in self.spider.parse_status_page_for_anime_status(response):
            del completed_anime['crawl_date']
            result.append(completed_anime)

        self.assertEqual(
            len(result),
            20
        )
        
        self.assertDictEqual(
            dict(result[0]),
            {
                'anime_id': '269',
                'score': 4,
                'progress' : 15,
                'status': 'dropped',
                'user_id': 'literaturenerd'
            }
        )
        self.assertDictEqual(
            dict(result[1]),
            {
                'anime_id': '966',
                'score': 6,
                'status': 'dropped',
                'user_id': 'literaturenerd'
            }
        )
        self.assertDictEqual(
            dict(result[-1]),
            {
                'anime_id': '204',
                'score': 3,
                'progress' : 2,
                'status': 'dropped',
                'user_id': 'literaturenerd'
            }
        )