import unittest

from crawler.spiders.profile_spider import ProfileSpider

from tests.utils import fake_html_response_from_file

class FavoritesTest(unittest.TestCase):
    
    def setUp(self):
        self.spider = ProfileSpider()
    
    def test(self):
        file_path = "fake_responses/profile/main_page/literaturenerd's Profile - MyAnimeList.net.html"
        url = 'https://myanimelist.net/profile/literaturenerd'

        expected_results = [
            {'anime_id': '226', 'user_id': 'literaturenerd'}, 
            {'anime_id': '19', 'user_id': 'literaturenerd'}, 
            {'anime_id': '6', 'user_id': 'literaturenerd'}, 
            {'anime_id': '934', 'user_id': 'literaturenerd'}, 
            {'anime_id': '2251', 'user_id': 'literaturenerd'}, 
            {'anime_id': '45', 'user_id': 'literaturenerd'}, 
            {'anime_id': '121', 'user_id': 'literaturenerd'}, 
            {'anime_id': '2966', 'user_id': 'literaturenerd'}, 
            {'anime_id': '1', 'user_id': 'literaturenerd'}, 
            {'anime_id': '759', 'user_id': 'literaturenerd'}
        ]
        response = fake_html_response_from_file(file_path, url)
        result = []
        for favorite in self.spider.parse_profile_main_page_for_favorites(response):
            del favorite['crawl_date']
            result.append(favorite)

        self.assertCountEqual(
            result,
            expected_results
        )