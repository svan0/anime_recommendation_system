import unittest

from crawler.spiders.anime_spider import AnimeSpider

from tests.utils import fake_html_response_from_file

class RecommendedAnimeScheduleTest(unittest.TestCase):

    def setUp(self):
        self.spider = AnimeSpider()
    
    def test_empty(self):
        expected_results = []
        file_path = 'fake_responses/anime/recommendation_page/Chainsaw Man - Recommendations - MyAnimeList.net.html'
        url = 'https://myanimelist.net/anime/44511/Chainsaw_Man/userrecs'

        result = []
        response = fake_html_response_from_file(file_path, url)
        for recommended_anime in self.spider.parse_recommendation_page_for_schedule_anime(response, local_file_response = True):
            del recommended_anime['last_inspect_date']
            result.append(dict(recommended_anime))
        
        self.assertCountEqual(
            result,
            expected_results
        )
    
    def test(self):

        expected_results = [
            {'url': 'https://myanimelist.net/anime/35073/Overlord_II'}, 
            {'url': 'https://myanimelist.net/anime/37430/Tensei_shitara_Slime_Datta_Ken'}
        ]

        file_path = 'fake_responses/anime/recommendation_page/Mushoku Tensei_ Isekai Ittara Honki Dasu Part 2 - Recommendations - MyAnimeList.net.html'
        url = 'https://myanimelist.net/anime/45576/Mushoku_Tensei__Isekai_Ittara_Honki_Dasu_Part_2/userrecs'

        result = []
        response = fake_html_response_from_file(file_path, url)
        for recommended_anime in self.spider.parse_recommendation_page_for_schedule_anime(response, local_file_response = True):
            del recommended_anime['last_inspect_date']
            result.append(dict(recommended_anime))
        
        self.assertCountEqual(
            result,
            expected_results
        )

