import unittest

from crawler.spiders.anime_spider import AnimeSpider

from tests.utils import fake_html_response_from_file

class ReviewProfileScheduleTest(unittest.TestCase):

    def setUp(self):
        self.spider = AnimeSpider()
    
    def test_empty(self):
        
        file_path = 'fake_responses/anime/review_page/Chainsaw Man - Reviews - MyAnimeList.net.html'
        url = 'https://myanimelist.net/anime/44511/Chainsaw_Man/reviews?p=1'

        expected_results = []
        
        result = []
        response = fake_html_response_from_file(file_path, url)
        for review in self.spider.parse_review_page_for_schedule_profiles(response, local_file_response = True):
            del review['last_inspect_date']
            result.append(dict(review))
        
        self.assertCountEqual(
            result,
            expected_results
        )

    def test(self):

        expected_results = [
            {'url': 'https://myanimelist.net/profile/tazillo'}, 
            {'url': 'https://myanimelist.net/profile/ChrissyKay'}, 
            {'url': 'https://myanimelist.net/profile/Archaeon'}, 
            {'url': 'https://myanimelist.net/profile/literaturenerd'}, 
            {'url': 'https://myanimelist.net/profile/M0nkeyD_Luffy'}, 
            {'url': 'https://myanimelist.net/profile/BiddingGortonio'}, 
            {'url': 'https://myanimelist.net/profile/bakababe'}, 
            {'url': 'https://myanimelist.net/profile/ryuu_zer0'}, 
            {'url': 'https://myanimelist.net/profile/Reimei-Chan'}, 
            {'url': 'https://myanimelist.net/profile/Rurouni_Tidus'}, 
            {'url': 'https://myanimelist.net/profile/Malighos'}, 
            {'url': 'https://myanimelist.net/profile/GoodAnime_101'}, 
            {'url': 'https://myanimelist.net/profile/bananapotato'}, 
            {'url': 'https://myanimelist.net/profile/Aceofplaces'}, 
            {'url': 'https://myanimelist.net/profile/Sahlin'}, 
            {'url': 'https://myanimelist.net/profile/Fanboy-kun'}, 
            {'url': 'https://myanimelist.net/profile/jacobjr1'}, 
            {'url': 'https://myanimelist.net/profile/myrah'}, 
            {'url': 'https://myanimelist.net/profile/KandaRainbowsoul'}, 
            {'url': 'https://myanimelist.net/profile/River_Cloud'}
        ]
            
        file_path = 'fake_responses/anime/review_page/Fullmetal Alchemist_ Brotherhood - Reviews - MyAnimeList.net.html'
        url = 'https://myanimelist.net/anime/5114/Fullmetal_Alchemist__Brotherhood/reviews'

        result = []
        response = fake_html_response_from_file(file_path, url)
        for review in self.spider.parse_review_page_for_schedule_profiles(response, local_file_response = True):
            del review['last_inspect_date']
            result.append(dict(review))

        self.assertCountEqual(
            result,
            expected_results
        )