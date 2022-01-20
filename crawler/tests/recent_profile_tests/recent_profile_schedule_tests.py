import unittest

from crawler.spiders.recent_profile_spider import RecentProfileSpider

from tests.utils import get_set_dict, fake_html_response_from_file

class TopAnimeScheduleTest(unittest.TestCase):
    
    def setUp(self):
        self.spider = RecentProfileSpider()
    
    def test(self):

        file_path = 'fake_responses/recent_profile/Users - MyAnimeList.net.html'
        url = 'https://myanimelist.net/users.php'
        
        expected_results = [
            {'url': 'https://myanimelist.net/profile/Smoking_Ash'}, 
            {'url': 'https://myanimelist.net/profile/angietrif'}, 
            {'url': 'https://myanimelist.net/profile/Euthaliathos'}, 
            {'url': 'https://myanimelist.net/profile/Kellyss'}, 
            {'url': 'https://myanimelist.net/profile/petersteffanlol'}, 
            {'url': 'https://myanimelist.net/profile/KingLemmy'}, 
            {'url': 'https://myanimelist.net/profile/riot390'}, 
            {'url': 'https://myanimelist.net/profile/Big0r'}, 
            {'url': 'https://myanimelist.net/profile/Nagerley'}, 
            {'url': 'https://myanimelist.net/profile/Cry2075'}, 
            {'url': 'https://myanimelist.net/profile/Quiel_Gammer245'}, 
            {'url': 'https://myanimelist.net/profile/Cosmin_CsMc'}, 
            {'url': 'https://myanimelist.net/profile/henrytrillnguyen'}, 
            {'url': 'https://myanimelist.net/profile/CuchamPrincezny'}, 
            {'url': 'https://myanimelist.net/profile/ppityu'}, 
            {'url': 'https://myanimelist.net/profile/MexicanSphere'}, 
            {'url': 'https://myanimelist.net/profile/Neztrexia'}, 
            {'url': 'https://myanimelist.net/profile/MissLlama'}, 
            {'url': 'https://myanimelist.net/profile/HCSdM'}, 
            {'url': 'https://myanimelist.net/profile/Travin'}
        ]

        result = []
        response = fake_html_response_from_file(file_path, url)
        for recent_profile_schedule in self.spider.parse(response):
            del recent_profile_schedule['last_inspect_date']
            result.append(recent_profile_schedule)

        self.assertCountEqual(
            result,
            expected_results
        )
