import unittest

from crawler.spiders.profile_spider import ProfileSpider

from tests.utils import fake_html_response_from_file

class FavoriteScheduleTest(unittest.TestCase):
    
    def setUp(self):
        self.spider = ProfileSpider()
    
    def test(self):
        file_path = "fake_responses/profile/main_page/literaturenerd's Profile - MyAnimeList.net.html"
        url = 'https://myanimelist.net/profile/literaturenerd'

        expected_results = [
            {'url': 'https://myanimelist.net/anime/226/Elfen_Lied'}, 
            {'url': 'https://myanimelist.net/anime/19/Monster'}, 
            {'url': 'https://myanimelist.net/anime/6/Trigun'}, 
            {'url': 'https://myanimelist.net/anime/934/Higurashi_no_Naku_Koro_ni'}, 
            {'url': 'https://myanimelist.net/anime/2251/Baccano'}, 
            {'url': 'https://myanimelist.net/anime/45/Rurouni_Kenshin__Meiji_Kenkaku_Romantan'}, 
            {'url': 'https://myanimelist.net/anime/121/Fullmetal_Alchemist'}, 
            {'url': 'https://myanimelist.net/anime/2966/Ookami_to_Koushinryou'}, 
            {'url': 'https://myanimelist.net/anime/1/Cowboy_Bebop'}, 
            {'url': 'https://myanimelist.net/anime/759/Tokyo_Godfathers'}
        ]

        response = fake_html_response_from_file(file_path, url)
        result = []
        for favorite in self.spider.parse_profile_main_page_for_favorites_scheduler(response):
            del favorite['last_inspect_date']
            result.append(favorite)

        self.assertCountEqual(
            result,
            expected_results
        )