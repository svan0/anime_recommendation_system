import unittest

from crawler.spiders.anime_spider import AnimeSpider

from tests.utils import get_set_dict, fake_html_response_from_file

class AnimeClubsTest(unittest.TestCase):
    
    def setUp(self):
        self.spider = AnimeSpider()
    
    def test(self):

        file_path = 'fake_responses/anime/clubs_page/Chainsaw Man - Clubs - MyAnimeList.net.html'
        url = 'https://myanimelist.net/anime/44511/Chainsaw_Man/clubs'
        
        expected_results = {
            'clubs': {'82156', '8652', '83352', '80480', '80758', '82880', '80393', '81083', '83199', '83805'}
        }

        response = fake_html_response_from_file(file_path, url)
        result = self.spider.parse_clubs_page_for_clubs(response, local_file_response = True)
        result = get_set_dict(result)

        self.assertDictEqual(
            result,
            expected_results
        )
