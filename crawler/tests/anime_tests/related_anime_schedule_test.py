import unittest

from crawler.spiders.anime_spider import AnimeSpider

from tests.utils import fake_html_response_from_file

class RelatedAnimeScheduleTest(unittest.TestCase):

    def setUp(self):
        self.spider = AnimeSpider()
    
    def test(self):

        expected_results = [
            {'url': 'https://myanimelist.net/anime/121/Fullmetal_Alchemist'}, 
            {'url': 'https://myanimelist.net/anime/6421/Fullmetal_Alchemist__Brotherhood_Specials'}, 
            {'url': 'https://myanimelist.net/anime/9135/Fullmetal_Alchemist__The_Sacred_Star_of_Milos'}, 
            {'url': 'https://myanimelist.net/anime/7902/Fullmetal_Alchemist__Brotherhood_-_4-Koma_Theater'}
        ]

        file_path = 'fake_responses/anime/main_page/Fullmetal Alchemist_ Brotherhood - MyAnimeList.net.html'
        url = 'https://myanimelist.net/anime/5114/Fullmetal_Alchemist__Brotherhood'

        result = []
        response = fake_html_response_from_file(file_path, url)
        for related_anime in self.spider.parse_anime_main_page_for_schedule_anime(response, local_file_response = True):
            del related_anime['last_inspect_date']
            result.append(dict(related_anime))
        
        self.assertCountEqual(
            result,
            expected_results
        )

