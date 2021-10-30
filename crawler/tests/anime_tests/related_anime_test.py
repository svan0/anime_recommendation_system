import unittest

from crawler.spiders.anime_spider import AnimeSpider

from tests.utils import fake_html_response_from_file

class RelatedAnimeTest(unittest.TestCase):

    def setUp(self):
        self.spider = AnimeSpider()
    
    def test(self):

        expected_results = [
            {'src_anime': '5114', 'dest_anime': '121'}, 
            {'src_anime': '5114', 'dest_anime': '6421'}, 
            {'src_anime': '5114', 'dest_anime': '9135'}, 
            {'src_anime': '5114', 'dest_anime': '7902'}
        ]

        file_path = 'fake_responses/anime/main_page/Fullmetal Alchemist_ Brotherhood - MyAnimeList.net.html'
        url = 'https://myanimelist.net/anime/5114/Fullmetal_Alchemist__Brotherhood'

        result = []
        response = fake_html_response_from_file(file_path, url)
        for related_anime in self.spider.parse_anime_main_page_for_related_anime(response, local_file_response = True):
            result.append(dict(related_anime))
        
        self.assertCountEqual(
            result,
            expected_results
        )

