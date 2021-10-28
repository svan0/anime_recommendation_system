import unittest

from crawler.spiders.anime_spider import AnimeSpider

from tests.utils import get_set_dict, fake_html_response_from_file

class AnimePicsTest(unittest.TestCase):
    
    def setUp(self):
        self.spider = AnimeSpider()
    
    def test(self):

        file_path = 'fake_responses/anime/pics_page/Fullmetal Alchemist_ Brotherhood - Pictures - MyAnimeList.net.html'
        url = 'https://myanimelist.net/anime/5114/Fullmetal_Alchemist__Brotherhood/pics'
        
        expected_results = {
            'pics': {'https://cdn.myanimelist.net/images/anime/13/13738.jpg',
                    'https://cdn.myanimelist.net/images/anime/2/17090.jpg',
                    'https://cdn.myanimelist.net/images/anime/2/17472.jpg',
                    'https://cdn.myanimelist.net/images/anime/5/47603.jpg',
                    'https://cdn.myanimelist.net/images/anime/10/57095.jpg',
                    'https://cdn.myanimelist.net/images/anime/7/74317.jpg',
                    'https://cdn.myanimelist.net/images/anime/1521/94614.jpg',
                    'https://cdn.myanimelist.net/images/anime/1208/94745.jpg',
                    'https://cdn.myanimelist.net/images/anime/1286/96542.jpg',
                    'https://cdn.myanimelist.net/images/anime/1223/96541.jpg',
                    'https://cdn.myanimelist.net/images/anime/1629/108486.jpg'
            }
        }

        response = fake_html_response_from_file(file_path, url)
        result = self.spider.parse_pics_page_for_pics(response, local_file_response = True)
        result = get_set_dict(result)
        
        self.assertDictEqual(
            result,
            expected_results
        )
