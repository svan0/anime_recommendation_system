import unittest

from crawler.spiders.profile_spider import ProfileSpider

from tests.utils import fake_html_response_from_file

class ProfileStatusAnimeScheduleTest(unittest.TestCase):
    
    def setUp(self):
        self.spider = ProfileSpider()
    
    def test_completed_page_type_1(self):

        file_path = "fake_responses/profile/watch_status_page/tazillo's Completed Anime List - MyAnimeList.net.html"
        url = 'https://myanimelist.net/animelist/tazillo?status=2'

        response = fake_html_response_from_file(file_path, url)
        result = []
        for completed_anime in self.spider.parse_status_page_for_anime_schedule(response):
            del completed_anime['last_inspect_date']
            result.append(completed_anime)
        
        self.assertEqual(
            len(result),
            266
        )

        self.assertDictEqual(
            dict(result[0]),
            {'url': 'https://myanimelist.net/anime/50/Aa_Megami-sama_TV'}
        )

        self.assertDictEqual(
            dict(result[-1]),
            {'url': 'https://myanimelist.net/anime/23847/Yahari_Ore_no_Seishun_Love_Comedy_wa_Machigatteiru_Zoku'}
        )
    
    def test_completed_page_type_2(self):

        file_path = "fake_responses/profile/watch_status_page/literaturenerd's Completed Anime List - MyAnimeList.net.html"
        url = 'https://myanimelist.net/animelist/literaturenerd?status=2'

        response = fake_html_response_from_file(file_path, url)
        result = []
        for completed_anime in self.spider.parse_status_page_for_anime_schedule(response):
            del completed_anime['last_inspect_date']
            result.append(completed_anime)

        self.assertEqual(
            len(result),
            1170
        )

        self.assertDictEqual(
            dict(result[0]),
            {'url': 'https://myanimelist.net/anime/48/hack__Sign'}
        )
        self.assertDictEqual(
            dict(result[-1]),
            {'url': 'https://myanimelist.net/anime/764/Zoids_Shinseiki_Zero'}
        )
       
    def test_dropped_page_type_1(self):

        file_path = "fake_responses/profile/watch_status_page/tazillo's Dropped Anime List - MyAnimeList.net.html"
        url = 'https://myanimelist.net/animelist/tazillo?status=4'

        response = fake_html_response_from_file(file_path, url)
        result = []
        for completed_anime in self.spider.parse_status_page_for_anime_schedule(response):
            del completed_anime['last_inspect_date']
            result.append(completed_anime)
        
        self.assertEqual(
            len(result),
            13
        )

        self.assertDictEqual(
            dict(result[0]),
            {'url': 'https://myanimelist.net/anime/7647/Arakawa_Under_the_Bridge'}
        )

        self.assertDictEqual(
            dict(result[-1]),
            {'url': 'https://myanimelist.net/anime/481/Yu%E2%98%86Gi%E2%98%86Oh_Duel_Monsters'}
        )
    
    def test_dropped_page_type_2(self):

        file_path = "fake_responses/profile/watch_status_page/literaturenerd's Dropped Anime List - MyAnimeList.net.html"
        url = 'https://myanimelist.net/animelist/literaturenerd?status=4'

        response = fake_html_response_from_file(file_path, url)
        result = []
        for completed_anime in self.spider.parse_status_page_for_anime_schedule(response):
            del completed_anime['last_inspect_date']
            result.append(completed_anime)

        self.assertEqual(
            len(result),
            20
        )

        self.assertDictEqual(
            dict(result[0]),
            {'url': 'https://myanimelist.net/anime/269/Bleach'}
        )
        self.assertDictEqual(
            dict(result[-1]),
            {'url': 'https://myanimelist.net/anime/204/Yumeria'}
        )