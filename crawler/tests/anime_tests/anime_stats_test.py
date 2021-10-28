import unittest

from crawler.spiders.anime_spider import AnimeSpider

from tests.utils import fake_html_response_from_file

class AnimeStatsTest(unittest.TestCase):
    
    def setUp(self):
        self.spider = AnimeSpider()
    
    def test_finished_tv(self):

        file_path = 'fake_responses/anime/stats_page/Fullmetal Alchemist_ Brotherhood - Statistics - MyAnimeList.net.html'
        url = 'https://myanimelist.net/anime/5114/Fullmetal_Alchemist__Brotherhood/stats'
        
        expected_results = {
            'completed_count': 1934871,
            'dropped_count': 40365,
            'on_hold_count': 90568,
            'plan_to_watch_count': 395238,
            'score_01_count': 22117,
            'score_02_count': 2484,
            'score_03_count': 1931,
            'score_04_count': 4061,
            'score_05_count': 11590,
            'score_06_count': 24823,
            'score_07_count': 85309,
            'score_08_count': 240332,
            'score_09_count': 477200,
            'score_10_count': 832081,
            'total_count': 2665743,
            'watching_count': 204701
        }

        response = fake_html_response_from_file(file_path, url)
        result = self.spider.parse_stats_page_for_stats(response, local_file_response = True)

        self.assertEqual(
            result,
            expected_results
        )

    def test_airing_tv(self):

        file_path = 'fake_responses/anime/stats_page/Mushoku Tensei_ Isekai Ittara Honki Dasu Part 2 - Statistics - MyAnimeList.net.html'
        url = 'https://myanimelist.net/anime/45576/Mushoku_Tensei__Isekai_Ittara_Honki_Dasu_Part_2/stats'

        expected_results = {
            'completed_count': 61,
            'dropped_count': 591,
            'on_hold_count': 1607,
            'plan_to_watch_count': 133025,
            'score_01_count': 332,
            'score_02_count': 78,
            'score_03_count': 83,
            'score_04_count': 144,
            'score_05_count': 331,
            'score_06_count': 928,
            'score_07_count': 4058,
            'score_08_count': 11910,
            'score_09_count': 13594,
            'score_10_count': 11953,
            'total_count': 303755,
            'watching_count': 168471
        }
        response = fake_html_response_from_file(file_path, url)
        result = self.spider.parse_stats_page_for_stats(response, local_file_response = True)
        
        self.assertEqual(
            result,
            expected_results
        )

    def test_tv_upcoming(self):
        
        file_path = 'fake_responses/anime/stats_page/Chainsaw Man - Statistics - MyAnimeList.net.html'
        url = 'https://myanimelist.net/anime/44511/Chainsaw_Man/stats'
        

        expected_results = {
            'completed_count': 0,
            'dropped_count': 1,
            'on_hold_count': 6,
            'plan_to_watch_count': 265750,
            'score_10_count': 5,
            'total_count': 265760,
            'watching_count': 3    
        }
        response = fake_html_response_from_file(file_path, url)
        result = self.spider.parse_stats_page_for_stats(response, local_file_response = True)

        self.assertEqual(
            result,
            expected_results
        )

    def test_movie(self):

        file_path = 'fake_responses/anime/stats_page/Koe no Katachi (A Silent Voice) - Statistics - MyAnimeList.net.html'
        url = 'https://myanimelist.net/anime/28851/Koe_no_Katachi/stats'
        
        expected_results = {
            'completed_count': 1498557,
            'dropped_count': 2389,
            'on_hold_count': 5066,
            'plan_to_watch_count': 227736,
            'score_01_count': 2555,
            'score_02_count': 933,
            'score_03_count': 1694,
            'score_04_count': 4144,
            'score_05_count': 9692,
            'score_06_count': 26188,
            'score_07_count': 84569,
            'score_08_count': 211530,
            'score_09_count': 386659,
            'score_10_count': 500747,
            'total_count': 1770094,
            'watching_count': 36346
        }

        response = fake_html_response_from_file(file_path, url)
        result = self.spider.parse_stats_page_for_stats(response, local_file_response = True)

        self.assertEqual(
            result,
            expected_results
        )


if __name__ == '__main__':
    unittest.main()
