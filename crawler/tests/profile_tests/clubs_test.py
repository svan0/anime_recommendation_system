import unittest

from crawler.spiders.profile_spider import ProfileSpider

from tests.utils import get_set_dict, fake_html_response_from_file

class ClubsTest(unittest.TestCase):
    
    def setUp(self):
        self.spider = ProfileSpider()
    
    def test(self):
        file_path = "fake_responses/profile/clubs_page/literaturenerd's Profile - Clubs - MyAnimeList.net.html"
        url = 'https://myanimelist.net/profile/literaturenerd/clubs'

        expected_results = {
            'clubs': {'67302', '72667', '76828', '66976', '11097', '74845', '67303', '26244', '73707', '68087', '67199', '17705', '59197', '67029'}
        }
        response = fake_html_response_from_file(file_path, url)
        result = self.spider.parse_clubs_page_for_clubs(response, local_file_response = True)
        result = get_set_dict(result)

        self.assertDictEqual(
            result,
            expected_results
        )