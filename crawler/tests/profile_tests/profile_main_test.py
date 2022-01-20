import unittest

from crawler.spiders.profile_spider import ProfileSpider

from tests.utils import fake_html_response_from_file

from dateutil.relativedelta import relativedelta
import datetime

class ProfileTest(unittest.TestCase):
    
    def setUp(self):
        self.spider = ProfileSpider()
    
    def test_days_ago(self):
        file_path = "fake_responses/profile/main_page/literaturenerd's Profile - MyAnimeList.net.html"
        url = 'https://myanimelist.net/profile/literaturenerd'

        expected_results = {
            'mean_score': 4.67,
            'num_blog_posts': 0,
            'num_days': 246.5,
            'num_forum_posts': 398,
            'num_recommendations': 3,
            'num_reviews': 375,
            'uid': 'literaturenerd',
            'url': 'https://myanimelist.net/profile/literaturenerd'
        }

        response = fake_html_response_from_file(file_path, url)
        result = self.spider.parse_profile_main_page(response)

        self.assertAlmostEqual(
            datetime.datetime.strptime(result['last_online_date'], "%Y-%m-%dT%H:%M:%S"),
            datetime.datetime.now() - relativedelta(hours=12), 
            delta=datetime.timedelta(seconds=10)
        )

        del result['last_online_date']
        self.assertDictEqual(
            dict(result), 
            expected_results
        )
    
    def test_now(self):
        file_path = "fake_responses/profile/main_page/svanO's Profile - MyAnimeList.net.html"
        url = 'https://myanimelist.net/profile/svanO'

        expected_results = {
            'mean_score': 8.75,
            'num_blog_posts': 0,
            'num_days': 95.8,
            'num_forum_posts': 2,
            'num_recommendations': 0,
            'num_reviews': 0,
            'uid': 'svanO',
            'url': 'https://myanimelist.net/profile/svanO'
        }

        response = fake_html_response_from_file(file_path, url)
        result = self.spider.parse_profile_main_page(response)

        self.assertAlmostEqual(
            datetime.datetime.strptime(result['last_online_date'], "%Y-%m-%dT%H:%M:%S"),
            datetime.datetime.now(), 
            delta=datetime.timedelta(seconds=10)
        )

        del result['last_online_date']

        self.assertDictEqual(
            dict(result), 
            expected_results
        )

    def test_date(self):
        file_path = "fake_responses/profile/main_page/tazillo's Profile - MyAnimeList.net.html"
        url = 'https://myanimelist.net/profile/tazillo'

        expected_results = {
            'mean_score': 7.57,
            'num_blog_posts': 1,
            'num_days': 91.1,
            'num_forum_posts': 188,
            'num_recommendations': 2,
            'num_reviews': 19,
            'uid': 'tazillo',
            'url': 'https://myanimelist.net/profile/tazillo'
        }

        response = fake_html_response_from_file(file_path, url)
        result = self.spider.parse_profile_main_page(response)

        self.assertAlmostEqual(
            datetime.datetime.strptime(result['last_online_date'], "%Y-%m-%dT%H:%M:%S"),
            datetime.datetime(2022, 10, 14, 15, 7, 0, 0), 
            delta=datetime.timedelta(seconds=10)
        )

        del result['last_online_date']

        self.assertDictEqual(
            dict(result), 
            expected_results
        )

