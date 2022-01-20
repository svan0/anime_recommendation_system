import unittest

from crawler.spiders.anime_spider import AnimeSpider

from tests.utils import fake_html_response_from_file

class ReviewTest(unittest.TestCase):

    def setUp(self):
        self.spider = AnimeSpider()
    
    def test_empty(self):
        
        file_path = 'fake_responses/anime/review_page/Chainsaw Man - Reviews - MyAnimeList.net.html'
        url = 'https://myanimelist.net/anime/44511/Chainsaw_Man/reviews?p=1'

        expected_results = []
        
        result = []
        response = fake_html_response_from_file(file_path, url)
        for review in self.spider.parse_review_page_for_reviews(response, local_file_response = True):
            del review['crawl_date']
            result.append(dict(review))
        
        self.assertCountEqual(
            result,
            expected_results
        )

    def test(self):

        expected_results = [
            {
                'url': 'https://myanimelist.net/reviews.php?id=22681', 
                'uid': '22681', 
                'anime_id': '5114', 
                'user_id': 'tazillo', 
                'review_date': '2010-01-25T00:00:00', 
                'num_useful': 3870, 
                'overall_score': 10, 
                'story_score': 10, 
                'animation_score': 9, 
                'sound_score': 9, 
                'character_score': 10, 
                'enjoyment_score': 10
            }, 
            {
                'url': 'https://myanimelist.net/reviews.php?id=185058', 
                'uid': '185058', 
                'anime_id': '5114', 
                'user_id': 'ChrissyKay', 
                'review_date': '2015-04-07T00:00:00', 
                'num_useful': 1562, 
                'overall_score': 7, 
                'story_score': 8, 
                'animation_score': 8, 
                'sound_score': 10, 
                'character_score': 6, 
                'enjoyment_score': 7
            }, 
            {
                'url': 'https://myanimelist.net/reviews.php?id=32075', 
                'uid': '32075', 
                'anime_id': '5114', 
                'user_id': 'Archaeon', 
                'review_date': '2010-11-15T00:00:00', 
                'num_useful': 1406, 
                'overall_score': 9, 
                'story_score': 8, 
                'animation_score': 9, 
                'sound_score': 9, 
                'character_score': 9, 
                'enjoyment_score': 9
            }, 
            {
                'url': 'https://myanimelist.net/reviews.php?id=186465', 
                'uid': '186465', 
                'anime_id': '5114', 
                'user_id': 'literaturenerd', 
                'review_date': '2015-04-25T00:00:00', 
                'num_useful': 901, 
                'overall_score': 7, 
                'story_score': 8, 
                'animation_score': 8, 
                'sound_score': 8, 
                'character_score': 8, 
                'enjoyment_score': 8
            }, 
            {
                'url': 'https://myanimelist.net/reviews.php?id=194638', 
                'uid': '194638', 
                'anime_id': '5114', 
                'user_id': 'M0nkeyD_Luffy', 
                'review_date': '2015-07-29T00:00:00', 
                'num_useful': 509, 
                'overall_score': 5, 
                'story_score': 2, 
                'animation_score': 7, 
                'sound_score': 7, 
                'character_score': 8, 
                'enjoyment_score': 5
            }, 
            {
                'url': 'https://myanimelist.net/reviews.php?id=32513', 
                'uid': '32513', 
                'anime_id': '5114', 
                'user_id': 'BiddingGortonio', 
                'review_date': '2010-12-04T00:00:00', 
                'num_useful': 433, 
                'overall_score': 3, 
                'story_score': 2, 
                'animation_score': 7, 
                'sound_score': 7, 
                'character_score': 1, 
                'enjoyment_score': 5
            }, 
            {
                'url': 'https://myanimelist.net/reviews.php?id=18750', 
                'uid': '18750', 
                'anime_id': '5114', 
                'user_id': 'bakababe', 
                'review_date': '2009-09-13T00:00:00', 
                'num_useful': 414, 
                'overall_score': 10, 
                'story_score': 10, 
                'animation_score': 10, 
                'sound_score': 9, 
                'character_score': 10, 
                'enjoyment_score': 10
            }, 
            {
                'url': 'https://myanimelist.net/reviews.php?id=23664', 
                'uid': '23664', 
                'anime_id': '5114', 
                'user_id': 'ryuu_zer0', 
                'review_date': '2010-03-02T00:00:00', 
                'num_useful': 265, 
                'overall_score': 9, 
                'story_score': 10, 
                'animation_score': 9, 
                'sound_score': 8, 
                'character_score': 10, 
                'enjoyment_score': 10
            }, 
            {
                'url': 'https://myanimelist.net/reviews.php?id=166473', 
                'uid': '166473', 
                'anime_id': '5114', 
                'user_id': 'Reimei-Chan', 
                'review_date': '2014-10-27T00:00:00', 
                'num_useful': 243, 
                'overall_score': 10, 
                'story_score': 10, 
                'animation_score': 10, 
                'sound_score': 10, 
                'character_score': 10, 
                'enjoyment_score': 10
            }, 
            {
                'url': 'https://myanimelist.net/reviews.php?id=19530', 
                'uid': '19530', 
                'anime_id': '5114', 
                'user_id': 'Rurouni_Tidus', 
                'review_date': '2009-10-09T00:00:00', 
                'num_useful': 205, 
                'overall_score': 10, 
                'story_score': 10, 
                'animation_score': 10, 
                'sound_score': 8, 
                'character_score': 9, 
                'enjoyment_score': 10
            }, 
            {
                'url': 'https://myanimelist.net/reviews.php?id=185466', 
                'uid': '185466', 
                'anime_id': '5114', 
                'user_id': 'Malighos', 
                'review_date': '2015-04-12T00:00:00', 
                'num_useful': 194, 
                'overall_score': 10, 
                'story_score': 10, 
                'animation_score': 10, 
                'sound_score': 10, 
                'character_score': 10, 
                'enjoyment_score': 10
            }, 
            {
                'url': 'https://myanimelist.net/reviews.php?id=360436', 
                'uid': '360436', 
                'anime_id': '5114', 
                'user_id': 'GoodAnime_101', 
                'review_date': '2020-10-17T00:00:00', 
                'num_useful': 70, 
                'overall_score': 6, 
                'story_score': 6, 
                'animation_score': 7, 
                'sound_score': 6, 
                'character_score': 5, 
                'enjoyment_score': 5
            }, 
            {
                'url': 'https://myanimelist.net/reviews.php?id=185825', 
                'uid': '185825', 
                'anime_id': '5114', 
                'user_id': 'bananapotato', 
                'review_date': '2015-04-17T00:00:00', 
                'num_useful': 158, 
                'overall_score': 10, 
                'story_score': 10, 
                'animation_score': 10, 
                'sound_score': 10, 
                'character_score': 10, 
                'enjoyment_score': 10
            }, 
            {
                'url': 'https://myanimelist.net/reviews.php?id=14348', 
                'uid': '14348', 
                'anime_id': '5114', 
                'user_id': 'Aceofplaces', 
                'review_date': '2009-04-20T00:00:00', 
                'num_useful': 146, 
                'overall_score': 10, 
                'story_score': 9, 
                'animation_score': 10, 
                'sound_score': 10, 
                'character_score': 9, 
                'enjoyment_score': 10
            }, 
            {
                'url': 'https://myanimelist.net/reviews.php?id=166007', 
                'uid': '166007', 
                'anime_id': '5114', 
                'user_id': 'Sahlin', 
                'review_date': '2014-10-24T00:00:00', 
                'num_useful': 141, 
                'overall_score': 10, 
                'story_score': 10, 
                'animation_score': 10, 
                'sound_score': 10, 
                'character_score': 10, 
                'enjoyment_score': 10
            }, 
            {
                'url': 'https://myanimelist.net/reviews.php?id=168119', 
                'uid': '168119', 
                'anime_id': '5114', 
                'user_id': 'Fanboy-kun', 
                'review_date': '2014-11-10T00:00:00', 
                'num_useful': 136, 
                'overall_score': 10, 
                'story_score': 10, 
                'animation_score': 10, 
                'sound_score': 10, 
                'character_score': 10, 
                'enjoyment_score': 10
            }, 
            {
                'url': 'https://myanimelist.net/reviews.php?id=167195', 
                'uid': '167195', 
                'anime_id': '5114', 
                'user_id': 'jacobjr1', 
                'review_date': '2014-11-02T00:00:00', 
                'num_useful': 133, 
                'overall_score': 10, 
                'story_score': 10, 
                'animation_score': 10, 
                'sound_score': 10, 
                'character_score': 10, 
                'enjoyment_score': 10
            }, 
            {
                'url': 'https://myanimelist.net/reviews.php?id=170047', 
                'uid': '170047', 
                'anime_id': '5114', 
                'user_id': 'myrah', 
                'review_date': '2014-11-26T00:00:00', 
                'num_useful': 132, 
                'overall_score': 10, 
                'story_score': 10, 
                'animation_score': 10, 
                'sound_score': 10, 
                'character_score': 10, 
                'enjoyment_score': 10
            }, 
            {
                'url': 'https://myanimelist.net/reviews.php?id=157869', 
                'uid': '157869', 
                'anime_id': '5114', 
                'user_id': 'KandaRainbowsoul', 
                'review_date': '2014-08-25T00:00:00', 
                'num_useful': 131, 
                'overall_score': 10, 
                'story_score': 9, 
                'animation_score': 10, 
                'sound_score': 10, 
                'character_score': 10, 
                'enjoyment_score': 10
            }, 
            {
                'url': 'https://myanimelist.net/reviews.php?id=176083', 
                'uid': '176083', 
                'anime_id': '5114', 
                'user_id': 'River_Cloud', 
                'review_date': '2015-01-04T00:00:00', 
                'num_useful': 129, 
                'overall_score': 10, 
                'story_score': 10, 
                'animation_score': 9, 
                'sound_score': 10, 
                'character_score': 10, 
                'enjoyment_score': 10
            }
        ]
            
        file_path = 'fake_responses/anime/review_page/Fullmetal Alchemist_ Brotherhood - Reviews - MyAnimeList.net.html'
        url = 'https://myanimelist.net/anime/5114/Fullmetal_Alchemist__Brotherhood/reviews'

        result = []
        response = fake_html_response_from_file(file_path, url)
        for review in self.spider.parse_review_page_for_reviews(response, local_file_response = True):
            del review['crawl_date']
            result.append(dict(review))
        
        self.assertCountEqual(
            result,
            expected_results
        )