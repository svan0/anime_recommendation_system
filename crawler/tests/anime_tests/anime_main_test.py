import unittest

from crawler.spiders.anime_spider import AnimeSpider

from tests.utils import get_set_dict, fake_html_response_from_file

class AnimeMainTest(unittest.TestCase):
    
    def setUp(self):
        self.spider = AnimeSpider()
    
    def test_finished_tv(self):

        file_path = 'fake_responses/anime/main_page/Fullmetal Alchemist_ Brotherhood - MyAnimeList.net.html'
        url = 'https://myanimelist.net/anime/5114/Fullmetal_Alchemist__Brotherhood'
        
        expected_results = {
            'end_date': '2010-07-04T00:00:00',
            'favorites_count': 188480,
            'genres': {'Action',
                        'Adventure',
                        'Comedy',
                        'Fantasy',
                        'Drama',
                        'Military',
                        'Shounen'},
            'main_pic': 'https://cdn.myanimelist.net/images/anime/1223/96541.jpg',
            'members_count': 2664818,
            'num_episodes': 64,
            'popularity_rank': 3,
            'score': 9.16,
            'score_count': 1618976,
            'score_rank': 1,
            'season': 'Spring 2009',
            'source_type': 'Manga',
            'start_date': '2009-04-05T00:00:00',
            'status': 'Finished Airing',
            'studios': {'Bones'},
            'synopsis': 'After a horrific alchemy experiment goes wrong in the Elric '
                        'household, brothers Edward and Alphonse are left in a '
                        'catastrophic new reality. Ignoring the alchemical principle '
                        'banning human transmutation, the boys attempted to bring their '
                        'recently deceased mother back to life. Instead, they suffered '
                        "brutal personal loss: Alphonse's body disintegrated while Edward "
                        "lost a leg and then sacrificed an arm to keep Alphonse's soul in "
                        'the physical realm by binding it to a hulking suit of armor.  '
                        'The brothers are rescued by their neighbor Pinako Rockbell and '
                        'her granddaughter Winry. Known as a bio-mechanical engineering '
                        'prodigy, Winry creates prosthetic limbs for Edward by utilizing '
                        '"automail," a tough, versatile metal used in robots and combat '
                        'armor. After years of training, the Elric brothers set off on a '
                        "quest to restore their bodies by locating the Philosopher's "
                        'Stone—a powerful gem that allows an alchemist to defy the '
                        'traditional laws of Equivalent Exchange.  As Edward becomes an '
                        'infamous alchemist and gains the nickname "Fullmetal," the '
                        "boys' journey embroils them in a growing conspiracy that "
                        'threatens the fate of the world.  ',
            'title': 'Fullmetal Alchemist: Brotherhood',
            'type': 'TV',
            'uid': '5114',
            'url': 'https://myanimelist.net/anime/5114/Fullmetal_Alchemist__Brotherhood'
        }

        response = fake_html_response_from_file(file_path, url)
        result = self.spider.parse_anime_main_page_for_info(response, local_file_response = True)
        result = get_set_dict(result)

        self.assertEqual(
            result,
            expected_results
        )
    
    def test_airing_tv(self):

        file_path = 'fake_responses/anime/main_page/Mushoku Tensei_ Isekai Ittara Honki Dasu Part 2 - MyAnimeList.net.html'
        url = 'https://myanimelist.net/anime/45576/Mushoku_Tensei__Isekai_Ittara_Honki_Dasu_Part_2'

        expected_results = {
            'favorites_count': 3916,
            'genres': {'Fantasy', 'Drama', 'Ecchi'},
            'main_pic': 'https://cdn.myanimelist.net/images/anime/1028/117777.jpg',
            'members_count': 303755,
            'num_episodes': 12,
            'popularity_rank': 559,
            'score': 8.61,
            'score_count': 42765,
            'score_rank': 67,
            'season': 'Fall 2021',
            'source_type': 'Light novel',
            'start_date': '2021-10-04T00:00:00',
            'status': 'Currently Airing',
            'studios': {'Studio Bind'},
            'synopsis': 'After the mysterious mana calamity, Rudeus Greyrat and his '
                        'fierce student Eris Boreas Greyrat are teleported to the Demon '
                        'Continent. There, they team up with their newfound companion '
                        "Ruijerd Supardia—the former leader of the Superd's Warrior "
                        'group—to form "Dead End," a successful adventurer party. Making '
                        'a name for themselves, the trio journeys across the continent to '
                        'make their way back home to Fittoa.  Following the advice he '
                        'received from the faceless god Hitogami, Rudeus saves Kishirika '
                        'Kishirisu, the Great Emperor of the Demon World, who rewards him '
                        'by granting him a strange power. Now, as Rudeus masters the '
                        'powerful ability that offers a number of new opportunities, it '
                        'might prove to be more than what he bargained for when '
                        'unexpected dangers threaten to hinder their travels.  ',
            'title': 'Mushoku Tensei: Isekai Ittara Honki Dasu Part 2',
            'type': 'TV',
            'uid': '45576',
            'url': 'https://myanimelist.net/anime/45576/Mushoku_Tensei__Isekai_Ittara_Honki_Dasu_Part_2'
        }

        response = fake_html_response_from_file(file_path, url)
        result = self.spider.parse_anime_main_page_for_info(response, local_file_response = True)
        result = get_set_dict(result)

        self.assertDictEqual(
            result,
            expected_results
        )

    def test_tv_upcoming(self):
        
        file_path = 'fake_responses/anime/main_page/Chainsaw Man - MyAnimeList.net.html'
        url = 'https://myanimelist.net/anime/44511/Chainsaw_Man'

        expected_results = {
            'favorites_count': 5478,
            'genres': {'Action', 'Adventure', 'Demons', 'Shounen'},
            'main_pic': 'https://cdn.myanimelist.net/images/anime/1632/110707.jpg',
            'members_count': 265760,
            'popularity_rank': 678,
            'source_type': 'Manga',
            'status': 'Not yet aired',
            'studios': {'MAPPA'},
            'synopsis': 'Denji has a simple dream—to live a happy and peaceful life, '
                        'spending time with a girl he likes. This is a far cry from '
                        'reality, however, as Denji is forced by the yakuza into killing '
                        'devils in order to pay off his crushing debts. Using his pet '
                        'devil Pochita as a weapon, he is ready to do anything for a bit '
                        'of cash.  Unfortunately, he has outlived his usefulness and is '
                        'murdered by a devil in contract with the yakuza. However, in an '
                        "unexpected turn of events, Pochita merges with Denji's dead body "
                        'and grants him the powers of a chainsaw devil. Now able to '
                        'transform parts of his body into chainsaws, a revived Denji uses '
                        'his new abilities to quickly and brutally dispatch his enemies. '
                        'Catching the eye of the official devil hunters who arrive at the '
                        'scene, he is offered work at the Public Safety Bureau as one of '
                        'them. Now with the means to face even the toughest of enemies, '
                        'Denji will stop at nothing to achieve his simple teenage '
                        'dreams.  ',
            'title': 'Chainsaw Man',
            'type': 'TV',
            'uid': '44511',
            'url': 'https://myanimelist.net/anime/44511/Chainsaw_Man'
        }
        
        response = fake_html_response_from_file(file_path, url)
        result = self.spider.parse_anime_main_page_for_info(response, local_file_response = True)
        result = get_set_dict(result)

        self.assertDictEqual(
            result,
            expected_results
        )

    def test_movie(self):

        file_path = 'fake_responses/anime/main_page/Koe no Katachi (A Silent Voice) - MyAnimeList.net.html'
        url = 'https://myanimelist.net/anime/28851/Koe_no_Katachi'
        
        expected_results = {
            'favorites_count': 68020,
            'genres': {'Drama', 'School', 'Shounen'},
            'main_pic': 'https://cdn.myanimelist.net/images/anime/1122/96435.jpg',
            'members_count': 1770094,
            'num_episodes': 1,
            'popularity_rank': 23,
            'score': 8.97,
            'score_count': 1208990,
            'score_rank': 13,
            'source_type': 'Manga',
            'start_date': '2016-09-17T00:00:00',
            'status': 'Finished Airing',
            'studios': {'Kyoto Animation'},
            'synopsis': 'As a wild youth, elementary school student Shouya Ishida sought '
                        'to beat boredom in the cruelest ways. When the deaf Shouko '
                        'Nishimiya transfers into his class, Shouya and the rest of his '
                        'class thoughtlessly bully her for fun. However, when her mother '
                        'notifies the school, he is singled out and blamed for everything '
                        'done to her. With Shouko transferring out of the school, Shouya '
                        'is left at the mercy of his classmates. He is heartlessly '
                        'ostracized all throughout elementary and middle school, while '
                        'teachers turn a blind eye.  Now in his third year of high '
                        'school, Shouya is still plagued by his wrongdoings as a young '
                        'boy. Sincerely regretting his past actions, he sets out on a '
                        'journey of redemption: to meet Shouko once more and make '
                        "amends.   tells the heartwarming tale of Shouya's reunion with "
                        'Shouko and his honest attempts to redeem himself, all while '
                        'being continually haunted by the shadows of his past.  ',
            'title': 'Koe no Katachi',
            'type': 'Movie',
            'uid': '28851',
            'url': 'https://myanimelist.net/anime/28851/Koe_no_Katachi'
        }

        response = fake_html_response_from_file(file_path, url)
        result = self.spider.parse_anime_main_page_for_info(response, local_file_response = True)
        result = get_set_dict(result)

        self.assertDictEqual(
            result,
            expected_results
        )


if __name__ == '__main__':
    unittest.main()
