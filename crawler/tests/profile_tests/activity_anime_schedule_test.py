import unittest

from crawler.spiders.profile_spider import ProfileSpider

from tests.utils import fake_xml_response_from_file

class ActivityAnimeScheduleTest(unittest.TestCase):
    
    def setUp(self):
        self.spider = ProfileSpider()
    
    def test(self):
        
        file_path = 'fake_responses/profile/activity_page/rss.xml'
        url = 'https://myanimelist.net/rss.php?type=rw&u=tazillo'

        expected_results = [
            {'url': 'https://myanimelist.net/anime/46095/Vivy__Fluorite_Eyes_Song'}, 
            {'url': 'https://myanimelist.net/anime/40456/Kimetsu_no_Yaiba_Movie__Mugen_Ressha-hen'}, 
            {'url': 'https://myanimelist.net/anime/42203/Re_Zero_kara_Hajimeru_Isekai_Seikatsu_2nd_Season_Part_2'}, 
            {'url': 'https://myanimelist.net/anime/35848/Promare'}, 
            {'url': 'https://myanimelist.net/anime/38000/Kimetsu_no_Yaiba'}, 
            {'url': 'https://myanimelist.net/anime/39587/Re_Zero_kara_Hajimeru_Isekai_Seikatsu_2nd_Season'}, 
            {'url': 'https://myanimelist.net/anime/38414/Re_Zero_kara_Hajimeru_Isekai_Seikatsu_-_Hyouketsu_no_Kizuna'}, 
            {'url': 'https://myanimelist.net/anime/31240/Re_Zero_kara_Hajimeru_Isekai_Seikatsu'}, 
            {'url': 'https://myanimelist.net/anime/38145/Doukyonin_wa_Hiza_Tokidoki_Atama_no_Ue'}, 
            {'url': 'https://myanimelist.net/anime/35363/Kobayashi-san_Chi_no_Maid_Dragon__Valentine_Soshite_Onsen_-_Amari_Kitai_Shinaide_Kudasai'}, 
            {'url': 'https://myanimelist.net/anime/33206/Kobayashi-san_Chi_no_Maid_Dragon'}, 
            {'url': 'https://myanimelist.net/anime/40815/Honzuki_no_Gekokujou__Shisho_ni_Naru_Tame_ni_wa_Shudan_wo_Erandeiraremasen_2nd_Season'}, 
            {'url': 'https://myanimelist.net/anime/40841/Honzuki_no_Gekokujou__Shisho_ni_Naru_Tame_ni_wa_Shudan_wo_Erandeiraremasen_OVA'}, 
            {'url': 'https://myanimelist.net/anime/39468/Honzuki_no_Gekokujou__Shisho_ni_Naru_Tame_ni_wa_Shudan_wo_Erandeiraremasen'}, 
            {'url': 'https://myanimelist.net/anime/40417/Fruits_Basket_2nd_Season'}
        ]
        
        response = fake_xml_response_from_file(file_path, url)
        result = []
        for activity in self.spider.parse_activity_page_for_scheduler(response):
            del activity['last_inspect_date']
            result.append(activity)
        
        self.assertCountEqual(
            result,
            expected_results
        )