import unittest

from crawler.spiders.top_anime_spider import TopAnimeSpider

from tests.utils import get_set_dict, fake_html_response_from_file

class TopAnimeScheduleTest(unittest.TestCase):
    
    def setUp(self):
        self.spider = TopAnimeSpider()
    
    def test(self):

        file_path = 'fake_responses/top_anime/Top Anime (50 - ) - MyAnimeList.net.html'
        url = 'https://myanimelist.net/topanime.php?limit=50'
        
        expected_results = [
            {'url': 'https://myanimelist.net/anime/457/Mushishi'}, 
            {'url': 'https://myanimelist.net/anime/2921/Ashita_no_Joe_2'}, 
            {'url': 'https://myanimelist.net/anime/28891/Haikyuu_Second_Season'}, 
            {'url': 'https://myanimelist.net/anime/5258/Hajime_no_Ippo__New_Challenger'}, 
            {'url': 'https://myanimelist.net/anime/431/Howl_no_Ugoku_Shiro'}, 
            {'url': 'https://myanimelist.net/anime/40591/Kaguya-sama_wa_Kokurasetai__Tensai-tachi_no_Renai_Zunousen'}, 
            {'url': 'https://myanimelist.net/anime/11665/Natsume_Yuujinchou_Shi'}, 
            {'url': 'https://myanimelist.net/anime/33352/Violet_Evergarden'}, 
            {'url': 'https://myanimelist.net/anime/34591/Natsume_Yuujinchou_Roku'}, 
            {'url': 'https://myanimelist.net/anime/38329/Seishun_Buta_Yarou_wa_Yumemiru_Shoujo_no_Yume_wo_Minai'}, 
            {'url': 'https://myanimelist.net/anime/2001/Tengen_Toppa_Gurren_Lagann'}, 
            {'url': 'https://myanimelist.net/anime/7311/Suzumiya_Haruhi_no_Shoushitsu'}, 
            {'url': 'https://myanimelist.net/anime/1535/Death_Note'}, 
            {'url': 'https://myanimelist.net/anime/22135/Ping_Pong_the_Animation'}, 
            {'url': 'https://myanimelist.net/anime/35760/Shingeki_no_Kyojin_Season_3'}, 
            {'url': 'https://myanimelist.net/anime/28957/Mushishi_Zoku_Shou__Suzu_no_Shizuku'}, 
            {'url': 'https://myanimelist.net/anime/12355/Ookami_Kodomo_no_Ame_to_Yuki'}, 
            {'url': 'https://myanimelist.net/anime/45576/Mushoku_Tensei__Isekai_Ittara_Honki_Dasu_Part_2'}, 
            {'url': 'https://myanimelist.net/anime/21/One_Piece'}, 
            {'url': 'https://myanimelist.net/anime/40834/Ousama_Ranking'}, 
            {'url': 'https://myanimelist.net/anime/31757/Kizumonogatari_II__Nekketsu-hen'}, 
            {'url': 'https://myanimelist.net/anime/48569/86_Part_2'}, 
            {'url': 'https://myanimelist.net/anime/37991/JoJo_no_Kimyou_na_Bouken_Part_5__Ougon_no_Kaze'}, 
            {'url': 'https://myanimelist.net/anime/28735/Shouwa_Genroku_Rakugo_Shinjuu'}, 
            {'url': 'https://myanimelist.net/anime/7785/Yojouhan_Shinwa_Taikei'}, 
            {'url': 'https://myanimelist.net/anime/32983/Natsume_Yuujinchou_Go'}, 
            {'url': 'https://myanimelist.net/anime/10379/Natsume_Yuujinchou_San'}, 
            {'url': 'https://myanimelist.net/anime/37779/Yakusoku_no_Neverland'}, 
            {'url': 'https://myanimelist.net/anime/11741/Fate_Zero_2nd_Season'}, 
            {'url': 'https://myanimelist.net/anime/40417/Fruits_Basket_2nd_Season'}, 
            {'url': 'https://myanimelist.net/anime/19647/Hajime_no_Ippo__Rising'}, 
            {'url': 'https://myanimelist.net/anime/36098/Kimi_no_Suizou_wo_Tabetai'}, 
            {'url': 'https://myanimelist.net/anime/38000/Kimetsu_no_Yaiba'}, 
            {'url': 'https://myanimelist.net/anime/4565/Tengen_Toppa_Gurren_Lagann_Movie_2__Lagann-hen'}, 
            {'url': 'https://myanimelist.net/anime/21329/Mushishi__Hihamukage'}, 
            {'url': 'https://myanimelist.net/anime/5300/Zoku_Natsume_Yuujinchou'}, 
            {'url': 'https://myanimelist.net/anime/12365/Bakuman_3rd_Season'}, 
            {'url': 'https://myanimelist.net/anime/33049/Fate_stay_night_Movie__Heavens_Feel_-_II_Lost_Butterfly'}, 
            {'url': 'https://myanimelist.net/anime/4282/Kara_no_Kyoukai_5__Mujun_Rasen'}, 
            {'url': 'https://myanimelist.net/anime/35839/Sora_yori_mo_Tooi_Basho'}, 
            {'url': 'https://myanimelist.net/anime/32/Neon_Genesis_Evangelion__The_End_of_Evangelion'}, 
            {'url': 'https://myanimelist.net/anime/38474/Yuru_Camp%E2%96%B3_Season_2'}, 
            {'url': 'https://myanimelist.net/anime/40776/Haikyuu__To_the_Top_2nd_Season'}, 
            {'url': 'https://myanimelist.net/anime/40787/Josee_to_Tora_to_Sakana-tachi'}, 
            {'url': 'https://myanimelist.net/anime/35843/Gintama__Porori-hen'}, 
            {'url': 'https://myanimelist.net/anime/801/Koukaku_Kidoutai__Stand_Alone_Complex_2nd_GIG'}, 
            {'url': 'https://myanimelist.net/anime/42203/Re_Zero_kara_Hajimeru_Isekai_Seikatsu_2nd_Season_Part_2'}, 
            {'url': 'https://myanimelist.net/anime/170/Slam_Dunk'}, 
            {'url': 'https://myanimelist.net/anime/30276/One_Punch_Man'}, 
            {'url': 'https://myanimelist.net/anime/16498/Shingeki_no_Kyojin'}
        ]

        result = []
        response = fake_html_response_from_file(file_path, url)
        for top_anime_schedule in self.spider.parse(response):
            del top_anime_schedule['last_inspect_date']
            result.append(top_anime_schedule)

        self.assertCountEqual(
            result,
            expected_results
        )
