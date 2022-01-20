import unittest
from datetime import datetime

from crawler.spiders.anime_spider import AnimeSpider
from crawler.items.data_items.anime_item import AnimeItem
from crawler.pipelines.data_process_pipelines.anime_process_pipeline import AnimeProcessPipeline

from tests.utils import get_set_dict, fake_html_response_from_file

class AnimeTest(unittest.TestCase):
    
    def setUp(self):
        self.spider = AnimeSpider()
        self.process = AnimeProcessPipeline()

    def test_finished_tv(self):
        
        expected_results = {
            'uid': '5114', 
            'url': 'https://myanimelist.net/anime/5114/Fullmetal_Alchemist__Brotherhood', 
            'title': 'Fullmetal Alchemist: Brotherhood', 
            'main_pic': 'https://cdn.myanimelist.net/images/anime/1223/96541.jpg', 
            'synopsis': 'After a horrific alchemy experiment goes wrong in the Elric household, brothers Edward and Alphonse are left in a catastrophic new reality. Ignoring the alchemical principle banning human transmutation, the boys attempted to bring their recently deceased mother back to life. Instead, they suffered brutal personal loss: Alphonse\'s body disintegrated while Edward lost a leg and then sacrificed an arm to keep Alphonse\'s soul in the physical realm by binding it to a hulking suit of armor.  The brothers are rescued by their neighbor Pinako Rockbell and her granddaughter Winry. Known as a bio-mechanical engineering prodigy, Winry creates prosthetic limbs for Edward by utilizing "automail," a tough, versatile metal used in robots and combat armor. After years of training, the Elric brothers set off on a quest to restore their bodies by locating the Philosopher\'s Stone—a powerful gem that allows an alchemist to defy the traditional laws of Equivalent Exchange.  As Edward becomes an infamous alchemist and gains the nickname "Fullmetal," the boys\' journey embroils them in a growing conspiracy that threatens the fate of the world.  ', 
            'type': 'TV', 
            'source_type': 'Manga', 
            'num_episodes': 64, 
            'status': 'Finished Airing', 
            'start_date': '2009-04-05T00:00:00', 
            'end_date': '2010-07-04T00:00:00', 
            'season': 'Spring 2009', 
            'studios': {'Bones'}, 
            'genres': {'Adventure', 'Action', 'Comedy', 'Drama', 'Fantasy', 'Military', 'Shounen'}, 
            'score': 9.04, 
            'score_count': 1701928, 
            'score_rank': 1, 
            'popularity_rank': 3, 
            'members_count': 2664818, 
            'favorites_count': 188480, 
            'watching_count': 204701, 
            'completed_count': 1934871, 
            'on_hold_count': 90568, 
            'dropped_count': 40365, 
            'plan_to_watch_count': 395238, 
            'total_count': 2665743, 
            'score_10_count': 832081, 
            'score_09_count': 477200, 
            'score_08_count': 240332, 
            'score_07_count': 85309, 
            'score_06_count': 24823, 
            'score_05_count': 11590, 
            'score_04_count': 4061, 
            'score_03_count': 1931, 
            'score_02_count': 2484, 
            'score_01_count': 22117, 
            'clubs': {'20081', '70446', '9407', '25565', '20278', '807', '52', '69803', '15638', '29065', '21116', '16091', '797', '16895', '81804', '15706', '11099', '14441', '1365', '14122', '15696', '11044', '77431', '77864', '21395', '11018', '62931', '73313', '799', '37279', '12678', '4855', '2341', '80863', '4996', '40745', '72190', '39125', '11014', '233', '58837', '17594', '23714', '23124', '22141', '33815', '5450', '79723', '21211', '64093', '79626', '22918', '6302', '71682', '11732', '10390', '19222', '31351', '5789', '14297', '73707', '34135', '19492', '42249', '20364', '29699', '10347', '8330', '5759', '21622', '73696', '25332', '382', '3963', '6498', '5374', '13946', '23044', '4812', '79725', '19344', '25477', '6442', '16476', '2913', '9477', '23440', '55973', '68432', '21156', '23502', '1525', '14606', '16345', '11114', '6986', '11569', '11715', '5964', '68339', '24824', '7004', '17921', '20325', '14746', '38699', '22201', '78423', '70620', '11887', '540', '3578', '433', '21072', '18739', '4724', '36757', '62547', '4672', '17512', '12062', '15590', '13211', '10065', '17043', '36041', '40193', '18079', '14806', '21203', '13450', '15180', '14256', '65807', '15891', '7326', '74637', '11686', '20468', '83202', '20589', '15109', '29503', '14207', '71958', '24967', '11654', '7525', '3878', '19563', '21426', '80102', '11045', '38337', '62539', '6391', '5586', '59203', '24778', '18041', '14697', '8281', '73325', '73729', '20777', '19779', '22435', '22523', '7271', '17322', '13156', '71769', '32283', '14767', '11214', '18471', '6321', '18602', '19242', '17129', '11339', '25937', '6440', '7091', '20454', '10877', '533', '7653', '11139', '11706', '23835', '9785', '19253', '19692', '37623', '11357', '8257', '31117', '21076', '7956', '23627', '80129', '33569', '65599', '12199', '13364', '72667', '67413', '16132', '22749', '71985', '10357', '20927', '21165', '74087', '7023', '23932', '13113', '78151', '33111', '65155', '10173', '75398', '12889', '20999', '19901', '20360', '31763', '19652', '12576', '72962', '14440', '21939', '73735', '12805', '18894', '24547', '66396', '1397', '23608', '8868', '25798', '9829', '15665', '16082', '12581', '35385', '13223', '66968', '13667', '13711', '23561', '8486', '5522', '21710', '14720', '8011', '64301', '14195', '14499', '5968', '38615', '23537', '21171', '24023', '10088', '71551', '13025', '11077', '25761', '9066', '34493', '19209', '12312', '27707', '61249', '9965', '15530', '29831', '35811', '23699', '37855', '24472', '21472', '70599', '78850', '22351', '24442', '18325', '29171', '15610', '60885', '40223', '13780', '57989', '23704', '27609', '66331', '63387', '75205', '65063', '32591', '11982', '22446', '61019', '24594', '6951', '20909', '78554', '20089', '59261', '83006', '35641', '32675', '14728', '9018', '20881', '13155', '62015', '66464', '32745', '24139', '6111', '28629', '22469', '68171', '33609', '73367', '12664', '29073', '19349', '80264', '80461', '32309', '26295', '12289', '10479', '41141', '41225', '32623', '20105', '11932', '20106', '20120', '24351', '34790', '62619', '21075', '78271', '26771', '17461', '33945', '20507', '69443', '20894', '24933', '52371', '19868', '21640', '75584', '82528', '69199', '29151', '71513', '66019', '77257', '19478', '74761', '72427', '21394', '73013', '81215', '26606', '22217', '71321', '20548', '76135', '38749', '24734', '74157', '13530', '15749', '63229', '70077', '23555', '25700', '58157', '70554', '68848', '79587', '76269', '35459', '30187', '36459', '17324', '23944', '71337', '61127', '69591', '22922', '80149', '72088', '67151', '23536', '80718', '78394', '81092', '64653', '58507', '9578', '59079', '70786', '81887', '70862', '79425', '65999', '10696', '79597', '61149', '76387', '74082', '22804', '76638', '66497', '40227', '77595', '32253', '67790', '63929', '70472', '37465', '25925', '75156', '78706', '78741', '33783', '78875', '75735', '82100', '65357', '34031', '68849', '28765', '79258', '76101', '65829', '38907', '83394', '29579', '80095', '69599', '69624', '23052', '25367', '25416', '36631', '25516', '63359', '75019', '37361', '75170', '14216', '75353', '23873', '59019', '65227', '38667', '83304', '69501', '39593', '69531', '74129', '41791', '69969', '25542', '74779', '36945', '32391', '13923', '64333', '81515', '37731', '68275', '68531', '33699', '38081', '59085', '38091', '71218', '79212', '73698', '61009', '71469', '69365', '76254', '73796', '22573', '34846', '15428', '35357', '83657', '66410', '24867', '31027', '25158', '19351', '41231', '69855', '72228', '67260', '78046', '11566', '80888', '80915', '26143', '78676', '65015', '81711', '78820', '58985', '38039', '75687', '73614', '75802', '75913', '79221', '71376', '34353', '69205', '83211', '76258', '20824', '61207', '80010', '83692', '35725', '66713', '74297', '77268', '69751', '77325', '62743', '72043', '67010', '74335', '36307', '74472', '32171', '62947', '80716', '70008', '70058', '67692', '63547', '36951', '74977', '70348', '78365', '72639', '67835', '16418', '80947', '75155', '68057', '5332', '70473', '78468', '37609', '58401', '70490', '73177', '70533', '33131', '81550', '70552', '33235', '64929', '81677', '75371', '73341', '73354', '38023', '81724', '26791', '27477', '73373', '68753', '79061', '82611', '76033', '73951', '76592', '71702', '83690', '69565', '76801', '71963', '74309', '67031', '13680', '41829', '21121', '72345', '19609', '74781', '70199', '72446', '57717', '67884', '68058', '64623', '58505', '37843', '58531', '23902', '68600', '65345', '68773'}, 
            'pics': {'https://cdn.myanimelist.net/images/anime/13/13738.jpg', 'https://cdn.myanimelist.net/images/anime/2/17090.jpg', 'https://cdn.myanimelist.net/images/anime/2/17472.jpg', 'https://cdn.myanimelist.net/images/anime/5/47603.jpg', 'https://cdn.myanimelist.net/images/anime/10/57095.jpg', 'https://cdn.myanimelist.net/images/anime/7/74317.jpg', 'https://cdn.myanimelist.net/images/anime/1521/94614.jpg', 'https://cdn.myanimelist.net/images/anime/1208/94745.jpg', 'https://cdn.myanimelist.net/images/anime/1223/96541.jpg', 'https://cdn.myanimelist.net/images/anime/1286/96542.jpg', 'https://cdn.myanimelist.net/images/anime/1629/108486.jpg'}
        }

        file_path = 'fake_responses/anime/main_page/Fullmetal Alchemist_ Brotherhood - MyAnimeList.net.html'
        url = 'https://myanimelist.net/anime/5114/Fullmetal_Alchemist__Brotherhood'
        response = fake_html_response_from_file(file_path, url)
        result = self.spider.parse_anime_main_page_for_info(response, local_file_response = True)

        file_path = 'fake_responses/anime/stats_page/Fullmetal Alchemist_ Brotherhood - Statistics - MyAnimeList.net.html'
        url = 'https://myanimelist.net/anime/5114/Fullmetal_Alchemist__Brotherhood/stats'
        response = fake_html_response_from_file(file_path, url)
        result = {**result, **(self.spider.parse_stats_page_for_stats(response, local_file_response = True))}

        file_path = 'fake_responses/anime/clubs_page/Fullmetal Alchemist_ Brotherhood - Clubs - MyAnimeList.net.html'
        url = 'https://myanimelist.net/anime/5114/Fullmetal_Alchemist__Brotherhood/clubs'
        response = fake_html_response_from_file(file_path, url)
        result = {**result, **(self.spider.parse_clubs_page_for_clubs(response, local_file_response = True))}

        file_path = 'fake_responses/anime/pics_page/Fullmetal Alchemist_ Brotherhood - Pictures - MyAnimeList.net.html'
        url = 'https://myanimelist.net/anime/5114/Fullmetal_Alchemist__Brotherhood/pics'
        response = fake_html_response_from_file(file_path, url)
        result = {**result, **(self.spider.parse_pics_page_for_pics(response, local_file_response = True))}

        result['crawl_date'] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        
        result = AnimeItem(**result)
        result = self.process.process_item(result, self.spider)
        result = dict(result)
        result = get_set_dict(result)

        del result['crawl_date']

        self.assertDictEqual(
            result,
            expected_results
        )
    
    def test_airing_tv(self):

        expected_results = {
            'uid': '45576', 
            'url': 'https://myanimelist.net/anime/45576/Mushoku_Tensei__Isekai_Ittara_Honki_Dasu_Part_2', 
            'title': 'Mushoku Tensei: Isekai Ittara Honki Dasu Part 2', 
            'main_pic': 'https://cdn.myanimelist.net/images/anime/1028/117777.jpg', 
            'synopsis': 'After the mysterious mana calamity, Rudeus Greyrat and his fierce student Eris Boreas Greyrat are teleported to the Demon Continent. There, they team up with their newfound companion Ruijerd Supardia—the former leader of the Superd\'s Warrior group—to form "Dead End," a successful adventurer party. Making a name for themselves, the trio journeys across the continent to make their way back home to Fittoa.  Following the advice he received from the faceless god Hitogami, Rudeus saves Kishirika Kishirisu, the Great Emperor of the Demon World, who rewards him by granting him a strange power. Now, as Rudeus masters the powerful ability that offers a number of new opportunities, it might prove to be more than what he bargained for when unexpected dangers threaten to hinder their travels.  ', 
            'type': 'TV', 
            'source_type': 'Light novel', 
            'num_episodes': 12, 
            'status': 'Currently Airing', 
            'start_date': '2021-10-04T00:00:00', 
            'season': 'Fall 2021', 
            'studios': {'Studio Bind'}, 
            'genres': {'Drama', 'Fantasy', 'Ecchi'}, 
            'score': 8.61, 
            'score_count': 43411, 
            'score_rank': 67, 
            'popularity_rank': 559, 
            'members_count': 303755, 
            'favorites_count': 3916, 
            'watching_count': 168471, 
            'completed_count': 0, 
            'on_hold_count': 1607, 
            'dropped_count': 591, 
            'plan_to_watch_count': 133025, 
            'total_count': 303694, 
            'score_10_count': 11953, 
            'score_09_count': 13594, 
            'score_08_count': 11910, 
            'score_07_count': 4058, 
            'score_06_count': 928, 
            'score_05_count': 331, 
            'score_04_count': 144, 
            'score_03_count': 83, 
            'score_02_count': 78, 
            'score_01_count': 332, 
            'clubs': {'27907', '8652', '59197', '81441'}, 
            'pics': {'https://cdn.myanimelist.net/images/anime/1687/110933.jpg', 'https://cdn.myanimelist.net/images/anime/1868/115018.jpg', 'https://cdn.myanimelist.net/images/anime/1311/117222.jpg', 'https://cdn.myanimelist.net/images/anime/1028/117777.jpg'}
        }
            
        file_path = 'fake_responses/anime/main_page/Mushoku Tensei_ Isekai Ittara Honki Dasu Part 2 - MyAnimeList.net.html'
        url = 'https://myanimelist.net/anime/45576/Mushoku_Tensei__Isekai_Ittara_Honki_Dasu_Part_2'
        response = fake_html_response_from_file(file_path, url)
        result = self.spider.parse_anime_main_page_for_info(response, local_file_response = True)

        file_path = 'fake_responses/anime/stats_page/Mushoku Tensei_ Isekai Ittara Honki Dasu Part 2 - Statistics - MyAnimeList.net.html'
        url = 'https://myanimelist.net/anime/45576/Mushoku_Tensei__Isekai_Ittara_Honki_Dasu_Part_2/stats'
        response = fake_html_response_from_file(file_path, url)
        result = {**result, **(self.spider.parse_stats_page_for_stats(response, local_file_response = True))}

        file_path = 'fake_responses/anime/clubs_page/Mushoku Tensei_ Isekai Ittara Honki Dasu Part 2 - Clubs - MyAnimeList.net.html'
        url = 'https://myanimelist.net/anime/45576/Mushoku_Tensei__Isekai_Ittara_Honki_Dasu_Part_2/clubs'
        response = fake_html_response_from_file(file_path, url)
        result = {**result, **(self.spider.parse_clubs_page_for_clubs(response, local_file_response = True))}

        file_path = 'fake_responses/anime/pics_page/Mushoku Tensei_ Isekai Ittara Honki Dasu Part 2 - Pictures - MyAnimeList.net.html'
        url = 'https://myanimelist.net/anime/45576/Mushoku_Tensei__Isekai_Ittara_Honki_Dasu_Part_2/pics'
        response = fake_html_response_from_file(file_path, url)
        result = {**result, **(self.spider.parse_pics_page_for_pics(response, local_file_response = True))}
        
        result['crawl_date'] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

        result = AnimeItem(**result)
        result = self.process.process_item(result, self.spider)
        result = dict(result)
        result = get_set_dict(result)

        del result['crawl_date']

        self.assertDictEqual(
            result,
            expected_results
        )

    def test_tv_upcoming(self):
        
        expected_results = {
            'uid': '44511', 
            'url': 'https://myanimelist.net/anime/44511/Chainsaw_Man', 
            'title': 'Chainsaw Man', 
            'main_pic': 'https://cdn.myanimelist.net/images/anime/1632/110707.jpg', 
            'synopsis': "Denji has a simple dream—to live a happy and peaceful life, spending time with a girl he likes. This is a far cry from reality, however, as Denji is forced by the yakuza into killing devils in order to pay off his crushing debts. Using his pet devil Pochita as a weapon, he is ready to do anything for a bit of cash.  Unfortunately, he has outlived his usefulness and is murdered by a devil in contract with the yakuza. However, in an unexpected turn of events, Pochita merges with Denji's dead body and grants him the powers of a chainsaw devil. Now able to transform parts of his body into chainsaws, a revived Denji uses his new abilities to quickly and brutally dispatch his enemies. Catching the eye of the official devil hunters who arrive at the scene, he is offered work at the Public Safety Bureau as one of them. Now with the means to face even the toughest of enemies, Denji will stop at nothing to achieve his simple teenage dreams.  ", 
            'type': 'TV', 
            'source_type': 'Manga', 
            'status': 'Not yet aired', 
            'studios': {'MAPPA'}, 
            'genres': {'Action', 'Adventure', 'Demons', 'Shounen'}, 
            'popularity_rank': 678, 
            'members_count': 265760, 
            'favorites_count': 5478, 
            'watching_count': 0, 
            'completed_count': 0, 
            'on_hold_count': 0, 
            'dropped_count': 0, 
            'plan_to_watch_count': 265750, 
            'total_count': 265750, 
            'score_10_count': 0, 
            'score_09_count': 0, 
            'score_08_count': 0, 
            'score_07_count': 0, 
            'score_06_count': 0, 
            'score_05_count': 0, 
            'score_04_count': 0, 
            'score_03_count': 0, 
            'score_02_count': 0, 
            'score_01_count': 0, 
            'clubs': {'8652', '82156', '83352', '80480', '80758', '82880', '80393', '81083', '83199', '83805'}, 
            'pics': {'https://cdn.myanimelist.net/images/anime/1915/110629.jpg', 'https://cdn.myanimelist.net/images/anime/1632/110707.jpg'}
        }

        file_path = 'fake_responses/anime/main_page/Chainsaw Man - MyAnimeList.net.html'
        url = 'https://myanimelist.net/anime/44511/Chainsaw_Man'
        response = fake_html_response_from_file(file_path, url)
        result = self.spider.parse_anime_main_page_for_info(response, local_file_response = True)

        file_path = 'fake_responses/anime/stats_page/Chainsaw Man - Statistics - MyAnimeList.net.html'
        url = 'https://myanimelist.net/anime/44511/Chainsaw_Man/stats'
        response = fake_html_response_from_file(file_path, url)
        result = {**result, **(self.spider.parse_stats_page_for_stats(response, local_file_response = True))}

        file_path = 'fake_responses/anime/clubs_page/Chainsaw Man - Clubs - MyAnimeList.net.html'
        url = 'https://myanimelist.net/anime/44511/Chainsaw_Man/clubs'
        response = fake_html_response_from_file(file_path, url)
        result = {**result, **(self.spider.parse_clubs_page_for_clubs(response, local_file_response = True))}

        file_path = 'fake_responses/anime/pics_page/Chainsaw Man - Pictures - MyAnimeList.net.html'
        url = 'https://myanimelist.net/anime/44511/Chainsaw_Man/pics'
        response = fake_html_response_from_file(file_path, url)
        result = {**result, **(self.spider.parse_pics_page_for_pics(response, local_file_response = True))}

        result['crawl_date'] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

        result = AnimeItem(**result)
        result = self.process.process_item(result, self.spider)
        result = dict(result)
        result = get_set_dict(result)
        
        del result['crawl_date']

        self.assertDictEqual(
            result,
            expected_results
        )
    
    def test_movie(self):
        
        expected_results = {
            'uid': '28851', 
            'url': 'https://myanimelist.net/anime/28851/Koe_no_Katachi', 
            'title': 'Koe no Katachi', 
            'main_pic': 'https://cdn.myanimelist.net/images/anime/1122/96435.jpg', 
            'synopsis': "As a wild youth, elementary school student Shouya Ishida sought to beat boredom in the cruelest ways. When the deaf Shouko Nishimiya transfers into his class, Shouya and the rest of his class thoughtlessly bully her for fun. However, when her mother notifies the school, he is singled out and blamed for everything done to her. With Shouko transferring out of the school, Shouya is left at the mercy of his classmates. He is heartlessly ostracized all throughout elementary and middle school, while teachers turn a blind eye.  Now in his third year of high school, Shouya is still plagued by his wrongdoings as a young boy. Sincerely regretting his past actions, he sets out on a journey of redemption: to meet Shouko once more and make amends.   tells the heartwarming tale of Shouya's reunion with Shouko and his honest attempts to redeem himself, all while being continually haunted by the shadows of his past.  ", 
            'type': 'Movie', 
            'source_type': 'Manga', 
            'num_episodes': 1, 
            'status': 'Finished Airing', 
            'start_date': '2016-09-17T00:00:00', 
            'studios': {'Kyoto Animation'}, 
            'genres': {'Drama', 'School', 'Shounen'}, 
            'score': 8.95, 
            'score_count': 1228711, 
            'score_rank': 13, 
            'popularity_rank': 23, 
            'members_count': 1770094, 
            'favorites_count': 68020, 
            'watching_count': 36346, 
            'completed_count': 1498557, 
            'on_hold_count': 5066, 
            'dropped_count': 2389, 
            'plan_to_watch_count': 227736, 
            'total_count': 1770094, 
            'score_10_count': 500747, 
            'score_09_count': 386659, 
            'score_08_count': 211530, 
            'score_07_count': 84569, 
            'score_06_count': 26188, 
            'score_05_count': 9692, 
            'score_04_count': 4144, 
            'score_03_count': 1694, 
            'score_02_count': 933, 
            'score_01_count': 2555, 
            'clubs': {'32683', '70446', '35120', '69803', '73113', '77864', '73313', '71106', '80863', '64093', '79626', '74355', '73264', '36931', '6498', '4284', '71894', '23777', '30325', '78423', '78176', '74429', '83202', '75456', '77871', '78600', '72520', '78786', '71985', '76798', '70702', '79381', '74937', '73848', '79587', '74328', '80718', '80772', '75013', '75359', '78506', '78405', '72532', '78510', '79133', '79258', '81835', '76372', '76771', '74061', '74129', '80161', '80712', '73793', '79485', '80046', '74677', '75249', '81630', '81725', '76185', '83002', '77151', '78380', '74033', '74297', '80604', '79934', '74538', '74751'}, 
            'pics': {'https://cdn.myanimelist.net/images/anime/6/79634.jpg', 'https://cdn.myanimelist.net/images/anime/3/80136.jpg', 'https://cdn.myanimelist.net/images/anime/10/80797.jpg', 'https://cdn.myanimelist.net/images/anime/9/84678.jpg', 'https://cdn.myanimelist.net/images/anime/1122/96435.jpg', 'https://cdn.myanimelist.net/images/anime/1254/109618.jpg'}, 
            'end_date': '2016-09-17T00:00:00'
        }

        file_path = 'fake_responses/anime/main_page/Koe no Katachi (A Silent Voice) - MyAnimeList.net.html'
        url = 'https://myanimelist.net/anime/28851/Koe_no_Katachi'
        response = fake_html_response_from_file(file_path, url)
        result = self.spider.parse_anime_main_page_for_info(response, local_file_response = True)

        file_path = 'fake_responses/anime/stats_page/Koe no Katachi (A Silent Voice) - Statistics - MyAnimeList.net.html'
        url = 'https://myanimelist.net/anime/28851/Koe_no_Katachi/stats/stats'
        response = fake_html_response_from_file(file_path, url)
        result = {**result, **(self.spider.parse_stats_page_for_stats(response, local_file_response = True))}

        file_path = 'fake_responses/anime/clubs_page/Koe no Katachi (A Silent Voice) - Clubs - MyAnimeList.net.html'
        url = 'https://myanimelist.net/anime/28851/Koe_no_Katachi/stats/clubs'
        response = fake_html_response_from_file(file_path, url)
        result = {**result, **(self.spider.parse_clubs_page_for_clubs(response, local_file_response = True))}

        file_path = 'fake_responses/anime/pics_page/Koe no Katachi (A Silent Voice) - Pictures - MyAnimeList.net.html'
        url = 'https://myanimelist.net/anime/28851/Koe_no_Katachi/stats/pics'
        response = fake_html_response_from_file(file_path, url)
        result = {**result, **(self.spider.parse_pics_page_for_pics(response, local_file_response = True))}
        
        result['crawl_date'] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        
        result = AnimeItem(**result)
        result = self.process.process_item(result, self.spider)
        result = dict(result)
        result = get_set_dict(result)

        del result['crawl_date']
        
        self.assertDictEqual(
            result,
            expected_results
        )
    

if __name__ == '__main__':
    unittest.main()
