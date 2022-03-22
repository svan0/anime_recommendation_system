from datetime import datetime

from math import isclose
import logging

from scrapy.exceptions import DropItem
from crawler.items.data_items.anime_item import AnimeItem

class AnimeProcessPipeline:
    """
        Drop anime items that do not statisy integrity constraints
    """
    def __init__(self, stats = None):
        self.stats = stats
    
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.stats)

    def process_item(self, item, spider):
        if not isinstance(item, AnimeItem):
            return item

        if 'url' not in item:
            raise DropItem("AnimeItem dropped because 'url' is null")
        
        not_null_fields = [
            'crawl_date', 'uid', 'title', 'synopsis', 'main_pic', 'type',
            'source_type', 'status', 'genres', 'popularity_rank', 'members_count',
            'favorites_count', 'watching_count', 'completed_count',
            'on_hold_count', 'dropped_count', 'plan_to_watch_count',
            'total_count'
        ]
        for field in not_null_fields:
            if field not in item:
                raise DropItem(f"AnimeItem {item['url']} dropped because '{field}' is null")
        
        int_fields = [
            'popularity_rank', 'members_count', 'favorites_count', 
            'total_count', 'watching_count', 'completed_count',
            'on_hold_count', 'dropped_count', 'plan_to_watch_count'
        ]
        for field in int_fields:
            if not isinstance(item[field], int):
                raise DropItem(f"AnimeItem {item['url']} dropped because '{field}' is not an integer")
        
        if 'studios' in item and not isinstance(item['studios'], list):
            raise DropItem(f"AnimeItem {item['url']} dropped because 'studios' is not a list")
        
        if not isinstance(item['genres'], list):
            raise DropItem(f"AnimeItem {item['url']} dropped because 'genres' is not a list")
        
        if ('clubs' in item) and (not isinstance(item['clubs'], list)):
            raise DropItem(f"AnimeItem {item['url']} dropped because 'clubs' is not a list")
        
        if ('pics' in item) and (not isinstance(item['pics'], list)):
            raise DropItem(f"AnimeItem {item['url']} dropped because 'pics' is not a list")
        
        if item['type'] not in {"TV", "Movie", "Special", "OVA", "ONA", "Special"}:
            raise DropItem(f"AnimeItem {item['url']} dropped because 'type' {item['type']} is not known")
        
        if item['source_type'] not in {"Web novel", "Mixed media", "Manga", "Book", "4-koma manga", "Music", "Other", "One-shot", "Doujinshi", "Light novel", "Novel", "Manhwa", "Manhua", "Original", "Visual novel", "Game", "Card game", "Web manga"}:
            raise DropItem(f"AnimeItem {item['url']} dropped because 'source_type' {item['source_type']} is not known")
        
        if item['status'] not in {"Not yet aired", "Currently Airing", "Finished Airing"}:
            raise DropItem(f"AnimeItem {item['url']} dropped because 'status' {item['status']} is not known")
        

        if  (item['status'] == "Not yet aired") or ('score' not in item) or ('score_count' not in item):
            item['score'] = None
            item['score_count'] = None
            item['score_rank'] = None

            del item['score']
            del item['score_count']
            del item['score_rank']

            for score in range(1, 11):
                item['score_{:02d}_count'.format(score)] = 0
        
        if item['status'] == "Not yet aired":
            item['total_count'] -= item['watching_count']
            item['total_count'] -= item['completed_count']
            item['total_count'] -= item['on_hold_count']
            item['total_count'] -= item['dropped_count']

            item['watching_count'] = 0
            item['completed_count'] = 0
            item['on_hold_count'] = 0
            item['dropped_count'] = 0

        if item['status'] == "Currently Airing":
            item['total_count'] -= item['completed_count']
            item['completed_count'] = 0

            if 'start_date' not in item:
                raise DropItem(f"AnimeItem {item['url']} dropped because it is airing but 'start_date' is not known")
            
            if item['type'] == 'TV' and 'season' not in item:
                raise DropItem(f"AnimeItem {item['url']} dropped because it is an airing TV show but 'season' is not known")
        
        if item['status'] == "Finished Airing":
            if 'num_episodes' not in item:
                raise DropItem(f"AnimeItem {item['url']} dropped because it has finished airing but 'num_episodes' is not known")
            
            if 'start_date' not in item:
                raise DropItem(f"AnimeItem {item['url']} dropped because it has finished airing but 'start_date' is not known")
            
            if 'end_date' not in item:
                item['end_date'] = item['start_date']

            if item['type'] == 'TV' and 'season' not in item:
                raise DropItem(f"AnimeItem {item['url']} dropped because it is a finished airing TV show but 'season' is not known")
        
        if item['total_count'] != (item['watching_count'] + item['completed_count'] + item['on_hold_count'] + item['dropped_count'] + item['plan_to_watch_count']):
            raise DropItem(f"AnimeItem {item['url']} dropped because watch 'status_count' do not sum up to 'total_count'")
        
        sum_score_voters = sum([item['score_{:02d}_count'.format(score)] if 'score_{:02d}_count'.format(score) in item else 0 for score in range(1, 11)])
        if ('score_count' in item) and (item['score_count'] != sum_score_voters):
            item['score_count'] = sum_score_voters
            logging.debug(f"{item['url']} 'score_xx_count' do not sum up to 'score_count' count. Changing 'score_count' to the sum")

        if ('score' in item):
            average_score = sum([score * item['score_{:02d}_count'.format(score)] if 'score_{:02d}_count'.format(score) in item else 0 for score in range(1, 11)]) / sum_score_voters
            average_score = int(100 * average_score) / 100
            if not isclose(item['score'] , average_score):
                item['score'] = average_score
                logging.debug(f"{item['url']} xx * 'score_xx_count' do not average up to 'score' count. Changing 'score' to the average")

        logging.debug(f"AnimeItem {item['url']} processed")
        if self.stats:
            self.stats.inc_value(f'{self.__class__.__name__}_processed_{item.__class__.__name__}', count = 1)
        return item

        
        
        
        

