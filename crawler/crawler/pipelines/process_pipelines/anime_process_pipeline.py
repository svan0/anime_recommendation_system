from math import isclose

from scrapy.exceptions import DropItem
from crawler.items.data_items.anime_item import AnimeItem

class AnimeProcessPipeline:

    def process_item(self, item, spider):
        if not isinstance(item, AnimeItem):
            return item
        
        if 'url' not in item:
            raise DropItem("AnimeItem dropped because 'url' is null")
        
        not_null_fields = [
            'crawl_date', 'uid', 'title', 'synopsis',
            'main_pic', 'type', 'source_type', 'status',
            'studios', 'genres', 'popularity_rank', 'members_count',
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
        
        if not isinstance(item['studios'], list):
            raise DropItem(f"AnimeItem {item['url']} dropped because 'studios' is not a list")
        
        if not isinstance(item['genres'], list):
            raise DropItem(f"AnimeItem {item['url']} dropped because 'genres' is not a list")
        
        if ('clubs' in item) and (not isinstance(item['clubs'], list)):
            raise DropItem(f"AnimeItem {item['url']} dropped because 'clubs' is not a list")
        
        if ('pics' in item) and (not isinstance(item['pics'], list)):
            raise DropItem(f"AnimeItem {item['url']} dropped because 'pics' is not a list")
        
        if item['type'] not in {"TV", "Movie", "Special", "OVA", "ONA", "Special"}:
            raise DropItem(f"AnimeItem {item['url']} dropped because 'type' {item['type']} is not known")
        
        if item['source_type'] not in {"Manga", "One-shot", "Doujinshi", "Light novel", "Novel", "Manhwa", "Manhua", "Original"}:
            raise DropItem(f"AnimeItem {item['url']} dropped because 'source_type' {item['source_type']} is not known")
        
        if item['status'] not in {"Not yet aired", "Currently Airing", "Finished Airing"}:
            raise DropItem(f"AnimeItem {item['url']} dropped because 'status' {item['status']} is not known")
        

        if  (item['status'] == "Not yet aired") or ('score' not in item) or ('score_count' not in item):
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
        
        if item['status'] == "Finished Airing":
            if 'num_episodes' not in item:
                raise DropItem(f"AnimeItem {item['url']} dropped because it has finished airing but 'num_episodes' is not known")
            
            if 'start_date' not in item:
                raise DropItem(f"AnimeItem {item['url']} dropped because it has finished airing but 'start_date' is not known")
            
            if item['num_episodes'] == 1:
                item['end_date'] = item['start_date']
            
            if 'end_date' not in item:
                raise DropItem(f"AnimeItem {item['url']} dropped because it has finished airing but 'end_date' is not known")

        if item['total_count'] != (item['watching_count'] + item['completed_count'] + item['on_hold_count'] + item['dropped_count'] + item['plan_to_watch_count']):
            raise DropItem(f"AnimeItem {item['url']} dropped because watch 'status_count' do not sum up to 'total_count'")
        
        sum_score_voters = sum([item['score_{:02d}_count'.format(score)] for score in range(1, 11)])
        if ('score_count' in item) and (item['score_count'] != sum_score_voters):
            raise DropItem(f"AnimeItem {item['url']} dropped because 'score_xx_count' do not sum up to 'score_count' count")
        
        average_score = sum([score * item['score_{:02d}_count'.format(score)] for score in range(1, 11)]) / sum_score_voters
        if ('score' in item) and (not isclose(item['score'] , average_score)):
            raise DropItem(f"AnimeItem {item['url']} dropped because computed average score is not close to 'score'")

        return item
        
        
        
        

