import re
from dateutil.parser import parse as dateutil_parse
from dateutil.relativedelta import relativedelta
import datetime


def none_text(text):
    if text in {"add some", "None found", "N/A", "Unknown", "Not available"}:
        return None
    return text

def get_past_date(str_days_ago):
    TODAY = datetime.date.today()
    splitted = str_days_ago.split()
    
    if len(splitted) == 1 and splitted[0].lower() == 'now':
        return datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    
    elif splitted[0].lower() == 'today':
        if len(splitted) > 1:
            TODAY = dateutil_parse(" ".join(splitted[1:]))
        return TODAY.strftime("%Y-%m-%dT%H:%M:%S")
    
    elif splitted[0].lower() == 'yesterday,':
        if len(splitted) > 1:
            TODAY = dateutil_parse(" ".join(splitted[1:]))
        date = TODAY - relativedelta(days=1)
        return date.strftime("%Y-%m-%dT%H:%M:%S")
    
    elif splitted[1].lower() in ['hour', 'hours', 'hr', 'hrs', 'h']:
        date = datetime.datetime.now() - relativedelta(hours=int(splitted[0]))
        return date.strftime("%Y-%m-%dT%H:%M:%S")

    elif splitted[1].lower() in ['day', 'days', 'd']:
        date = TODAY - relativedelta(days=int(splitted[0]))
        return date.strftime("%Y-%m-%dT%H:%M:%S")
    
    elif splitted[1].lower() in ['wk', 'wks', 'week', 'weeks', 'w']:
        date = TODAY - relativedelta(weeks=int(splitted[0]))
        return date.strftime("%Y-%m-%dT%H:%M:%S")
    
    elif splitted[1].lower() in ['mon', 'mons', 'month', 'months', 'm']:
        date = TODAY - relativedelta(months=int(splitted[0]))
        return date.strftime("%Y-%m-%dT%H:%M:%S")
    
    elif splitted[1].lower() in ['yrs', 'yr', 'years', 'year', 'y']:
        date = TODAY - relativedelta(years=int(splitted[0]))
        return date.strftime("%Y-%m-%dT%H:%M:%S")
    
    else:
        raise Exception()

def get_last_online_date(text):
    try:
        return get_past_date(text)
    except:
        try:
            return parse_date(text)
        except:
            return None

def get_url(url):
    if "myanimelist.net" not in url:
        return f"https://myanimelist.net{url}"
    return url

def no_synopsis(text):
    if "No synopsis information has been added to this title." in text:
        return None
    return text

def clean_text(text):
    text = re.sub("\s{2,}", "", text)
    text = re.sub("\[Written by MAL Rewrite\]","",text)
    text = text.strip()
    return text

def get_anime_id(anime_id):
    # If anime_id is the URL to the anime, extract the id
    # else return the anime_id
    try:
        return anime_id.split('/anime/')[1].split('/')[0]
    except:
        return anime_id

def get_user_id(user_id):
    # If user_id is the URL to the user, extract the id
    # else return the user_id
    try:
        return user_id.split('/profile/')[1].split('/')[0]
    except:
        try:
            return user_id.split('type=rw&u=')[1]
        except:
            return user_id

def get_club_id(club_id):
    # If club_id is the URL to the club, extract the id
    # else return the club_id
    try:
        return club_id.split('?cid=')[1]
    except:
        return club_id

def get_review_id(review_id):
    # If review_id is the URL to the club, extract the id
    # else return the review_id
    try:
        return review_id.split('?id=')[1]
    except:
        return review_id

def parse_date(text):
    try:
        return dateutil_parse(text).strftime("%Y-%m-%dT%H:%M:%S")
    except Exception as e:
        return None

def transform_to_int(text):
    if type(text) == str:
        text = re.sub("[^0-9]", "", text)
        try:
            return int(text)
        except:
            return None
    return text

def transform_to_float(text):
    if type(text) == str:
        text = re.sub("[,]", ".", text)
        text = re.sub("[^0-9.]", "", text)
        try:
            return float(text)
        except:
            return None
    return text

def extract_start_date(text):
    try:
        text = text.split('to')[0]
        return parse_date(text)
    except Exception as e:
        return None

def extract_end_date(text):
    try:
        text = text.split('to')[1]
        return parse_date(text)
    except Exception as e:
        return None

def get_activity_type(text):
    if "Watching" in text:
        return "watching"
    if "Completed" in text:
        return "completed"
    if "On-Hold" in text:
        return "on_hold"
    if "Dropped" in text:
        return "dropped"
    if "Plan to Watch" in text:
        return "plan_to_watch"
    return None