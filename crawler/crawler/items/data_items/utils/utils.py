import re
from dateutil.parser import parse as dateutil_parse

def get_url(url):
    if "myanimelist.net" not in url:
        return f"https://myanimelist.net{url}"
    return url

def clean_text(text):
    text = re.sub("\s{2,}", "", text)
    text = re.sub("\[Written by MAL Rewrite\]","",text)
    text = text.strip()
    return text

def get_anime_id(anime_id):
    # If anime_id is the URL to the anime, extract the id
    # else return the anime_id
    try:
        return anime_id.split('anime/')[1].split('/')[0]
    except:
        return anime_id

def get_user_id(user_id):
    # If user_id is the URL to the user, extract the id
    # else return the user_id
    try:
        return user_id.split('profile/')[1].split('/')[0]
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
    text = re.sub("[^0-9]", "", text)
    try:
        return int(text)
    except:
        return None

def transform_to_float(text):
    text = re.sub("[,]", ".", text)
    text = re.sub("[^0-9.]", "", text)
    try:
        return float(text)
    except:
        return None

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