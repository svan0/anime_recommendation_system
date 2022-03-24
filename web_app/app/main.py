import flask
from flask import Flask
from flask import render_template
from flask import redirect

from google.cloud import pubsub
import redis

import os
import json
from random import sample
from datetime import datetime
from dotenv import load_dotenv

app = Flask(__name__)

@app.before_first_request
def startup_and_load_data():

    load_dotenv()

    global redis_client
    redis_host = os.getenv('REDIS_INSTANCE_HOST')
    redis_port = int(os.getenv('REDIS_INSTANCE_PORT'))
    redis_client = redis.Redis(
        host=redis_host,
        port=redis_port
    )

    global publish_client
    global topic_path
    publish_client = pubsub.PublisherClient()
    project = os.getenv('PROJECT_ID')
    ingestion_topic = os.getenv('PUBSUB_TOPIC')
    topic_path = publish_client.topic_path(project, ingestion_topic)

def push_message_to_pub_sub(
    datetime = None,
    user_id = None,
    event_type = None,
    event_on = None
):
    message = {
        'datetime': datetime,
        'user_id': user_id,
        'event_type': event_type,
        'event_on': event_on 
    }

    pubsub_message = json.dumps(message).encode("utf-8")
    publish_client.publish(topic_path, pubsub_message)
    app.logger.info(f"Message {message['event_type']} pushed to {topic_path}")

def sample_recommendations(list_recs):
    if len(list_recs) > 5:
        list_recs = sample(list_recs, 5)
    return list_recs

@app.route("/", methods=["GET"])
def index():
    current_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    push_message_to_pub_sub(
        datetime=current_time,
        event_type = 'enter_index'
    )
    return render_template("index.html")


@app.route("/like", methods=["GET"])
def like():
    current_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    user_id = flask.request.args.get('user_id')
    push_message_to_pub_sub(
        datetime = current_time,
        user_id = user_id, 
        event_type = f"like_{flask.request.args.get('rec_type')}"
    )
    return redirect(f"/recs?user_id={user_id}")

@app.route("/dislike", methods=["GET"])
def dislike():
    current_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    user_id = flask.request.args.get('user_id')
    push_message_to_pub_sub(
        datetime = current_time,
        user_id = user_id, 
        event_type = f"dislike_{flask.request.args.get('rec_type')}"
    )
    return redirect(f"/recs?user_id={user_id}")

@app.route("/go_to", methods=["GET"])
def go_to():
    current_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    user_id = flask.request.args.get('user_id')
    url = flask.request.args.get('url')
    push_message_to_pub_sub(
        datetime = current_time,
        user_id = user_id, 
        event_type = 'go_to',
        event_on=url
    )
    return redirect(url)

@app.route("/recs", methods=["GET", "POST"])
def recommendations():

    current_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    if flask.request.method == 'POST':
        user_id = flask.request.values.get('user_id')
    else:
        user_id = flask.request.args.get('user_id')

    user_id = user_id.lower()
    
    push_message_to_pub_sub(
        datetime=current_time,
        user_id = user_id, 
        event_type = 'enter_rec'
    )

    user_anime_recs = redis_client.lrange(f"{user_id}_user_anime_recs", 0, -1)
    user_anime_recs = list(map(lambda x : x.decode("utf8"), user_anime_recs))
    user_anime_recs = sample_recommendations(user_anime_recs)
    user_anime_recs = list(map(lambda anime_id: {
        'name' : redis_client.get(f"{anime_id}_name").decode("utf8"),
        'url' : redis_client.get(f"{anime_id}_url").decode("utf8"),
        'img_url': redis_client.get(f"{anime_id}_img_url").decode("utf8")
    }, user_anime_recs))

    for rec in user_anime_recs:
        push_message_to_pub_sub(
            datetime=current_time,
            user_id = user_id, 
            event_type = 'user_anime_rec', 
            event_on = rec['url']
        )
    
    recent_watch = redis_client.get(f"{user_id}_recent_watch")
    if recent_watch is not None:
        recent_watch = recent_watch.decode("utf8")
        recent_watch = {
            'name' : redis_client.get(f"{recent_watch}_name").decode("utf8"),
            'url' : redis_client.get(f"{recent_watch}_url").decode("utf8"),
            'img_url': redis_client.get(f"{recent_watch}_img_url").decode("utf8")
        }
    
    anime_anime_recs = redis_client.lrange(f"{user_id}_anime_anime_recs", 0, -1)
    anime_anime_recs = list(map(lambda x : x.decode("utf8"), anime_anime_recs))
    anime_anime_recs = sample_recommendations(anime_anime_recs)
    anime_anime_recs = list(map(lambda anime_id: {
        'name' : redis_client.get(f"{anime_id}_name").decode("utf8"),
        'url' : redis_client.get(f"{anime_id}_url").decode("utf8"),
        'img_url': redis_client.get(f"{anime_id}_img_url").decode("utf8")
    }, anime_anime_recs))

    for rec in anime_anime_recs:
        push_message_to_pub_sub(
            datetime=current_time,
            user_id = user_id, 
            event_type = 'anime_anime_rec', 
            event_on = rec['url']
        )

    return render_template(
        'recs.html', 
        user_id = user_id, 
        user_anime_recs = user_anime_recs,
        recent_watch = recent_watch,
        anime_anime_recs = anime_anime_recs
    )

if __name__ == '__main__':
    app.run(debug=True)
    

        
