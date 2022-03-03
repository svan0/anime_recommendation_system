from flask import Flask
from flask import render_template
import json
from random import sample

app = Flask(__name__)

with open('data/anime_info.json') as f:
    anime_info = json.load(f)

with open('data/anime_anime.json') as f:
    anime_anime = json.load(f)

with open('data/user_anime.json') as f:
    user_anime = json.load(f)
    
@app.route("/<user_id>")
def index(user_id):
    user_anime_recs = []
    if user_id in user_anime:
        for rec in user_anime[user_id]:
            user_anime_recs.append(anime_info[rec])
    
    if len(user_anime_recs) > 5:
        user_anime_recs = sample(user_anime_recs, 5)

    recent_watch = None
    anime_anime_recs = []
    if user_id in anime_anime:
        for rec in anime_anime[user_id]["recommendations"]:
            anime_anime_recs.append(anime_info[rec])
        recent_watch = anime_info[anime_anime[user_id]["recent_watch"]]

    if len(anime_anime_recs) > 5:
        anime_anime_recs = sample(anime_anime_recs, 5)

    return render_template(
        'index.html', 
        user_id = user_id, 
        user_anime_recs = user_anime_recs,
        recent_watch = recent_watch,
        anime_anime_recs = anime_anime_recs
    )

if __name__ == '__main__':
    app.run(debug=True)
    

        
