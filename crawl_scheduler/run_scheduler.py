import os
import json
import requests
import time
import google.oauth2.id_token
import google.auth.transport.requests

profile_url = "https://us-central1-anime-rec-dev.cloudfunctions.net/profile_crawl_scheduler"
params = {
    "max_num_urls" : 100
}

t0 = time.time()
i = 0
while i < 500:


    request = google.auth.transport.requests.Request()
    PROFILE_TOKEN = google.oauth2.id_token.fetch_id_token(request, profile_url)

    r = requests.post(
        profile_url, 
        headers={'Authorization': f"Bearer {PROFILE_TOKEN}", "Content-Type": "application/json"},
        data=json.dumps(params)    
    )
    print("profile : ", i, ": ", r)
    
    i = i + 1
    time.sleep(300)
    

