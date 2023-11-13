import os
import requests
import xmltodict



# from task 1 : get episodes : Request data and convert to dict
def getEpisodesRequests():
    data = requests.get("https://www.marketplace.org/feed/podcast/marketplace/")
    feed = xmltodict.parse(data.text)
    episodes = feed["rss"]["channel"]["item"]
    
    return episodes


# from task 4 : download episodes : open file and append the podcasts in
def download_episodes(episodes):
    audio_files = []
    for episode in episodes:
        name_end = episode["link"].split('/')[-1]
        filename = f"{name_end}.mp3"
        audio_path = os.path.join('episodes', filename)
        
        # Create the directory if it doesn't exist
        os.makedirs('episodes', exist_ok=True)
        
        if not os.path.exists(audio_path):
            print(f"Downloading {filename}")
            audio = requests.get(episode["enclosure"]["@url"])
            with open(audio_path, "wb+") as f:
                f.write(audio.content)
        audio_files.append({
            "link": episode["link"],
            "filename": filename
        })