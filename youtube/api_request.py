from params import *


def chunked_iterable(iterable, chunk_size):
    it = list(iterable)
    for i in range(0, len(it), chunk_size):
        yield it[i : i + chunk_size]

def _get_json(url, params):
    params = dict(params)
    params["key"] = YOUTUBE_API_KEY
    r = requests.get(url, params=params, timeout=30)
    if r.status_code != 200:
        r.raise_for_status()
    time.sleep(REQUEST_DELAY_SECONDS)
    return r.json()

def fetch_video_ids_by_topic(topic, max_results: int = SEARCH_MAX_RESULTS):
    params = {
        "part": "snippet",
        "q": topic,
        "type": "video",
        "maxResults": max_results,
    }
    data = _get_json(YT_SEARCH_URL, params)
    items = data.get("items", [])
    video_ids = []
    for it in items:
        vid = it.get("id", {}).get("videoId")
        if vid:
            video_ids.append(vid)
    return video_ids


def fetch_video_details(video_ids):
    all_items = []
    for chunk in chunked_iterable(video_ids, 50):
        params = {
            "part": "snippet,contentDetails,statistics",
            "id": ",".join(chunk),
            "maxResults": 50,
        }
        data = _get_json(YT_VIDEOS_URL, params)
        items = data.get("items", [])
        all_items.extend(items)
    return all_items

def fetch_channel_details(channel_ids):
    all_items = []
    for chunk in chunked_iterable(channel_ids, 50):
        params = {
            "part": "snippet,statistics,brandingSettings",
            "id": ",".join(chunk),
            "maxResults": 50,
        }
        data = _get_json(YT_CHANNELS_URL, params)
        items = data.get("items", [])
        all_items.extend(items)
    return all_items
