import json
import time
import  psycopg2
import psycopg2.extras
import requests

YOUTUBE_API_KEY = "AIzaSyDgZPtOQD21X19L3EFGvMlA6P7uJxeuHos"
TOPICS = [
    "technology",
    "finance",
    "medicine",
    "education",
    "science",
    "law",
    "psychology",
    "marketing",
    "business",
    "engineering",
    "design",
    "music",
    "sports",
    "politics",
    "economics",
    "history",
    "gaming",
    "travel",
    "food",
    "art"
]

DB_HOST = "db"
DB_PORT = 5432
DB_NAME = "airflow_db"
DB_USER = "airflow"
DB_PASSWORD = "airflow"

SEARCH_MAX_RESULTS = 50

YT_SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"
YT_VIDEOS_URL = "https://www.googleapis.com/youtube/v3/videos"
YT_CHANNELS_URL = "https://www.googleapis.com/youtube/v3/channels"

REQUEST_DELAY_SECONDS = 2