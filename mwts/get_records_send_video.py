from params_two import *

def fetch_videos(conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("""
        SELECT v.video_id, v.channel_id, v.topic, v.title, v.description,
               v.tags, v.category_id, v.published_at,
               v.view_count, v.like_count, v.comment_count, v.duration
        FROM dev.raw_youtube_videos v
        LEFT JOIN dev.sent_videos s ON v.video_id = s.video_id
        WHERE s.video_id IS NULL
    """)
    rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=[desc[0] for desc in cur.description])
    cur.close()
    if not df.empty:
        num_cols = ["view_count", "like_count", "comment_count", "category_id"]
        df[num_cols] = df[num_cols].fillna(0).astype(int)

        str_cols = ["video_id", "channel_id", "topic", "title", "description", "tags", "duration"]
        df[str_cols] = df[str_cols].fillna("").astype(str)
    return df

def mark_videos_as_sent(conn, video_ids):
    if not video_ids:
        return
    cur = conn.cursor()
    sql = """
        INSERT INTO dev.sent_videos (video_id)
        VALUES %s
        ON CONFLICT (video_id) DO NOTHING
    """
    psycopg2.extras.execute_values(cur, sql, [(vid,) for vid in video_ids])
    conn.commit()
    cur.close()

def upload_batch(records):
    payload = {
    "records": [
        {
            "fields": {
                "Название": str(r["video_id"]),
                "channel_id": str(r["channel_id"]),
                "topic": str(r["topic"]),
                "title": str(r["title"]),
                "description": str(r.get("description") or ""),
                "tags": str(r.get("tags") or ""),
                "category_id": int(r.get("category_id") or 0),
                "published_at": str(r.get("published_at")),
                "view_count": int(r.get("view_count") or 0),
                "like_count": int(r.get("like_count") or 0),
                "comment_count": int(r.get("comment_count") or 0),
                "duration": str(r.get("duration") or "")
            }
        } for r in records
    ],
    "fieldKey": "name"}
    response = requests.post(API_VIDEO, headers=HEADERS, data = json.dumps(payload))
    if response.status_code not in (200, 201):
        response.raise_for_status()

def main():
    conn = None
    try:
        conn = connect_to_db()
        videos = fetch_videos(conn)
        if videos.empty:
            return

        batch = []
        for _, row in videos.iterrows():
            batch.append(row)
            if len(batch) >= BATCH_SIZE:
                batch_dicts = [r.to_dict() for r in batch]
                upload_batch(batch_dicts)
                mark_videos_as_sent(conn, [r["video_id"] for r in batch_dicts])
                time.sleep(3)
                batch = []

        if batch:
            batch_dicts = [r.to_dict() for r in batch]
            upload_batch(batch_dicts)
            mark_videos_as_sent(conn, [r["video_id"] for r in batch_dicts])
    finally:
        if conn:
            conn.close()


