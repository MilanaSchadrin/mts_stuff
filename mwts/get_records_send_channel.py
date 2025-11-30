from params_two  import *

def fetch_new_channels(conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("""
        SELECT  c.channel_id, c.title, c.description, 
               c.view_count, c.subscriber_count, c.video_count
        FROM dev.raw_youtube_channels c
        LEFT JOIN dev.sent_channels s ON c.channel_id = s.channel_id
        WHERE s.channel_id IS NULL
    """)
    rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=[desc[0] for desc in cur.description])
    cur.close()
    if not df.empty:
        num_cols = ["view_count", "subscriber_count", "video_count"]
        df[num_cols] = df[num_cols].fillna(0).astype(int)

        str_cols = ["channel_id", "title", "description"]
        df[str_cols] = df[str_cols].fillna("").astype(str)
    return df

def mark_channels_as_sent(conn, channel_ids):
    if not channel_ids:
        return
    cur = conn.cursor()
    sql = """
    INSERT INTO dev.sent_channels (channel_id)
    VALUES %s
    ON CONFLICT (channel_id) DO NOTHING
    """
    psycopg2.extras.execute_values(cur, sql, [(cid,) for cid in channel_ids])
    conn.commit()
    cur.close()

def upload_batch(records):
    payload = {
        "records": [
            {
                "fields": {
                    "Название": r["channel_id"],
                    "title": r["title"],
                    "description": r["description"],
                    "view_count": r["view_count"],
                    "subscriber_count": r["subscriber_count"],
                    "video_count": r["video_count"],
                }
            } for r in records
        ],
        "fieldKey": "name"
    }
    response = requests.post(API_CHANNELS, headers=HEADERS, data=json.dumps(payload))
    if response.status_code not in (200, 201):
        response.raise_for_status()

def main():
    conn = None
    try:
        conn = connect_to_db()
        channels = fetch_new_channels(conn)
        if channels.empty:
            return

        batch = []
        for _, row in channels.iterrows():
            batch.append(row)
            if len(batch) >= BATCH_SIZE:
                batch_dicts = [r.to_dict() for r in batch]
                upload_batch(batch_dicts)
                mark_channels_as_sent(conn, [r["channel_id"] for r in batch_dicts])
                time.sleep(3)
                batch = []

        if batch:
            batch_dicts = [r.to_dict() for r in batch]
            upload_batch(batch_dicts)
            mark_channels_as_sent(conn, [r["channel_id"] for r in batch_dicts])
    finally:
        if conn:
            conn.close()

