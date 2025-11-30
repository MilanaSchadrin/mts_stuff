from params_two import *

def connect_to_db():
    print('Connecting to the Postgresql db')
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user = DB_USER,
            password = DB_PASSWORD
        )
        return conn
    except psycopg2.Error as e:
        print(e)
        raise

def fetch_channels(conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("""
        SELECT  c.channel_id, c.title, c.description, 
               c.view_count, c.subscriber_count, c.video_count
        FROM dev.raw_youtube_channels c
        LEFT JOIN dev.analy_channels s ON c.channel_id = s.channel_id
        WHERE s.channel_id IS NULL
    """)
    rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=[desc[0] for desc in cur.description])
    cur.close()
    num_cols = ['subscriber_count', 'view_count', 'video_count']
    str_cols = ['channel_id', 'title']
    df[num_cols] = df[num_cols].fillna(0)
    df[str_cols] = df[str_cols].fillna('')
    return df

def enrich_channels(df):
    df['views_per_subscriber'] = df['view_count'] / df['subscriber_count'].replace(0, np.nan)
    df['views_per_video'] = df['view_count'] / df['video_count'].replace(0, np.nan)
    df['subscribers_per_video'] = df['subscriber_count'] / df['video_count'].replace(0, np.nan)
    df['content_efficiency'] = df['view_count'] / (df['video_count'].replace(0, np.nan) * df['subscriber_count'].replace(0, np.nan))
    df['engagement_ratio'] = df['view_count'] / df['subscriber_count'].replace(0, np.nan)
    df = df.replace([np.inf, -np.inf], np.nan).fillna(0)

    def categorize_channel_size(subscribers):
        if subscribers < 10000:
            return 'Малые (0-10K)'
        elif subscribers < 100000:
            return 'Средние (10K-100K)'
        elif subscribers < 1000000:
            return 'Крупные (100K-1M)'
        else:
            return 'Мега-каналы (1M+)'
    df['channel_size'] = df['subscriber_count'].apply(categorize_channel_size)

    def categorize_activity(videos):
        if videos < 50:
            return 'Низкая активность'
        elif videos < 200:
            return 'Средняя активность'
        else:
            return 'Высокая активность'
    df['content_activity'] = df['video_count'].apply(categorize_activity)

    df['normalized_views_per_sub'] = (df['views_per_subscriber'] - df['views_per_subscriber'].min()) / (df['views_per_subscriber'].max() - df['views_per_subscriber'].min())
    df['normalized_views_per_video'] = (df['views_per_video'] - df['views_per_video'].min()) / (df['views_per_video'].max() - df['views_per_video'].min())
    df['normalized_subs_per_video'] = (df['subscribers_per_video'] - df['subscribers_per_video'].min()) / (df['subscribers_per_video'].max() - df['subscribers_per_video'].min())

    df['composite_score'] = (
        df['normalized_views_per_sub'] * 0.4 +
        df['normalized_views_per_video'] * 0.3 +
        df['normalized_subs_per_video'] * 0.3
    )

    return df


def mark_channels_as_sent(conn, channel_ids):
    if not channel_ids:
        return
    cur = conn.cursor()
    sql = """
        INSERT INTO dev.analy_channels (channel_id)
        VALUES %s
        ON CONFLICT (channel_id) DO NOTHING
    """
    psycopg2.extras.execute_values(cur, sql, [(cid,) for cid in channel_ids])
    conn.commit()
    cur.close()

def upload_batch(records):
    records = records.to_dict(orient='records')
    payload = {
        "records": [
            {
                "fields": {
                    "Название": r["channel_id"],
                    "title": r["title"],
                    "description": r["description"],
                    "view_count": int(r["view_count"]),
                    "subscriber_count": int(r["subscriber_count"]),
                    "video_count": int(r["video_count"]),
                    "views_per_subscriber": float(r["views_per_subscriber"]),
                    "views_per_video": float(r["views_per_video"]),
                    "subscribers_per_video": float(r["subscribers_per_video"]),
                    "content_efficiency": float(r["content_efficiency"]),
                    "engagement_ratio": float(r["engagement_ratio"]),
                    "channel_size": str(r["channel_size"]),
                    "content_activity": str(r["content_activity"]),
                    "normalized_views_per_sub": float(r["normalized_views_per_sub"]),
                    "normalized_views_per_video": float(r["normalized_views_per_video"]),
                    "normalized_subs_per_video": float(r["normalized_subs_per_video"]),
                    "composite_score": float(r["composite_score"])
                }
            } for r in records
        ],
        "fieldKey": "name"
    }

    response = requests.post(API_ANALYTICS, headers=HEADERS, json=payload)
    if response.status_code not in (200, 201):
        response.raise_for_status()

def main():
    conn = connect_to_db()
    df_channels = fetch_channels(conn)
    df_channels = enrich_channels(df_channels)
    for start in range(0, len(df_channels), BATCH_SIZE):
            batch_df = df_channels.iloc[start:start+BATCH_SIZE]
            upload_batch(batch_df)
            mark_channels_as_sent(conn, batch_df['channel_id'].tolist())
    conn.close()


