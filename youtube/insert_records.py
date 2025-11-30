from api_request import *

def connect_to_db():
    print('Connecting to the Postgresql db')
    try:
        conn = psycopg2.connect(
            host = DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user = DB_USER,
            password = DB_PASSWORD
        )
        return conn
    except psycopg2.Error as e:
        print(e)
        raise

def clear_tables(conn):
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE dev.raw_youtube_channels CASCADE;")
    cur.execute("TRUNCATE TABLE dev.raw_youtube_videos CASCADE;")
    cur.execute("TRUNCATE TABLE dev.sent_videos CASCADE;")
    cur.execute("TRUNCATE TABLE dev.sent_channels CASCADE;")
    conn.commit()
    cur.close()

def create_sent_videos_table(conn):
    sql = """
    CREATE TABLE IF NOT EXISTS dev.sent_videos (
        id SERIAL PRIMARY KEY,
        video_id TEXT UNIQUE,
        inserted_at TIMESTAMP DEFAULT NOW()
    );
    """
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close()

def create_sent_channels_table(conn):
    sql = """
    CREATE TABLE IF NOT EXISTS dev.sent_channels (
        id SERIAL PRIMARY KEY,
        channel_id TEXT UNIQUE,
        inserted_at TIMESTAMP DEFAULT NOW()
    );
    """
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close()

def create_analyse_videos_table(conn):
    sql = """
    CREATE TABLE IF NOT EXISTS dev.analy_channels (
        id SERIAL PRIMARY KEY,
        channel_id TEXT UNIQUE,
        inserted_at TIMESTAMP DEFAULT NOW()
    );
    """
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close()

def create_table(conn):
    sql = """
    CREATE SCHEMA IF NOT EXISTS dev;

    CREATE TABLE IF NOT EXISTS dev.raw_youtube_channels (
        id SERIAL PRIMARY KEY,
        channel_id TEXT NOT NULL UNIQUE,
        title TEXT,
        description TEXT,
        published_at TIMESTAMP,
        view_count BIGINT,
        subscriber_count BIGINT,
        video_count BIGINT,
        country TEXT,
        thumbnails JSONB,
        branding_settings JSONB,
        raw_channel JSONB,
        inserted_at TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS dev.raw_youtube_videos (
        id SERIAL PRIMARY KEY,
        topic TEXT,
        video_id TEXT NOT NULL UNIQUE,
        channel_id TEXT,
        title TEXT,
        description TEXT,
        published_at TIMESTAMP,
        tags TEXT[],
        category_id TEXT,
        duration TEXT,
        dimension TEXT,
        definition TEXT,
        caption TEXT,
        projection TEXT,
        thumbnails JSONB,
        default_language TEXT,
        default_audio_language TEXT,
        view_count BIGINT,
        like_count BIGINT,
        favorite_count BIGINT,
        comment_count BIGINT,
        raw_snippet JSONB,
        raw_content_details JSONB,
        raw_statistics JSONB,
        inserted_at TIMESTAMP DEFAULT NOW()
    );
    """
    try:
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
    except psycopg2.Error:
        conn.rollback()
        raise



def insert_channels(conn, channels):
    if not channels:
        return
    rows = []
    for ch in channels:
        snippet = ch.get("snippet", {})
        stats = ch.get("statistics", {})
        branding = ch.get("brandingSettings", {})
        channel_id = ch.get("id")
        rows.append(
            (
                channel_id,
                snippet.get("title"),
                snippet.get("description"),
                snippet.get("publishedAt"),
                _safe_int(stats.get("viewCount")),
                _safe_int(stats.get("subscriberCount")),
                _safe_int(stats.get("videoCount")),
                branding.get("country"),
                json.dumps(snippet.get("thumbnails")) if snippet.get("thumbnails") is not None else None,
                json.dumps(branding.get("channel")) if branding.get("channel") is not None else None,
                json.dumps(ch),
            )
        )

    insert_sql = """
    INSERT INTO dev.raw_youtube_channels (
        channel_id, title, description, published_at,
        view_count, subscriber_count, video_count, country,
        thumbnails, branding_settings, raw_channel
    ) VALUES %s
    ON CONFLICT (channel_id) DO NOTHING
    """

    cur = conn.cursor()
    try:
        psycopg2.extras.execute_values(cur, insert_sql, rows, template=None, page_size=100)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def insert_videos(conn, topic, videos):
    if not videos:
        return

    rows = []
    for v in videos:
        vid = v.get("id")
        snippet = v.get("snippet", {})
        stats = v.get("statistics", {})
        details = v.get("contentDetails", {})

        rows.append(
            (
                topic,
                vid,
                snippet.get("channelId"),
                snippet.get("title"),
                snippet.get("description"),
                snippet.get("publishedAt"),
                snippet.get("tags"),
                snippet.get("categoryId"),
                details.get("duration"),
                details.get("dimension"),
                details.get("definition"),
                details.get("caption"),
                details.get("projection"),
                json.dumps(snippet.get("thumbnails")) if snippet.get("thumbnails") is not None else None,
                snippet.get("defaultLanguage"),
                snippet.get("defaultAudioLanguage"),
                _safe_int(stats.get("viewCount")),
                _safe_int(stats.get("likeCount")),
                _safe_int(stats.get("favoriteCount")),
                _safe_int(stats.get("commentCount")),
                json.dumps(snippet),
                json.dumps(details),
                json.dumps(stats),
            )
        )

    insert_sql = """
    INSERT INTO dev.raw_youtube_videos (
        topic, video_id, channel_id, title, description, published_at,
        tags, category_id, duration, dimension, definition, caption, projection,
        thumbnails, default_language, default_audio_language,
        view_count, like_count, favorite_count, comment_count,
        raw_snippet, raw_content_details, raw_statistics
    ) VALUES %s
    ON CONFLICT (video_id) DO NOTHING
    """

    cur = conn.cursor()
    try:
        psycopg2.extras.execute_values(cur, insert_sql, rows, template=None, page_size=200)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()

def _safe_int(value):
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def etl_for_topic(conn, topic):
    try:
        video_ids = fetch_video_ids_by_topic(topic, SEARCH_MAX_RESULTS)
        if not video_ids:
            return

        videos = fetch_video_details(video_ids)
        if not videos:
            return

        channel_ids = list({v.get("snippet", {}).get("channelId") for v in videos if v.get("snippet", {}).get("channelId")})
        channel_ids = [cid for cid in channel_ids if cid]

        channels = fetch_channel_details(channel_ids) if channel_ids else []
        insert_channels(conn, channels)

        insert_videos(conn, topic, videos)
    except Exception as e:
        print(f"Error processing topic '{topic}': {e}")

def main():
    conn = None
    try:
        conn = connect_to_db()
        #clear_tables(conn)
        create_table(conn)
        create_sent_videos_table(conn)
        create_sent_channels_table(conn)
        create_analyse_videos_table(conn)
        for topic in TOPICS:
            etl_for_topic(conn, topic)
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if conn is not None:
            conn.close()
