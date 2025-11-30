import os
import requests
import json
from flask import Flask, request, render_template, jsonify
from dotenv import load_dotenv
import datetime
from collections import defaultdict
import urllib.parse

load_dotenv()

app = Flask(__name__)

OPENROUTER_API_KEY = "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"
MWS_API_TOKEN = "uskRJrUwVfVy9PTdYVhCPoN"

YOUTUBE_API_URL = "https://tables.mws.ru/fusion/v1/datasheets/dstoLqB3Hci6MqrEKH/records?viewId=viwXBJe2lMPB4&fieldKey=name"

@app.route('/')
def home():
    return render_template('chat.html')

@app.route('/chat', methods=['POST'])
def chat():
    user_message = request.json.get('message')

    youtube_data = get_youtube_data()

    if not youtube_data or not youtube_data.get('videos'):
        return jsonify({
            'error': 'Не удалось загрузить данные из базы. Проверьте подключение и наличие данных в таблице.'
        }), 500

    return analyze_and_respond(user_message, youtube_data)

def get_youtube_data():
    try:
        headers = {
            "Authorization": f"Bearer {MWS_API_TOKEN}",
            "Content-Type": "application/json"
        }

        response = requests.get(
            YOUTUBE_API_URL,
            headers=headers,
            timeout=30
        )

        if response.status_code != 200:
            print(f"❌ Ошибка HTTP {response.status_code}: {response.text}")
            return None

        data = response.json()
        print("📊 Ответ API:", json.dumps(data, ensure_ascii=False)[:500], "...")

        if not data.get("success"):
            print("❌ API success=false")
            return None

        raw_records = data["data"].get("records", [])

        flat_records = []
        for item in raw_records:
            fields = item.get("fields", {})
            fields["recordId"] = item.get("recordId")
            flat_records.append(fields)

        if not flat_records:
            print("⚠️ База вернула 0 записей")
            return None

        channels_data = extract_channels_from_videos(flat_records)

        return {
            "videos": flat_records,
            "channels": channels_data,
            "is_real_data": True
        }

    except Exception as e:
        print(f"❌ Ошибка get_youtube_data: {e}")
        return None


def extract_channels_from_videos(videos):
    """Extract channel information from flat video list"""
    channels = {}

    for v in videos:
        channel_id = v.get("channel_id", "Неизвестный канал")

        if channel_id not in channels:
            channels[channel_id] = {
                "Название": channel_id,
                "title": channel_id,
                "description": f"Канал {channel_id}",
                "total_views": 0,
                "total_likes": 0,
                "total_comments": 0,
                "video_count": 0
            }

        channels[channel_id]["total_views"] += int(v.get("view_count", 0))
        channels[channel_id]["total_likes"] += int(v.get("like_count", 0))
        channels[channel_id]["total_comments"] += int(v.get("comment_count", 0))
        channels[channel_id]["video_count"] += 1

    return list(channels.values())


def analyze_and_respond(user_message, youtube_data):

    data_context = format_youtube_data_for_ai(youtube_data)

    analysis_prompt = create_youtube_analysis_prompt(user_message, data_context)

    try:
        response = requests.post(
            url="https://openrouter.ai/api/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                "HTTP-Referer": "https://your-flask-app.pythonanywhere.com",
                "X-Title": "YouTube Content Analyzer",
                "Content-Type": "application/json"
            },
            data=json.dumps({
                "model": "meta-llama/llama-3.3-70b-instruct:free",
                "messages": [
                    {
                        "role": "system",
                        "content": """Ты - эксперт по YouTube аналитике и контент-стратегии. Ты анализируешь реальные данные о видео и каналах.
                        Отвечай ТОЛЬКО на русском языке. Будь максимально конкретным и используй ТОЛЬКО факты из предоставленных реальных данных.
                        Не придумывай информацию. Если данных недостаточно для ответа - сообщи об этом."""
                    },
                    {
                        "role": "user",
                        "content": analysis_prompt
                    }
                ]
            }),
            timeout=30,
            verify=False
        )

        if response.status_code == 200:
            data = response.json()
            bot_reply = data['choices'][0]['message']['content']
            return jsonify({
                'reply': bot_reply,
                'analysis_type': detect_youtube_analysis_type(user_message),
                'data_used': f"📊 Проанализировано {len(youtube_data['videos'])} видео, {len(youtube_data['channels'])} каналов",
                'is_real_data': True
            })
        else:
            error_msg = f"Ошибка AI API: {response.status_code} - {response.text}"
            print(f"❌ {error_msg}")
            return jsonify({'error': f"Ошибка AI сервиса: {response.status_code}"}), 500

    except Exception as e:
        error_msg = f"Ошибка подключения к AI: {str(e)}"
        print(f"❌ {error_msg}")
        return jsonify({'error': "Ошибка подключения к AI сервису"}), 500

def format_youtube_data_for_ai(youtube_data):
    """Format YouTube data for AI analysis"""
    videos = youtube_data.get('videos', [])
    channels = youtube_data.get('channels', [])

    if not videos:
        return "В базе данных нет видео для анализа."

    channel_lookup = {channel['Название']: channel for channel in channels}

    formatted_data = "РЕАЛЬНЫЕ ДАННЫЕ YOUTUBE ИЗ БАЗЫ ДАННЫХ:\n\n"
    formatted_data += f"📊 Всего видео в базе: {len(videos)}\n"
    formatted_data += f"📺 Всего каналов в базе: {len(channels)}\n\n"

    total_views = sum(video.get('view_count', 0) for video in videos)
    total_likes = sum(video.get('like_count', 0) for video in videos)
    total_comments = sum(video.get('comment_count', 0) for video in videos)
    avg_engagement = (total_likes / total_views * 100) if total_views > 0 else 0

    formatted_data += f"📈 ОБЩАЯ СТАТИСТИКА БАЗЫ ДАННЫХ:\n"
    formatted_data += f"• Всего просмотров: {total_views:,}\n"
    formatted_data += f"• Всего лайков: {total_likes:,}\n"
    formatted_data += f"• Всего комментариев: {total_comments:,}\n"
    formatted_data += f"• Средняя вовлеченность: {avg_engagement:.2f}%\n\n"

    formatted_data += "📝 СТРУКТУРА ДАННЫХ ВИДЕО (первые 3 записи):\n"
    for i, video in enumerate(videos[:3], 1):
        formatted_data += f"Видео {i}:\n"
        for key, value in video.items():
            if value:  # Only show non-empty fields
                formatted_data += f"  {key}: {value}\n"
        formatted_data += "\n"

    videos_with_views = [v for v in videos if v.get('view_count')]
    if videos_with_views:
        top_videos = sorted(videos_with_views, key=lambda x: x.get('view_count', 0), reverse=True)[:3]
        formatted_data += "🏆 САМЫЕ ПРОСМАТРИВАЕМЫЕ ВИДЕО:\n"
        for i, video in enumerate(top_videos, 1):
            channel_name = video.get('channel_id') or video.get('channel_name') or 'Неизвестно'
            engagement_rate = (video.get('like_count', 0) / video.get('view_count', 1)) * 100
            formatted_data += f"{i}. {video.get('title', video.get('Название', 'Без названия'))}\n"
            formatted_data += f"   📺 Канал: {channel_name}\n"
            formatted_data += f"   👁️ Просмотры: {video.get('view_count', 0):,}\n"
            formatted_data += f"   👍 Лайки: {video.get('like_count', 0):,}\n"
            formatted_data += f"   💬 Комментарии: {video.get('comment_count', 0):,}\n"
            formatted_data += f"   📊 Вовлеченность: {engagement_rate:.2f}%\n\n"

    available_fields = set()
    for video in videos:
        available_fields.update(video.keys())

    formatted_data += "📋 ДОСТУПНЫЕ ПОЛЯ В БАЗЕ ДАННЫХ:\n"
    formatted_data += f"{', '.join(sorted(available_fields))}\n\n"

    formatted_data += f"📋 ПРИМЕЧАНИЕ: Все данные являются реальными и загружены из базы данных в реальном времени."

    return formatted_data

def create_youtube_analysis_prompt(user_message, data_context):
    """Create specialized prompt for YouTube analytics based on real data"""

    user_lower = user_message.lower()

    if any(word in user_lower for word in ['популярн', 'топ', 'лучш']):
        return f"""{data_context}

Пользователь спрашивает: "{user_message}"

Проанализируй ТОЛЬКО на основе предоставленных реальных данных:
1. Какие видео/каналы самые популярные и почему
2. Какие факторы влияют на популярность (тематика, длительность, вовлеченность)
3. Конкретные цифры и статистику из базы данных

Не придумывай данные! Используй только то, что есть в предоставленной статистике."""

    elif any(word in user_lower for word in ['рекомендац', 'улучшен', 'совет', 'контент-план']):
        return f"""{data_context}

Пользователь спрашивает: "{user_message}"

На основе РЕАЛЬНЫХ данных из базы предложи рекомендации:
1. Какие темы/форматы работают лучше всего
2. Как улучшить вовлеченность на основе успешных примеров
3. Конкретные шаги для улучшения контент-стратегии

Все рекомендации должны быть основаны ТОЛЬКО на реальных данных из базы."""

    elif any(word in user_lower for word in ['анализ', 'статистик', 'отчет', 'метри']):
        return f"""{data_context}

Пользователь спрашивает: "{user_message}"

Предоставь детальный анализ на основе реальных данных:
1. Ключевые метрики и показатели
2. Тренды и закономерности
3. Сравнительный анализ

Используй ТОЛЬКО факты из предоставленных данных."""

    elif any(word in user_lower for word in ['структур', 'поля', 'данные']):
        return f"""{data_context}

Пользователь спрашивает: "{user_message}"

Опиши структуру данных и доступные поля в базе.
Покажи примеры реальных записей и объясни, какую информацию можно извлечь из этих данных."""

    else:
        return f"""{data_context}

Вопрос пользователя: "{user_message}"

Ответь на основе ТОЛЬКО реальных данных из базы.
Если информации недостаточно для ответа - честно скажи об этом.
Используй конкретные цифры и факты из предоставленной статистики."""

def detect_youtube_analysis_type(user_message):
    """Detect the type of YouTube analysis requested"""
    user_lower = user_message.lower()

    analysis_types = {
        'популярн': "📊 Анализ популярности",
        'топ': "🏆 Топ видео",
        'лучш': "🏆 Лучший контент",
        'рекомендац': "💡 Рекомендации",
        'улучшен': "💡 Улучшения",
        'совет': "💡 Советы",
        'контент-план': "📋 Контент-план",
        'анализ': "📈 Анализ данных",
        'статистик': "📈 Статистика",
        'отчет': "📈 Отчет",
        'метри': "📊 Метрики",
        'тренд': "📈 Тренды",
        'канал': "📺 Анализ каналов",
        'структур': "📋 Структура данных",
        'поля': "📋 Поля данных"
    }

    for keyword, analysis_type in analysis_types.items():
        if keyword in user_lower:
            return analysis_type

    return "🎬 Анализ YouTube данных"

@app.route('/debug/db')
def debug_db():
    youtube_data = get_youtube_data()

    if youtube_data and youtube_data.get('videos'):
        return jsonify({
            'status': 'success',
            'total_videos': len(youtube_data['videos']),
            'total_channels': len(youtube_data['channels']),
            'first_video': youtube_data['videos'][0] if youtube_data['videos'] else None,
            'available_fields': list(youtube_data['videos'][0].keys()) if youtube_data['videos'] else []
        })
    else:
        return jsonify({
            'status': 'error',
            'message': 'Не удалось подключиться к базе данных или данные отсутствуют'
        }), 500

if __name__ == '__main__':
    app.run(debug=True, port=5001)