# YouTube Data Pipeline с Airflow и Docker

## Описание

Система предназначена для автоматизированного извлечения, обработки и хранения данных из YouTube с использованием **YouTube Data API** и **Apache Airflow**.

Она обеспечивает устойчивость к ошибкам, масштабируемость и мониторинг выполнения задач через Airflow Web UI.

---

## Версии компонентов

* **PostgreSQL**: 14.18
* **Apache Airflow**: 3.1.3
* **Docker Engine**: 24.x (рекомендуется последняя стабильная версия)
* **Python**: 3.12 (используется внутри контейнера Airflow)

> Важно: Python 3.12 совместим с Airflow 3.x
---
## Структура

mts_stuff/
├── docker-compose.yml
├── requirements.txt
├── airflow/
│   └── dags/
│       └── orchestrator.py
├── youtube/
│   ├── insert_records.py
│   ├── api?request.py
│   └── params.py
├── mwts/
│   ├── params_two.py
├── analytics.py
│   ├── get_records_send_video.py
│   └── get_records_send_channel.py
├── postgres/
│   ├── data/
│   └── airflow_init.sql

## Конфигурация

### Параметры API

* **YOUTUBE_API_KEY**: ключ доступа к YouTube Data API.
* **REQUEST_DELAY_SECONDS**: задержка между запросами к API для предотвращения превышения лимитов.
* **SEARCH_MAX_RESULTS**: максимальное количество результатов поиска за один запрос.

### Настройки базы данных

* **Хост**: `db`
* **Порт**: `5432`
* **База данных**: `airflow_db`
* **Пользователь**: `airflow`
* **Пароль**: `airflow`

### Мониторинг и логирование

* **Airflow UI** доступен по адресу `http://localhost:8000`.
* Логи задач можно просматривать через **Airflow Web UI**.
* Система обрабатывает ошибки API и продолжает работу при временных сбоях.

---

## Особенности реализации

* **Пакетная обработка**: оптимизация работы с API и базой данных.
* **Обработка ошибок**: устойчивость к временным сбоям API, использование повторных попыток и задержек.
* **Отслеживание состояния**: предотвращение дублирования данных.
* **Масштабируемость**: легко добавлять новые тематики и метрики, поддержка параметризации DAG.

---

---

## Подготовка и запуск

1. **Клонируйте репозиторий**:

   ```bash
   git clone <репозиторий>
   cd <репозиторий>
   ```

2. **Создайте необходимые папки и файлы**:

   ```text
   ./postgres/data/                # для хранения данных Postgres
   ./postgres/airflow_init.sql     # начальная инициализация БД
   ./airflow/dags/                 # DAG файлы Airflow
   ./youtube/                      # пакеты с кодом для YouTube API
   ./mwts/                         # пакеты с кодом для mws tables
   ./requirements.txt              # список зависимостей Python
   ```
3. **Создайте виртуальное окружение Python и активируйте его**:
  ```bash
  python3.12 -m venv venv
  source venv/bin/activate   # Linux/macOS
  .\venv\Scripts\activate.bat  # Windows cmd
  .\venv\Scripts\Activate.ps1  # Windows PowerShell
  ```
4.**Установите зависимости**:
```bash
  pip install --upgrade pip
  pip install -r requirements.txt
```
5. **Запустите контейнеры**:

   ```bash
   docker-compose up -d
   ```

6. **Откройте Airflow Web UI**:

   ```
   http://localhost:8000
   ```
   Теоретически, автоматическое обновление для главных таблиц уже настроено на каждые 15 минут, а для аналитических на каждые 40, но airfow позволяет manually запустить

7. **Остановка контейнеров**:

   ```bash
   docker-compose down
   ```

---

