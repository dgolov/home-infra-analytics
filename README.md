# Home Infrastructure Analytics

Сервис сбора и визуализации метрик с виртуальных машин для домашней инфраструктуры.

## Архитектура

Проект состоит из следующих компонентов:

- **Collector** - агент для сбора системных метрик с ВМ
- **API** - FastAPI сервис для приема и хранения метрик
- **ClickHouse** - база данных для хранения временных рядов
- **Redis** - кэш для API
- **Grafana** - панель визуализации метрик

## Собираемые метрики

Система собирает следующие типы метрик:

- **CPU** - загрузка процессора
- **RAM** - использование оперативной памяти
- **Disk** - дисковое пространство и I/O операции
- **Network** - сетевой трафик
- **Load Average** - средняя загрузка системы

## Структура данных

Метрики хранятся в ClickHouse с различными уровнями агрегации:

- `metrics_raw` - сырые данные (TTL: 2 дня)
- `metrics_1m` - агрегация за 1 минуту (TTL: 14 дней)
- `metrics_5m` - агрегация за 5 минут (TTL: 14 дней)
- `metrics_1h` - агрегация за 1 час (TTL: 14 дней)

## Быстрый старт

### Требования

- Docker и Docker Compose
- Python 3.10+ (для локальной разработки)

### Запуск с Docker Compose

1. Клонируйте репозиторий:
```bash
git clone <repository-url>
cd home-infra-analytics
```

2. Создайте файл `.env` с переменными окружения:
```bash
REDIS_PASSWORD=your_redis_password
```

3. Запустите все сервисы:
```bash
docker-compose up -d
```

Сервисы будут доступны по адресам:
- API: http://localhost:8000
- Grafana: http://localhost:3000
- ClickHouse: http://localhost:8123

### Настройка коллектора

1. Перейдите в директорию `collectors`
2. Создайте файл `.env` на основе `.env.example`
3. Установите зависимости:
```bash
pip install -r requirements.txt
```

4. Запустите сбор метрик:
```bash
python main.py
```

Для автоматического запуска добавьте в crontab:
```
* * * * * cd /path/to/collectors && python main.py
```

## API Эндпоинты

### Прием метрик

```http
POST /metrics
Content-Type: application/json

[
  {
    "date": "2024-01-01",
    "ts": "2024-01-01T12:00:00",
    "host": "server1",
    "vm": "vm1",
    "metric": "cpu_usage",
    "value": 75.5,
    "tags": {"core": "0"}
  }
]
```

### Получение метрик

```http
GET /metrics?start=2024-01-01T00:00:00&end=2024-01-02T00:00:00&metric=cpu_usage&host=server1
```

## Разработка

### Локальная разработка API

```bash
cd api
pip install poetry
poetry install
poetry run uvicorn main:app --reload
```

### Запуск тестов

```bash
cd api
poetry run pytest
```

### Линтинг и форматирование

```bash
cd api
poetry run ruff check .
poetry run mypy .
```

## Мониторинг

### Grafana

1. Откройте Grafana: http://localhost:3000
2. Добавьте datasource ClickHouse:
   - URL: http://clickhouse:8123
   - Database: infra
   - User: default
3. Импортируйте дашборды из директории `grafana/`

### Примеры запросов к ClickHouse

Получение средней загрузки CPU за последний час:
```sql
SELECT
    avg(avg_value) as avg_cpu,
    host,
    vm
FROM infra.metrics_1m
WHERE metric = 'cpu_usage'
  AND minute >= now() - INTERVAL 1 HOUR
GROUP BY host, vm
```

## Конфигурация

### Collector

- `HOST` - имя хоста для отправки метрик
- `API_URL` - URL API сервиса
- `ALLOWED_METRICS` - список разрешенных метрик
- `ENABLED_LIST` - список включенных коллекторов

### API

- `CLICKHOUSE_URL` - URL ClickHouse
- `CLICKHOUSE_USER` - пользователь ClickHouse
- `CLICKHOUSE_PASSWORD` - пароль ClickHouse
- `CLICKHOUSE_DB` - имя базы данных
- `REDIS_HOST` - хост Redis
- `REDIS_PORT` - порт Redis
- `REDIS_PASSWORD` - пароль Redis

## Лицензия

MIT License
