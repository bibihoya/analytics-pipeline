#!/usr/bin/env bash
set -euo pipefail

# Запуск из корня проекта в WSL: bash e2e.sh.
# Нужны Bash, curl, GNU date и Docker Compose; используются настройки .env по умолчанию.
# Перед ручным сценарием поднимите стенд и соберите обязательный интеграционный тест:
# docker compose up --build -d
# docker compose --profile test run --build --rm integration-test

# Берём UTC-дату 30 дней назад: генератор и плановый пересчёт её не затрагивают.
# Новый пользователь изолирует события этого запуска; повторные запуски добавят новых пользователей.
day=$(date -u -d '30 days ago' +%F)
base_seconds=$(date -u -d "$day 12:00:00" +%s)
user="manual-$(cat /proc/sys/kernel/random/uuid)"

# Отправляем по началу и завершению просмотра в Day 0, Day 1 и Day 7.
# В ответах Movie Service смотрим event_id: всего должно быть шесть успешных публикаций.
for offset in 0 1 7; do
    session=$(cat /proc/sys/kernel/random/uuid)
    for kind in VIEW_STARTED VIEW_FINISHED; do
        progress=0
        if [[ "$kind" == VIEW_FINISHED ]]; then
            progress=5400
        fi
        event_id=$(cat /proc/sys/kernel/random/uuid)
        timestamp=$(date -u -d "@$((base_seconds + offset * 86400 + progress))" +%FT%T.000Z)
        curl -fsS 'http://localhost:18080/events' \
            -H 'Content-Type: application/json' --data-binary @- <<JSON
{
    "event_id": "$event_id",
    "user_id": "$user",
    "movie_id": "manual-movie",
    "event_type": "$kind",
    "timestamp": "$timestamp",
    "device_type": "DESKTOP",
    "session_id": "$session",
    "progress_seconds": $progress
}
JSON
    done
done

# Ждём доставки всех шести событий через Kafka в ClickHouse, максимум 60 секунд.
# При недоставке сценарий завершается с ошибкой; детали смотрим в логах movie-service и clickhouse.
query="SELECT count() FROM cinema.raw_events FINAL WHERE user_id = '$user' FORMAT TabSeparated"
deadline=$((SECONDS + 60))
stored=0
while ((SECONDS < deadline)); do
    stored=$(curl -fsS -u default:cinema \
        -H 'Content-Type: text/plain' --data-binary "$query" 'http://127.0.0.1:18123/')
    if [[ "$stored" == 6 ]]; then
        break
    fi
    sleep 0.5
done
if [[ "$stored" != 6 ]]; then
    printf 'Не все события дошли до ClickHouse: %s из 6\n' "$stored" >&2
    exit 1
fi

# Запускаем пересчёт за дату первой активности и выводим готовые метрики из PostgreSQL.
# Смотрим: average_watch_seconds=5400, view_conversion=1, retention_d1=1, retention_d7=1.
# У cohort_retention значения для дней 0, 1 и 7 равны 1; при первом запуске на этой дате dau=1.
# Доли 1 соответствуют 100%; в логах aggregation-service видны число событий и длительность расчёта.
curl -fsS -X POST "http://localhost:18081/aggregate?date=$day"
docker compose exec -T postgres psql -U cinema -d cinema \
    -c "SELECT * FROM daily_metrics WHERE date = '$day' ORDER BY metric, dimension;"

# Проверяем идемпотентность: после второго пересчёта count(*) тот же, а computed_at обновляется.
docker compose exec -T postgres psql -U cinema -d cinema \
    -c "SELECT count(*), max(computed_at) FROM daily_metrics WHERE date = '$day';"
curl -fsS -X POST "http://localhost:18081/aggregate?date=$day"
docker compose exec -T postgres psql -U cinema -d cinema \
    -c "SELECT count(*), max(computed_at) FROM daily_metrics WHERE date = '$day';"

# Дважды экспортируем метрики из PostgreSQL в один S3-объект.
# В MinIO Console (http://localhost:19001, minioadmin/minioadmin) открываем указанный ниже путь.
# Смотрим: объект один, внутри JSON есть дата, все метрики и computed_at у каждой записи.
curl -fsS -X POST "http://localhost:18081/export?date=$day"
curl -fsS -X POST "http://localhost:18081/export?date=$day"
printf 'MinIO: movie-analytics/daily/%s/aggregates.json\n' "$day"

# В Grafana (admin/admin) смотрим Retention Cohort Heatmap, DAU и View conversion.
# Исторические когорты генератора видны за последние 14 дней; для ручной когорты расширьте диапазон до 30 дней назад.
printf 'Grafana: http://localhost:13000/d/cinema-analytics\n'

# Останавливаем один брокер и повторяем обязательный тест доставки.
# --no-deps оставляет kafka-1 остановленным; в выводе теста ожидаем PASS.
# trap возвращает брокер и при ошибке теста, чтобы не оставлять стенд без узла.
docker compose stop kafka-1
trap 'docker compose start kafka-1' EXIT
docker compose --profile test run --no-deps --rm integration-test
docker compose start kafka-1
trap - EXIT

# Смотрим описание топика: 3 партиции, replication factor 3, min.insync.replicas=2.
# После синхронизации в Isr каждой партиции должны быть все три брокера; при необходимости повторите команду.
docker compose exec -T kafka-2 kafka-topics \
    --bootstrap-server kafka-2:9092 --describe --topic movie-events
