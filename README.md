# proview-event-ingester

Загрузчик событий **ProView** из Kafka в ClickHouse. Владеет схемой хранилища: сырые события и база-агрегатор `proview_agg`, которую обновляют материализованные представления.

Часть платформы: [документация](https://github.com/DemonIngvi/proview-docs) · [развёртывание](https://github.com/DemonIngvi/proview-deploy).

## Как работает — at-least-once с идемпотентной вставкой

1. Опрос Kafka до `BATCH_SIZE` записей или `FLUSH_INTERVAL`. На это время ребаланс заблокирован (`BlockRebalanceOnPoll`), поэтому пачка не пересекает смену владельца партиции.
2. Разбор. Невалидные сообщения уходят в DLQ `proview.events.dlq` с заголовком `error`.
3. Вставка одним блоком с `insert_deduplication_token`. Токен — хеш диапазонов смещений, детерминированный для одной и той же пачки. Повторы идут с экспоненциальной паузой, пока вставка не пройдёт.
4. Коммит смещений, и только после него разрешение ребаланса.

Если ClickHouse недоступен, события копятся в Kafka (хранение 7 дней) и загружаются после восстановления. Повтор той же пачки ClickHouse отбрасывает, включая зависимые MV. Это проверено интеграционным тестом и chaos-тестом в `proview-deploy`.

## Схема

| Таблица | Движок | Хранение | |
|---|---|---|---|
| `proview.events_raw` | MergeTree, партиция по дню | 30 дней | сырые события для разборов и пересчёта |
| `proview_agg.banner_stats_1m` | AggregatingMergeTree | 14 дней | счётчики, суммы длительностей, `uniqState(visitor_id)` по минуте |
| `proview_agg.banner_stats_1h` | AggregatingMergeTree | 730 дней | то же по часу |
| `proview_agg.mv_banner_stats_{1m,1h}` | Materialized View | — | обновляют агрегаты при каждой вставке |

Измерения агрегатов: `event_type, banner_id, placement, country, device`.

`event-ingester migrate` создаёт базы с движком из `CLICKHOUSE_DB_ENGINE`: `Atomic` на одном узле или `Replicated('/clickhouse/databases/{db}', '{shard}', '{replica}')` в кластере, тогда все MergeTree реплицируются. Затем применяются идемпотентные миграции и записываются в `proview.schema_migrations`.

## Конфигурация

| Переменная | По умолчанию | |
|---|---|---|
| `KAFKA_BROKERS`, `KAFKA_TOPIC`, `KAFKA_DLQ_TOPIC`, `KAFKA_GROUP` | —, `proview.events.v1`, `proview.events.dlq`, `proview-event-ingester` | |
| `KAFKA_TLS`, `KAFKA_SASL_*` | выкл. | |
| `CLICKHOUSE_ADDRS`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, `CLICKHOUSE_TLS` | —, `default`, —, `false` | |
| `CLICKHOUSE_DB_ENGINE` | `Atomic` | см. выше |
| `BATCH_SIZE` / `FLUSH_INTERVAL` | `20000` / `1s` | ClickHouse любит редкие крупные вставки |
| `MAX_BACKOFF` | `30s` | потолок паузы между повторами |

Масштабирование: число реплик не больше числа партиций (24). В чарте есть HPA по CPU или KEDA `ScaledObject` по лагу группы потребителей.

## Метрики

`proview_ingester_records_total{outcome}` (inserted / invalid), `proview_ingester_insert_duration_seconds`, `proview_ingester_batch_rows`, `proview_ingester_errors_total{stage}` (poll / insert / dlq / commit), `proview_ingester_last_commit_timestamp_seconds` (алерт на остановку загрузки). Лаг смотрится через экспортёр Kafka (Strimzi Kafka Exporter).

## Разработка

```bash
make test
PROVIEW_TEST_CLICKHOUSE=localhost:9000 PROVIEW_TEST_CLICKHOUSE_PASSWORD=pw make test-integration
make lint docker helm-lint
```

Интеграционный тест на настоящем ClickHouse проверяет идемпотентность миграций, дедупликацию повторной вставки в raw-таблице и в обоих агрегатах, а также корректность `uniqMerge`.

Лицензия: Apache-2.0.
