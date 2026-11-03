# WB_Tech — сервис обработки заказов с Kafka

Сервис принимает заказы в формате JSON через Apache Kafka, сохраняет их в PostgreSQL, кэширует в памяти и отдает данные по запросу через тот же брокер.

## Архитектура

Один сервис `wb_back`, который:
- читает сообщения из Kafka (топик `orders`);
- маршрутизирует их по ключу: `Add` — добавление заказа, `Get` — запрос по UID;
- сохраняет заказы в PostgreSQL (4 таблицы: `orders`, `delivery`, `payment`, `items`);
- кэширует заказы в LRU-кэше (in-memory, до 1000 записей);
- отправляет ответы обратно в Kafka с ключом `Send`.

**Схема потока:**

[Клиент] --(Add, JSON)--> [Kafka: orders] --> [wb_back: Add-консьюмер] --> [PostgreSQL + кэш]
[Клиент] --(Get, UID)--> [Kafka: orders] --> [wb_back: Get-консьюмер] --> [кэш / PostgreSQL] --> [Kafka: Send] --> [Клиент]

## Технологический стек

- **Язык:** Go 1.24
- **Брокер сообщений:** Apache Kafka 4.0.0 (KRaft, без ZooKeeper)
- **Kafka-клиент:** `segmentio/kafka-go v0.4.49`
- **База данных:** PostgreSQL (`lib/pq v1.10.9`)
- **Кэш:** собственный LRU-кэш на `container/list` + `sync.RWMutex`
- **Конфигурация:** переменные окружения (`os.Getenv`)
- **Контейнеризация:** Docker, Docker Compose
- **Логирование:** стандартный `log`

## Особенности реализации

- **Один топик, три ключа.** Все сообщения идут в топик `orders`, но маршрутизируются по ключу: `Get`, `Add`, `Send`. Это упрощает инфраструктуру: не нужно создавать отдельные топики под каждый тип сообщения.
- **Два консьюмера с разными consumer group.** `get-consumer-group` и `add-consumer-group` читают один топик, но фильтруют сообщения по ключу. Это позволяет независимо масштабировать обработку добавления и чтения.
- **LRU-кэш с прогреванием.** При старте кэш заполняется данными из БД (`GetUIDs` → `GetOrderUID` → `CreateJsonOrder`). При переполнении удаляется 50% самых старых записей.
- **Транзакционная запись.** Добавление заказа выполняется в транзакции: `DELETE` старых записей + `INSERT` новых в 4 таблицы. При ошибке на любом шаге — `ROLLBACK`.
- **Graceful shutdown.** Сервис перехватывает `SIGINT`/`SIGTERM`, отменяет контекст, закрывает каналы и дожидается завершения горутин.
- **Валидация на уровне моделей.** `Order.Validate()` проверяет обязательные поля, дату (не в будущем), вложенные `Delivery`, `Payment`, `Items`.

## Запуск

### Требования

- Docker и Docker Compose
- PostgreSQL (локально или в контейнере — в compose его нет)

### Шаги

1. Склонируйте репозиторий:
   ```bash
   git clone https://github.com/ChristmasBlue/WB_Tech.git
   cd WB_Tech
   ```

2. Создайте .env на основе .env.example:
    ```bash
    cp .env.example .env
    ```
    
Отредактируйте .env: укажите параметры подключения к PostgreSQL (DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME).

3. Запустите PostgreSQL (если он не запущен) и создайте таблицы:
    ```sql
   CREATE TABLE orders (...);
    CREATE TABLE delivery (...);
    CREATE TABLE payment (...);
    CREATE TABLE items (...);
    ```
(Схему таблиц можно посмотреть в service/service.go — там все INSERT-запросы с полями.)

4. Запустите сервисы:
   ```bash
   docker compose up --build
   ```

5. Сервис wb_back слушает localhost:8080, Kafka — localhost:9092


###Важно: KAFKA_BROKERS

Для локального запуска (без Docker) укажите KAFKA_BROKERS=localhost:9092.

Для запуска в Docker Compose укажите KAFKA_BROKERS=kafka:9092 — внутри сети app-net Kafka доступна по имени kafka.

##Отправка сообщений
###Добавление заказа (ключ Add)
Отправьте JSON заказа в топик orders с ключом Add. Пример через kafka-console-producer:
```bash
    docker exec -it kafka_kraft /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --property "parse.key=true" \
  --property "key.separator=:"
```

Затем введите:
```bash
    Add:{"order_uid":"...","track_number":"...","delivery":{...},"payment":{...},"items":[...]}
```

###Запрос заказа (ключ Get)

Отправьте UID заказа с ключом Get:

```text
Get:b563f740-6c1f-4f9a-9c1f-1a2b3c4d5e6f
Ответ придет в топик orders с ключом Send в формате JSON.
```

##Структура проекта

```text
WB_Tech/
├── WB_back/
│   ├── main.go
│   ├── cache/        # LRU-кэш
│   ├── config/       # загрузка переменных окружения
│   ├── consumer/     # два консьюмера Kafka
│   ├── models/       # структуры и валидация
│   ├── parser/       # JSON-сериализация
│   ├── producer/     # продюсер Kafka
│   └── service/      # работа с PostgreSQL
├── docker-compose.yaml
├── .env.example
└── README.md
```

###Ограничения и возможные улучшения

Одностадийная сборка Docker. Образ включает Go-тулчейн и исходники, поэтому он большой. В продакшне — multi-stage build.

BatchSize: 1 у продюсера. Для простоты отладки. В продакшне — батчинг для пропускной способности.

Нет UPSERT. Запись через DELETE + INSERT. В продакшне — ON CONFLICT DO UPDATE и уникальный индекс на order_uid.

Нет retry/DLQ. При ошибке обработки сообщение логируется и пропускается. В продакшне — retry с backoff и dead letter queue.

Авто-коммит offset. Явный CommitMessages не настроен. Для учебного проекта достаточно.


### Что важно проверить перед публикацией

1. **Схема таблиц.** В README я не привел полный `CREATE TABLE` — только сказал, что схему можно посмотреть в `service.go`. Если хотите, можно добавить полные SQL-запросы (они есть в `insertOrder`, `insertDelivery`, `insertPayment`, `insertItems`).
2. **`KAFKA_BROKERS`.** Явно разделите локальный запуск и Docker — это самая частая ошибка при запуске.
3. **PostgreSQL не в compose.** Это важно: читатель должен знать, что БД нужно поднять отдельно.
4. **`WB_front` удален.** В README нет упоминания фронта — это правильно, потому что его нет в коде.


### Что дальше

Если черновик устраивает — можно скопировать в `README.md`. Если хотите что-то изменить (добавить схему таблиц, убрать раздел «Ограничения», переписать «Архитектуру» проще) — скажите, поправлю.

После README можно вернуться к **стеку технологий для резюме** — на основе фактов из кода, без выдумок.
