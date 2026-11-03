# pgval/app/job/: собственное задание Flink

Задание команды: Kafka `click` → конвейер → Kafka `output` / `lateOutput`. Не зависит от кода автора; версии Flink и коннектора Kafka (1.17.1) те же, что у baseline.

```bash
pgval/app/job/build.sh                                   # сборка -> pgval/app/target/job.jar (нужен только Docker)
JOB=own docker compose -f pgval/app/compose.yaml up -d --wait
JOB=own pgval/app/checks/sst-small/run.sh                # малый пример с известным ответом
```

Параметры передаются после jar как `--имя значение`, через compose — в `JOB_ARGS`, например `JOB=own JOB_ARGS="--window-ms 30000"`. Все параметры и значения по умолчанию — в [Config.java](src/main/java/p08/Config.java).

## Устройство

| Файл | Что делает |
|---|---|
| [Main.java](src/main/java/p08/Main.java) | Общее для всех конвейеров: источник и приёмники Kafka, контрольные точки, режим E1/A1, водяные знаки |
| [Config.java](src/main/java/p08/Config.java) | Параметры задания |
| [model/Click.java](src/main/java/p08/model/Click.java) | Входное событие |
| [io/Json.java](src/main/java/p08/io/Json.java) | Чтение `Click` и запись любого результата в JSON |
| [pipeline/Pipeline.java](src/main/java/p08/pipeline/Pipeline.java) | Интерфейс конвейера: поток `Click` → поток результатов (и, если нужно, опоздавших событий) |
| [pipeline/Pipelines.java](src/main/java/p08/pipeline/Pipelines.java) | Список конвейеров, выбор через `--pipeline` |
| [pipeline/PageWindow.java](src/main/java/p08/pipeline/PageWindow.java) | Эталонный конвейер с той же семантикой, что SST автора: окна по странице, в выходе идентификаторы событий |

`PageWindow` нужен, чтобы проверить каркас на тех же входах, что и baseline. Собственная топология команды — отдельный конвейер.

## Как добавить конвейер

1. Создать класс в `pipeline/`, реализующий `Pipeline`, по образцу `PageWindow`.
2. Выходные записи — классы с публичными полями, реализующие `Keyed`: поля становятся JSON, `kafkaKey()` — ключом записи.
3. Добавить строку в `Pipelines`: `ALL.put("имя", НовыйКонвейер::new);`
4. `pgval/app/job/build.sh`, затем `JOB=own JOB_ARGS="--pipeline имя"`.

Kafka, контрольные точки и режим E1/A1 при этом не меняются.

## Отличия от задания автора

- Контрольные точки (5 с) включаются в коде, а не берутся из конфигурации клиента: без них E1 не фиксирует результат и не сообщает об ошибке.
- Лишние поля во входе игнорируются (у автора — бесконечный перезапуск). Запись без `id`, `page` или `timestamp` по-прежнему останавливает задание, чтобы ошибка входа не выглядела как потеря данных.
- Стандартные `KafkaSource`/`KafkaSink` Flink вместо изменённых копий коннектора.
- В `lateOutput` поля `page`, `clickId`, `clickTimestamp` (без `updateId`, `updateTimestamp`).

## Проверено

| Проверка | Режим, сбой | Результат |
|---|---|---|
| `JOB=own pgval/app/checks/sst-small/run.sh` | E1, без сбоя | PASS: 6 окон и 1 опоздавшее событие совпадают с ответом |
| `JOB=own KILL_AFTER=12 DOWN_FOR=5 pgval/app/checks/sst-gen/run.sh --events-per-window 500 --windows 10 --rate 1000` | E1, остановка TaskManager | 60 окон: потеряно 0, повторено 0, неверно 0 |
