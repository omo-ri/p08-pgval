# Генератор в Docker

Одноразовый сервис `generator` создаёт план и события, после чего завершается.
Он использует общий с валидатором образ `pgval:local`. Kafka и Docker-сокет
внутри контейнера не нужны; сеть отключена.

Из корня репозитория:

```bash
mkdir -p runs reports
export PGVAL_UID=$(id -u) PGVAL_GID=$(id -g)
docker compose -f pgval/compose.yaml build generator
docker compose -f pgval/compose.yaml run --rm generator \
  --generator sst-uniform --seed 42 \
  --windows 2 --pages 2 --events-per-window 10 \
  --out-dir /runs/sst-001
```

Результат на хосте — только `runs/sst-001/plan.json`: события, действия и параметры находятся внутри него. [Схема плана](../PLAN.md).
Каталог результата должен быть новым: существующий эксперимент не перезаписывается.
`PGVAL_RUNS_DIR` позволяет выбрать другую общую папку; лучше задавать абсолютный путь.
Генератор имеет доступ к ней на запись, валидатор — только на чтение.

Если Docker доступен через sudo, используйте:

```bash
sudo docker compose -f pgval/compose.yaml build generator
sudo env PGVAL_UID="$(id -u)" PGVAL_GID="$(id -g)" \
  docker compose -f pgval/compose.yaml run --rm generator \
  --generator sst-uniform --seed 42 \
  --windows 2 --pages 2 --events-per-window 10 \
  --out-dir /runs/sst-001
```

После выполнения плана исполнителем выход приложения сохраняется в той же папке.
Валидатор запускается отдельно:

```bash
docker compose -f pgval/compose.yaml run --rm validator \
  --plan /runs/sst-001/plan.json \
  --observed-inputs /runs/sst-001/click.jsonl \
  --outputs /runs/sst-001/output.jsonl \
  --late-outputs /runs/sst-001/lateOutput.jsonl \
  --complete --report /reports/sst-001-validation.json
```

Последняя команда требует журналов исполнителя; генератор их не создаёт.
`--complete` передаётся после подтверждения финализации и завершения сбора.
Текущий генератор рассчитан на SST и один раздел Kafka; ограничения и формат
плана описаны в [общей инструкции](../README.md).

Проверены сборка общего образа, повторяемость плана при двух запусках,
права владельца на хосте и чтение созданного плана контейнером валидатора
на ручном примере (одно окно, ID 1 и 2). Это проверка связи компонентов,
а не сквозной запуск Flink. Отдельно общий образ проверен на архиве Flink: 60/60 окон.

## План с отказом

Сценарии описывает исполнитель. Готовые описания можно встроить при генерации:

```bash
cp pgval/generator/examples/fault-actions.json runs/fault-actions.json
docker compose -f pgval/compose.yaml run --rm generator \
  --generator sst-uniform --windows 1 --pages 1 --events-per-window 2 --rate 1 \
  --faults-json /runs/fault-actions.json --out-dir /runs/sst-fault-001
```

Пример задаёт обычный перезапуск TaskManager через 100 мс после начала отправки.
В итоговом плане события и отказ находятся вместе; исходный файл с отказами больше
не нужен. Команда только создаёт план, сам отказ выполняет исполнитель.
