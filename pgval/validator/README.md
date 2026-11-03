# Офлайн-валидатор в Docker

Контракт и обычный запуск: [../README.md](../README.md).
Контейнер читает журналы, соединение с Kafka ему не требуется. Образ и Compose общие с [генератором](../generator/README.md); общая папка `runs` доступна валидатору только для чтения.
## Проверка нового плана v2

После выполнения плана исполнителем в папке запуска должны находиться `plan.json`,
фактический вход `click.jsonl` и выходные журналы. Из корня репозитория:

```bash
mkdir -p runs reports
export PGVAL_UID=$(id -u) PGVAL_GID=$(id -g)
docker compose -f pgval/compose.yaml build validator
docker compose -f pgval/compose.yaml run --rm validator \
  --plan /runs/sst-001/plan.json \
  --observed-inputs /runs/sst-001/click.jsonl \
  --outputs /runs/sst-001/output.jsonl \
  --late-outputs /runs/sst-001/lateOutput.jsonl \
  --complete --report /reports/sst-001-validation.json
```

События берутся из плана, отдельный `--inputs` не нужен. Если он передан, журнал
отправки дополнительно сверяется с планом. `--observed-inputs` сверяет фактический
вход Kafka; без него проверяется только соответствие выхода запланированному входу.
`--complete` означает, что вызывающая сторона подтвердила финализацию и полный сбор.
При запланированном отказе `fault_execution=not_checked`: проверка правильности
результата не подтверждает выполнение отказа.

## Проверка архивного запуска v1

Для старого плана события читаются из обязательного `--inputs`.
Следующая команда работает на сохранённом архиве без запуска Kafka и Flink:

```bash
mkdir -p runs reports
export PGVAL_UID=$(id -u) PGVAL_GID=$(id -g)
docker compose -f pgval/compose.yaml build
docker compose -f pgval/compose.yaml run --rm validator \
  --plan /data/pgval/validator/examples/sst-archive-plan.json \
  --inputs /data/app/checks/sst-gen/results/20261103T155921/input.tsv \
  --outputs /data/app/checks/sst-gen/results/20261103T155921/output.jsonl \
  --late-outputs /data/app/checks/sst-gen/results/20261103T155921/lateOutput.jsonl \
  --complete --report /reports/validation.json
```

Репозиторий монтируется только для чтения, `reports` — для записи; сеть отключена.
Код возврата 0 — pass, 1 — fail/incomplete/invalid_input. Полный результат сохраняется в JSON.

## Доступ к Docker

Docker — основной способ запуска валидатора в стенде. Если `docker version`
показывает `permission denied` для `/var/run/docker.sock`, проверьте:

```bash
systemctl is-active docker
ls -l /var/run/docker.sock
id -nG
sudo docker version
```

В проверенной среде daemon работает, сокет принадлежит `root:docker`,
а пользователь не входит в группу `docker`. Доступ через `sudo` работает.
Для команд выше можно использовать:

```bash
sudo docker compose -f pgval/compose.yaml build
sudo env PGVAL_UID="$(id -u)" PGVAL_GID="$(id -g)" \
  docker compose -f pgval/compose.yaml run --rm validator \
  --plan /data/pgval/validator/examples/sst-archive-plan.json \
  --inputs /data/app/checks/sst-gen/results/20261103T155921/input.tsv \
  --outputs /data/app/checks/sst-gen/results/20261103T155921/output.jsonl \
  --late-outputs /data/app/checks/sst-gen/results/20261103T155921/lateOutput.jsonl \
  --complete --report /reports/docker-validation.json
```

UID/GID передаются контейнеру, чтобы отчёт принадлежал текущему пользователю.
Сборка и этот запуск проверены: `pass`, 60/60 окон, без пропусков и лишних вкладов.
Права сокета и состав групп не изменялись.
