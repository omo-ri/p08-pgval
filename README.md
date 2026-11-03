# P08 PGVal: сквозная проверка гарантий потоковой обработки

**Проектное задание:** P08. PGVal, `project-tasks/p08-pgval.md` в [репозитории курса](https://distsys.ru/osukhoroslov/nis2)

**Участники:**

- Ван Петр
- Долгий Александр
- Заварин Александр

**Исходный артефакт:** [jawadtahir/DSPF-BM@d74c9b80a3a1](https://github.com/jawadtahir/DSPF-BM/tree/d74c9b80a3a1) (Apache-2.0). Код артефакта в репозиторий не копируется.

## Сокращённый запуск

Команды технических проверок первой точки: [nis/checkpoints/01.md](nis/checkpoints/01.md).

Нужны Docker с Compose и клон `jawadtahir/DSPF-BM` на ревизии `d74c9b80a3a1` в `~/repos/DSPF-BM` (или путь в `DSPF_BM`).

```bash
app/baseline/build.sh          # сборка задания Flink автора -> app/target/topology.jar
docker compose -f app/compose.yaml up -d --wait   # стенд + SST; PG=a1 для at-least-once
app/checks/sst-small/run.sh    # чистый стенд + SST (E1) + малый пример с известным ответом
```

## Генерация плана теста

Генератор и независимый валидатор запускаются одноразовыми контейнерами из общего образа:

```bash
mkdir -p runs reports
export PGVAL_UID=$(id -u) PGVAL_GID=$(id -g)
docker compose -f pgval/compose.yaml build generator
docker compose -f pgval/compose.yaml run --rm generator \
  --generator sst-uniform --windows 2 --pages 2 --events-per-window 10 \
  --out-dir /runs/sst-001
```

Результат — `runs/sst-001/plan.json` с параметрами, событиями и действиями.
[Формат плана v2](pgval/PLAN.md), [добавление описаний отказов](pgval/generator/README.md#план-с-отказом),
[запуск валидатора](pgval/validator/README.md). Исполнение v2 подключает разработчик
исполнителя. Прототипы `pgval` работают отдельно от скриптов приложения.
При недоступном Docker-сокете см. [запуск через sudo](pgval/validator/README.md#доступ-к-docker).

## Навигация

- [Описание проекта](nis/project.md)
- [План эксперимента](nis/experiment-plan.md)
- [Контрольная точка 1](nis/checkpoints/01.md)
- [Требования курса](requirements/README.md)
- Код:
  - [app/](app/) — тестируемая система: Kafka, Flink и задание Flink

Генератор, независимый Oracle, валидатор и контракт журналов: [pgval/README.md](pgval/README.md).
