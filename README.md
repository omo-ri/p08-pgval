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

## Навигация

- [Описание проекта](nis/project.md)
- [План эксперимента](nis/experiment-plan.md)
- [Контрольная точка 1](nis/checkpoints/01.md)
- [Требования курса](requirements/README.md)
- Код:
  - [app/](app/) — тестируемая система: Kafka, Flink и задание Flink
