# app/：被测系统（Kafka + Flink + 作业）

> 负责人：Ван Петр。草稿，中文版，确认后翻译成俄语。
> 状态（2026-11-03）：作者的 SST 作业已作为 baseline 在单机上跑通，正常运行和 TaskManager 崩溃两种情况都做了检查。我们自己的新拓扑还没开始写。

## 1. 这是什么

PGVal 要检验的是：Kafka + Flink 这条链路在故障下，**端到端的结果**是否真的"不多不少"（exactly-once）。

`app/` 提供的就是**被测的那条链路**：

```
生成器 ──写──→ Kafka "click" ──→ Flink 作业（按页面分组、60 秒窗口、输出事件 ID 列表）──→ Kafka "output" ──读──→ 验证器
                                                                              └──→ Kafka "lateOutput"（迟到事件）
```

- Kafka 和 Flink 用的是官方版本，没有改动。
- 目前运行的作业是**作者的 SST**（DSPF-BM 里的 `App.java`），从固定提交的源码构建，代码没有改。它是我们的 **baseline**，以后新拓扑要和它在同一个环境里对比。
- 生成器、验证器、故障执行器**不需要知道 Flink 的存在**：只要会往 Kafka 写、从 Kafka 读就行，用什么语言都可以。

## 2. 快速开始（任何一台装了 Docker 的机器）

**前提**

- Docker 和 Docker Compose v2（Linux、WSL2 或 macOS）
- 可用内存约 2 GB（实测三个容器一共约 1.3 GB）
- Python 3（只用于假生成器和检查脚本，不需要装额外的包）
- 作者的仓库，检出到固定提交：

```bash
git clone https://github.com/jawadtahir/DSPF-BM ~/repos/DSPF-BM
git -C ~/repos/DSPF-BM checkout d74c9b80a3a1
```

路径不一样的话，用环境变量 `DSPF_BM=/你的路径` 指定。

**第 1 步：构建作者的作业**（只需一次，第一次约 4 分钟）

```bash
app/baseline/build.sh
```

生成 `app/target/topology.jar`，并打印 sha256。

**第 2 步：启动被测系统**

```bash
docker compose -f app/compose.yaml up -d --wait
```

这一条命令会依次启动 Kafka，建 topic，启动 Flink（JobManager + TaskManager），然后提交 SST 作业。默认用 E1；要用 A1，就在前面加 `PG=a1`。

Flink 的 Web 界面：<http://localhost:8081>

**第 3 步：跑检查**

```bash
app/checks/sst-small/run.sh      # 12 条手写事件，答案事先算好
app/checks/sst-gen/run.sh        # 假生成器产生 3 001 条事件，自动对比
```

每个脚本都会先**清空环境**（`docker compose down -v`）再从头启动，保证每次运行的条件相同。

**停止**

```bash
docker compose -f app/compose.yaml down -v
```

## 3. 版本

| 组件 | 论文 | 作者代码 | 我们 | 说明 |
|---|---|---|---|---|
| Flink（JobManager、TaskManager） | 1.18.0 | 1.17.1 | **1.17.1**（`apache/flink:1.17.1-java11`） | 和作者的代码保持一致 |
| Flink Kafka connector | — | 1.17.1 | 1.17.1（打包在 jar 里） | |
| Kafka 客户端（在 connector 里） | — | 3.2.3 | 3.2.3 | |
| Kafka broker | — | 3.6.0（`bitnami/kafka`） | **3.7.0**（`apache/kafka`） | 官方镜像从 3.7.0 才开始提供；bitnami 已停止免费更新 |
| JDK | — | 11 | 11 | |
| 构建用的 Maven | — | 3.6.3 | 3.9.6（`maven:3.9.6-eclipse-temurin-11`） | 只影响构建 |

最新版本（今天查的）：Flink 2.3.0，但 Kafka connector 只支持到 Flink 2.2；Kafka 4.4.0。升级 Flink 要改作者的代码（2.0 删掉了一些旧 API），所以 **baseline 保持 1.17.1**。我们自己的作业用什么版本，等选好新拓扑以后一起决定。

## 4. 目录

```
app/
├── compose.yaml            被测系统：Kafka + Flink + 建 topic + 提交作业
├── baseline/build.sh       从源码构建作者的作业
├── fakegen/gen.py          假生成器（临时，以后换成队友的正式版本）
├── checks/
│   ├── sst-small/          12 条手写事件的检查，以及运行记录
│   └── sst-gen/            用生成器的检查，可以加故障，以及运行记录
├── job/                    （空）以后放我们自己的 Flink 作业
└── target/                 构建产物（不提交）
```

## 5. 接口：队友需要知道的

这些是被测系统对外的约定。我改动其中任何一项，都会提前通知大家。

**Kafka 地址**：`kafka:9092`，只能在 compose 网络里访问（`docker compose exec kafka ...`，或者在 compose 网络里起一个容器）。

**输入 `click`**：一条消息 = 一个事件

```
key:   "/help"        （JSON 字符串，带引号）
value: {"id":1,"timestamp":"2026-11-03T10:00:05.000","page":"/help"}
```

- `id`：唯一编号，验证器靠它追踪每个事件
- `timestamp`：事件时间，UTC，格式 `yyyy-MM-ddTHH:mm:ss.SSS`。**必须单调递增**，原因见第 7 节
- `page`：页面名
- **不能有其他字段。** 实测：多一个 `"user":"x"` 字段，作业就会抛出 `UnrecognizedPropertyException`，进入重启。每次恢复后又读到同一条消息，于是**无限重启**，后面的数据全部卡住

**输出 `output`**：每个页面、每个窗口一条

```
{"page":"/help","windowStart":1793700000000,"windowEnd":1793700060000,"clickIds":[1,3,5],"updateIds":[]}
```

- 窗口是 60 秒的滚动窗口，左闭右开。**时间是毫秒时间戳**，和输入的格式不同
- `clickIds` 的**顺序不固定**，要当集合来比较
- **必须用 `read_committed` 读取**，否则会读到 E1 下被中止的结果，误以为有重复

**迟到输出 `lateOutput`**：每条迟到事件单独一条

```
{"page":"/news","clickId":11,"clickTimestamp":1793700030000,"updateId":0,"updateTimestamp":0}
```

**窗口什么时候输出**：要等事件时间更晚的事件到来，把水位线推过窗口的结束时间。最后一个窗口要靠一条"收尾"事件才能关闭（假生成器会自动发）。E1 下，结果还要再等下一个 checkpoint（最多 5 秒）才对读者可见。

## 6. 配置和与作者的差异

全部配置都在 `compose.yaml` 里。★ 表示会影响实验结果的参数。

| 参数 | 作者 | 我们 | 为什么 |
|---|---|---|---|
| Kafka broker 数量 / 副本数 | 3 / 3 | 1 / 1 | 单机。所以**不测 Kafka 自身的故障** |
| ★ 事务日志的副本数和 min ISR | 3 / 2 | 1 / 1 | 不改的话单节点上 E1 跑不起来 |
| topic 分区数 | 3 | 1（`PARTITIONS=` 可以改） | 分区多于 1 时，某个分区没有数据，水位线就会卡住（第 7 节） |
| JM / TM 内存 | 10 GB / 15 GB | 1 GB / 1.5 GB | 单机 |
| TM 数量 × slot / 并行度 | 3 × 1 / 3 | 1 × 1 / 1 | 单机。所以**测不出并行度的影响** |
| ★ checkpoint 间隔 / 模式 | 5 s / EXACTLY_ONCE | 一样 | |
| checkpoint 存储位置 | 作者自己的 HDFS | 共享 Docker 卷 `file:///checkpoints` | |
| ★ 心跳间隔 / 超时 | 1 s / 3 s | 一样 | 论文里网络故障分成"延迟 < 超时"和"延迟 > 超时"，指的就是这个超时 |
| ★ 重启策略 | 固定延迟，近乎无限次 | 一样 | |
| 算子链 | 关闭 | 一样 | |
| ★ 事务超时（写在作业代码里） | 30 s | 一样 | 和新故障有关（第 9 节） |
| ★ 处理保证 `-pg` | 脚本里写了 `e1`，但**实际没生效**（bug，见第 7 节） | `PG=e1`（默认）或 `a1` | |

## 7. 过程中发现的问题

1. **作者的 compose 有 bug，E1 实际上从没生效。** 提交命令写的是 `-pg ¼{PG}`，`¼` 是 U+00BC，不是 `$`。作业收到的参数是字面上的 `¼{PG}`，不等于 `e1`，于是走了 A1。我们自己写了提交命令。
2. **作者的作业无法按原样构建。** Dockerfile 要求一个仓库里没有的 `settings.xml`，依赖的 `datamodel` 放在需要登录的 GitHub Packages 上。我们先在本地构建 `datamodel`，再构建作业（`baseline/build.sh`），作者的源码一行没改。
3. **水位线每 200 ms 才更新一次，不是每条事件都更新。** 一批事件同时到达时，本该算迟到的事件可能被算进窗口。所以"一个事件是否迟到"取决于它实际到达的时机，不只看输入数据本身。手写的例子分成两批发送就是为了这个。
4. **分区多于 1 时，水位线可能卡住。** 作者用的是 `forMonotonousTimestamps`，没有设置空闲超时。只要有一个分区没有数据，水位线就停在原地，窗口永远不关闭。
5. **作业级配置是在提交作业的客户端读取的。** 提交作业的容器如果没有 checkpoint 配置，作业照样能跑，但**一次 checkpoint 都不做**，E1 的结果永远不会提交，而且不报错。我们踩过这个坑。
6. 作者的序列化器出错时返回 `null`，会悄悄写出一条空消息，不报错。
7. **一条格式不对的输入就能让作业无限重启**（见第 5 节）。这本身也可以算作一种故障场景。

## 8. 做过的检查和结果

| 检查 | 条件 | 结果 | 记录 |
|---|---|---|---|
| 手写小例子 | 12 条事件，E1，无故障 | **PASS**（6 条窗口结果 + 1 条迟到事件，和手算的一致），跑了 2 次 | `checks/sst-small/results/` |
| 生成器，正常运行 | 3 001 条事件，30 个窗口，E1 | **PASS** | `checks/sst-gen/results/20261103T154932` |
| 生成器，正常运行，作者的负载 | 30 001 条，60 个窗口，每页面每窗口 500 条，E1 | **PASS** | `…/20261103T155102` |
| 杀掉 TaskManager，停 5 秒 | 同上，E1 | **PASS**：从 checkpoint 恢复，不丢不重 | `…/20261103T155430` |
| 杀掉 TaskManager，停 5 秒 | 同上，**A1** | **6 个窗口重复** | `…/20261103T155616` |
| 杀掉 TaskManager，停 39 秒 | 同上，E1 | **PASS** | `…/20261103T155921` |

复现命令：

```bash
PG=a1 KILL_AFTER=12 DOWN_FOR=5 app/checks/sst-gen/run.sh --events-per-window 500 --windows 10 --rate 1000
```

**A1 为什么正好重复 6 条：** 故障前最后一个 checkpoint 完成后约 3 秒，TaskManager 被杀。每秒 1 000 条事件，一个窗口有 3 000 条，所以这 3 秒里正好关闭了一个窗口，输出 6 条结果（每个页面一条）。A1 下这些结果写出去就立刻可见。恢复后 Flink 回到那个 checkpoint，重新计算这个窗口，于是同样的结果又输出了一次。E1 下，这些结果当时还在未提交的事务里，被中止后作废，所以没有重复。

**停 39 秒的 E1 为什么也没丢数据：** 被杀的那一刻，只有一个"还没被任何 checkpoint 记录"的事务，它本来就要作废、重新计算，所以超时被中止也没关系。要丢数据，需要一个"已经被 checkpoint 记录、但还没提交"的事务，而 checkpoint 完成到提交之间只有几毫秒，随机选时间 kill 几乎打不中。

每次运行的目录里有：`args.txt`（参数）、`input.tsv`（输入）、`output.jsonl` 和 `lateOutput.jsonl`（输出）、`timeline.txt`（故障和恢复的时间线）、`flink.log`、`compare.txt`（对比结果）。

## 9. 思路和下一步

**故障要和处理阶段挂钩，而不是按固定时间注入。** E1 下，一条数据要经过这些阶段：

```
Source 读入 → 进入窗口 → 窗口关闭、计算 → 写进事务（未提交）
  → checkpoint 开始 → sink 预提交 → checkpoint 完成 → 提交事务（读者可见）
```

作者的脚本按固定时间注入故障，只能碰到大概率发生的情况。第 8 节已经说明，随机 kill 打不中"checkpoint 完成后、提交前"这个间隙。

要做到可控，需要两样东西：

- **观测**：知道现在处在哪个阶段。可以用 Flink REST API、日志、对比 `read_uncommitted` 和 `read_committed` 两种读法。我们自己的作业还可以埋"钩子"，主动发出阶段信号。这部分由我（被测系统）提供。
- **注入**：在那个时刻执行故障：kill、pause、网络延迟、切断某一段连接等。这部分由故障执行器负责。

**新故障的候选（还没验证）：** 在 Flink 和 Kafka 之间放一个代理（例如 Toxiproxy），**切断 sink 到 Kafka 的连接，持续时间超过事务超时（30 秒）**。切断期间只要有 checkpoint 完成，对应的事务就一定提交不了，等连接恢复时它已经被 Kafka 中止 → 预计 E1 下会丢数据。这个方法不需要毫秒级的精确触发，结果应该能稳定复现。

**新拓扑**：还没选，候选有滑动窗口、允许迟到事件的窗口（同一个窗口会输出多次）、会话窗口、两级窗口。需要团队一起讨论。

## 10. 局限

- 单机：1 个 broker，并行度 1，每个 topic 1 个分区。测不出分区和并行度的影响，也不测 Kafka 自身的故障。
- 每种情况只跑了 1 次，正式实验每种至少要跑 3 次。
- 生成器和检查脚本里的对比部分都是临时版本，只做到窗口级别的对比，没有像论文那样统计逐个事件的可靠性。
- 故障是手动加在检查脚本里的，正式的故障注入由队友负责。
