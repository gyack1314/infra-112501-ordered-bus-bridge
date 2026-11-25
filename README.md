# 产线事件通道迁移中的跨流去重与消费接续

训练记录迁移使用旧通道 old 和新通道 new。上游设备在每个 epoch 内从 1 编号，两个通道的传输编号各自独立。样例都为合成记录。交接后新流须有权读取旧流已经接纳的业务事件及裁定结果，是否从原流复制由总线实现决定。

本仓库在事件契约之上实现了一个纯标准库的 Python 中间件，把旧流交接给新流：现场生产者持续双写，消费组各自独立交接，运维可以按设备查询卡点并分批下线老通道。

## 模块组成

| 文件 | 职责 |
| --- | --- |
| `migration_bus.py` | 核心状态机 `MigrationBus`：业务位置接纳、去重、冲突证据、空号与代次顺序、组游标、持有权、切换、下线与查询分类。 |
| `channel_adapters.py` | 本地适配方：`ProducerAdapter` / `ConsumerGroupAdapter` / `CoordinatorAdapter` 分别扮演生产方、消费组和协调器，old 与 new 仅通道标签不同。 |
| `event_store.py` | JSONL 事件日志 + 状态快照。每条事件先 fsync 落盘；重启载入快照后只重放尾部日志。 |
| `replay.py` | 把 trace JSONL 回放到状态机的 CLI，含中途重启对照演练。 |
| `migration_demo.py` | 用适配方跑通双写、冲突、缺口、代次、切换、接回、下线、退订与重启的端到端断言脚本。 |
| `trace_io.py` | 原有资料封装检查工具。 |

## 事件格式

每个 JSONL 文件是一段独立演练。每行含整数逻辑秒点 timestamp、事件名称 type 和对象 data。同一秒的行按文件顺序排列。时间从零起算。文件里的业务标识只用于建立输入之间的引用。所有场景均可在本地用合成角色重新运行。

- **event**　接收到事件，device、epoch、seq 为业务位置，channel 和 transport 为来源，body 是业务载荷。载荷 JSON 对象按字段值判同一内容。
- **complete_epoch**　生产方声明该 epoch 不再新增位置。last_seq 给出末号，gaps 是逐段闭区间，区间中没有事件。到声明之前已产生而尚未抵达的事件可以在声明之后送到；已接收内容的位置不能报空，空号位置之后也不能再写入。
- **resolve**　生产方引用 device、epoch、seq 和有效 transport，裁定未交付位置的冲突。
- **subscribe**　消费组为指定设备订阅 channel。组为新组时没有已确认进度，游标惰性对齐到设备的最早已知代次。
- **poll**　组内 member 请求指定设备的下一条记录，delivery 是请求标识，ack 和 giveback 回引此标识。ack 或 giveback 须来自持有者。
- **ack**　处理完指定交付，以下一次 poll 观察后续位置。
- **giveback**　持有者主动放弃指定交付。
- **expire**　仿真组协调器通知指定 member 和 delivery 的持有权超时。
- **switch**　消费组申请指定设备由 old 转到 new，through 是旧侧交接截止位置，用 epoch、seq 表示。旧侧尚有交付在途时先等待确认、退回或过期。原交接点不视为消费进度。
- **retire**　尝试下线 device 的 old 通道，through 是计划下线覆盖的业务位置。
- **unsubscribe**　组退订指定设备；退订会关闭该组在设备上的在途交付，并解除其对下线的阻塞。
- **inspect**　按设备和可选 group 查询进展。

## 中间件规则

### 生产侧：同一业务位置只生效一次

- 业务位置由 `(device, epoch, seq)` 唯一确定，与 channel/transport 无关。
- 同 transport 重试、或双写期间不同 transport 但 body 字段值相同：只进入一次（`deduped`）。
- 同位置出现不同 body：位置进入 `conflict`，保留全部传输副本作为证据，停止向下交付；只有 `resolve` 指定有效 transport 后才放行。
- 位置一旦交付，后续不同内容只追加到 `post_delivery_conflicts` 留证，不改变已交付内容，也不允许再 resolve。
- 缺号挡住同设备后续位置的交付；`complete_epoch` 的 gaps 显式声明的空号会被游标跳过。
- 新代次必须等旧代次宣告完结后才能写入；不同设备互不影响。

### 消费侧：组内顺序与持有权

- 每个组在每个设备上维护独立游标，只沿设备内顺序前进，空号自动跳过。
- 一个组同一设备同时只有一个 held 交付；`expire` / `giveback` 后，未确认位置可由组内其他成员接回（`redelivered`），旧 delivery 标识立即作废，旧持有者的迟到 ack 被拒绝（`rejected_stale` / `rejected_not_holder`）。
- 旧组不读取只存在于 new 的副本（`awaiting_channel`）；新组可以读取 old 已接纳内容，对应“交接后新流有权读取旧流事件”。

### 切换与下线

- `switch` 按 (group, device) 生效：仍有 old 的 held 交付时进入 `waiting_in_flight`；没有仍可完成的旧在途后自动落到 new。
- 切换时未确认的旧交付被关闭，新流从组的第一个未确认位置接续，即使它位于原定 through 之前；through 只用于下线判定，不是消费进度。
- 新组不必等待尚未切换的旧组。
- `retire` 仅在以下条件全部满足时成功，否则返回分类 blockers：
  - through 覆盖的代次均已宣告完结，范围内没有未接纳事件或未裁定冲突；
  - 没有组仍停在 old（含切换等待中的组），未切换的组须显式 `unsubscribe`；
  - 已切换组的游标必须越过各自的交接点。
- 下线后 old 通道拒绝新记录；同一范围重复 retire 幂等。

### inspect 的四种卡点分类

| classification | 含义 |
| --- | --- |
| `producer_conflict` | 游标位置存在未裁定的生产端冲突 |
| `event_missing` | 已完结代次里游标位置既无内容也未报空 |
| `old_delivery_completable` | 组仍持有可完成的 old 在途交付（切换会因此等待） |
| `catching_up` | 没有外部阻塞，只是组自身进度尚未追上 |

设备级 `summary` 另外汇总全部 `producer_conflicts` 与 `missing_events`。

## 资料文件

- unfilled-before-new-epoch.jsonl
- handoff-with-unacked-work.jsonl

## 本地运行

需 Python 3.11，仅使用标准库，无需联网。

```sh
# 资料封装检查
python3 trace_io.py data

# 全部单元测试（49 个，含并发与重启恢复）
python3 -m unittest -v

# 用新旧适配方跑端到端迁移演练（每步带断言）
python3 migration_demo.py

# 回放资料 trace，打印 inspect 与被拒绝的请求
python3 replay.py run data/handoff-with-unacked-work.jsonl
python3 replay.py run data/unfilled-before-new-epoch.jsonl

# 带持久化的回放（事件日志 + 快照写入目录）
python3 replay.py run data/handoff-with-unacked-work.jsonl --store-dir .state/handoff

# 中途重启演练：分段处理与一次性运行的最终状态必须完全一致
python3 replay.py restart-demo data/handoff-with-unacked-work.jsonl --store-dir .state/restart
```

资料工具输出读取的文件、事件数量及首尾秒点。`replay.py` 默认把违反契约的请求记录为 `violation` 结果而不中断，加 `--strict` 可让首个违规立即抛出。
