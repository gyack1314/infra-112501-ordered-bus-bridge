# 产线事件通道迁移中的跨流去重与消费接续

训练记录迁移使用旧通道 old 和新通道 new。上游设备在每个 epoch 内从 1 编号，两个通道的传输编号各自独立。样例都为合成记录。 交接后新流须有权读取旧流已经接纳的业务事件及裁定结果，是否从原流复制由总线实现决定。

## 事件格式

每个 JSONL 文件是一段独立演练。每行含整数逻辑秒点 timestamp、事件名称 type 和对象 data。同一秒的行按文件顺序排列。时间从零起算。文件里的业务标识只用于建立输入之间的引用。所有场景均可在本地用合成角色重新运行。

- **event**　接收到事件，device、epoch、seq 为业务位置，channel 和 transport 为来源，body 是业务载荷。载荷 JSON 对象按字段值判同一内容。
- **complete_epoch**　生产方声明该 epoch 不再新增位置。last_seq 给出末号，gaps 是逐段闭区间，区间中没有事件。到声明之前已产生而尚未抵达的事件可以在声明之后送到。
- **resolve**　生产方引用 device、epoch、seq 和有效 transport，裁定未交付位置的冲突。
- **subscribe**　消费组为指定设备订阅 channel。组为新组时没有已确认进度。
- **poll**　组内 member 请求指定设备的下一条记录，delivery 是请求标识，ack 和 giveback 回引此标识。ack 或 giveback 须来自持有者。
- **ack**　处理完指定交付，以下一次 poll 观察后续位置。
- **giveback**　持有者主动放弃指定交付。
- **expire**　仿真组协调器通知指定 member 和 delivery 的持有权超时。
- **switch**　消费组申请指定设备由 old 转到 new，through 是旧侧交接截止位置，用 epoch、seq 表示。 旧侧尚有交付在途时先等待确认、退回或过期。原交接点不视为消费进度。
- **retire**　尝试下线 device 的 old 通道，through 是计划下线覆盖的业务位置。
- **inspect**　按设备和可选 group 查询进展。退订使用 unsubscribe，作用域为 group 和 device。

## 资料文件

- unfilled-before-new-epoch.jsonl
- handoff-with-unacked-work.jsonl

## 本地资料检查

需 Python 3.11。资料工具使用标准库，无需联网。下列命令读取事件文件并检查封装格式。

```sh
python3 trace_io.py data
python3 -m unittest -v
```

资料工具输出读取的文件、事件数量及首尾秒点。业务状态由使用该事件契约的服务维护。
