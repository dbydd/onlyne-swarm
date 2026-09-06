# Swarm 正文协议头（PROTOCOL，修订一 2026-09-06）

onlyne 本体零侵入的上层协议：swarm 元数据夹带在消息正文首部，外加一层解析器。
传输层（Envelope、FIFO、socket 事件）完全复用现有 onlyne 通道。

射后不理：`transfer_send_to` 是转派血缘（本任务由哪个任务激发而来），只用于
TUI 家族视图、cancel 级联、ledger 关联。零等待语义、零路由语义。

## 1. 线格式

```text
---swarm
task_id: 550e8400-e29b-41d1-a716-446655440000
from: planner
transfer_send_to: 11111111-2222-3333-4444-555555555555
attempt: 1
---
## role: planner

<人类可读 Markdown 载荷>
```

- 首行必须恰为 `---swarm`，末分隔行必须恰为 `---`。
- 头部为 YAML 映射，四字段必填：
  - `task_id`：UUID v4，调度器在 submit 与 in 监听入口生成。复用为 session id。
    swarm.db 内唯一索引，重复投递直接丢弃并记日志。
  - `from`：发起方树相对路径，root 为 `.`（如 `planner`、`a/b`）。
  - `transfer_send_to`：生成本任务的那个 task_id。顶层任务为空字符串或缺省。
  - `attempt`：整数，首次为 1。调度器不重放，attempt 主要用于 out 去重与 TUI 展示。
- 分隔行之后为 Markdown 载荷。调度器投递时在载荷前前置 `## role: <name>` 段
  （role 文取自合并后的模板 `role` 字段）。
- 旧 `reply_to` 头不再识别：带 `reply_to` 的消息按头解析失败处理（普通消息）。

## 2. 解析规则

1. 正文不以 `---swarm\n` 开头 → 非 swarm 消息，按普通消息处理（不建 session）。
2. 缺 `---` 闭合行 → 解析失败，按普通消息处理，不报错中断管线。
3. 头部 YAML 解析失败或缺必填字段 → 解析失败，按普通消息处理，并在 daemon 日志记 `warn`。
4. `task_id` 非法（非 UUID）→ 按普通消息处理。
5. 带旧 `reply_to` 字段 → 按普通消息处理（新旧字段名互不识别）。
6. 头解析成功 → swarm 任务消息，进入优先级链（见 SPEC §5）。

## 3. out 格式

目标 session 经 out 写出结果时，正文同样携带 swarm 头：`task_id` 为本任务 id，
`from` 为本 workspace 路径，`transfer_send_to` 原值返回，`attempt` 原值返回。
out 即成功信号（done），调度器记台账并回收 terminal，不做任何转发。

失败与取消不写消息体：早停记 failed 台账行，cancel 记 cancelled 台账行。
`swarm-failed:` / `swarm-cancelled:` 前缀只出现在 ledger `reason` 字段。

## 4. 示例

顶层提交：

```text
---swarm
task_id: aaaabbbb-cccc-dddd-eeee-ffffffffffff
from: .
transfer_send_to:
attempt: 1
---
## role: planner

请产出三阶段拆解。
```

子任务激发（planner 激发的新任务）：

```text
---swarm
task_id: ccccdddd-eeee-ffff-0000-111111111111
from: planner
transfer_send_to: aaaabbbb-cccc-dddd-eeee-ffffffffffff
attempt: 1
---
## role: reviewer

请评审上述拆解。
```

planner 自己的 out（交活即退，不等 reviewer）：

```text
---swarm
task_id: aaaabbbb-cccc-dddd-eeee-ffffffffffff
from: planner
transfer_send_to:
attempt: 1
---
已激发评审任务 ccccdddd…，本跳产物见 runs/…。
```

## 5. 与 Envelope 的关系

不扩展 `MessageEnvelope` 结构体。swarm 头只活在 `text` 字段内。
`platform_metadata` 不写 swarm 键，保持旧代码反序列化零改动。
需要结构化查询时由调度器解析正文后写入 swarm.db。
