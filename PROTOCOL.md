# Swarm 正文协议头（PROTOCOL）

onlyne 本体零侵入的上层协议：swarm 元数据夹带在消息正文首部，外加一层解析器。
传输层（Envelope、FIFO、socket 事件）完全复用现有 onlyne 通道。

## 1. 线格式

```text
---swarm
task_id: 550e8400-e29b-41d1-a716-446655440000
from: planner
reply_to: 11111111-2222-3333-4444-555555555555
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
  - `reply_to`：父 task_id。顶层任务为空字符串或缺省。
  - `attempt`：整数，首次为 1。每次重建 session 自增（调度器侧当前策略为不重放，
    attempt 主要用于 out 回复去重与 TUI 展示）。
- 分隔行之后为 Markdown 载荷。调度器投递时在载荷前前置 `## role: <name>` 段
  （role 文取自合并后的模板 `role` 字段）。

## 2. 解析规则

1. 正文不以 `---swarm\n` 开头 → 非 swarm 消息，按普通消息处理（不建 session）。
2. 缺 `---` 闭合行 → 解析失败，按普通消息处理，不报错中断管线。
3. 头部 YAML 解析失败或缺必填字段 → 解析失败，按普通消息处理，并在 daemon 日志记 `warn`。
4. `task_id` 非法（非 UUID）→ 按普通消息处理。
5. 头解析成功 → swarm 任务消息，进入优先级链（见 SPEC §5）。

## 3. 回复格式

目标 session 经 out 写出回复时，正文同样携带 swarm 头：`task_id` 为本任务 id，
`from` 为本 workspace 路径，`reply_to` 为收到的 `reply_to`（即回给父任务），
`attempt` 原值返回。调度器按 `reply_to` 找到发起方做回调转发。

失败回调正文载荷首段为固定标记行 `> swarm-failed: <原因>`，成功回调无标记。
cancelled 回调标记行为 `> swarm-cancelled: <原因>`。

## 4. 示例

顶层提交：

```text
---swarm
task_id: aaaabbbb-cccc-dddd-eeee-ffffffffffff
from: .
reply_to:
attempt: 1
---
## role: planner

请产出三阶段拆解。
```

子任务回调（planner 写给 root）：

```text
---swarm
task_id: aaaabbbb-cccc-dddd-eeee-ffffffffffff
from: planner
reply_to:
attempt: 1
---
产出如下……
```

## 5. 与 Envelope 的关系

不扩展 `MessageEnvelope` 结构体。swarm 头只活在 `text` 字段内。
`platform_metadata` 不写 swarm 键，保持旧代码反序列化零改动。
需要结构化查询时由调度器解析正文后写入 swarm.db。
