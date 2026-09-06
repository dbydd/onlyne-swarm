# 调度器架构（ARCH）

Rust 实现的独立 daemon。`onlyne-swarm run` 在 swarm root（启动时 cwd）前台运行，
`attach` 连接已运行实例。子目录误启动按祖先标记（`_onlyne_workspaces` 或
`.onlyne/swarm.db`）拒绝，`--force` 越过。

## 1. 模块

```text
src/main.rs        CLI 入口（run/attach/submit/cancel/list/status/tui/workspace）
src/root.rs        root 定位与祖先标记检查、树枚举
src/template.rs    jsonc 解析、深合并、back_edges 归一化
src/sync.rs        .schedule -> _onlyne_workspaces 生成、onlyne_in 软链维护
src/db.rs          swarm.db（rusqlite）：tasks、edges/callbacks、dead_letter
src/proto.rs       swarm 正文头解析与构造（PROTOCOL.md）
src/daemon.rs      各 workspace onlyne daemon 拉起/保活/回收
src/events.rs      订阅各 onlyne.sock（priority 最高级），路由 in/out 事件
src/sched.rs       任务状态机：pending 调度、ready 匹配、投递、回调记账、回收判定
src/orca.rs        orca terminal create/close/send 封装（JSON 输出解析）
src/ipc.rs         swarm.sock 服务：status/list/submit/cancel/toggle_swarm/subscribe
src/tui.rs         ratatui 监控面板（swarm.sock 订阅客户端）
```

依赖建议：tokio、clap、serde/serde_json、rusqlite、ratatui、uuid、json5（jsonc 解析）、
tracing。Unix socket 行分隔 JSON 与 onlyne 主仓库同构。

## 2. 启动序列（run）

1. root 检查（祖先标记拒绝 / `--force` 越过）。
2. `sync` 一次：生成目录、实例快照、onlyne_in 软链；back_edges 缺失目标直接报错退出。
3. 打开（或创建）`.onlyne/swarm.db`，重放 pending 任务。
4. 拉起树内全部 workspace 的 onlyne daemon（统一保活；崩溃重启；关闭时统一回收）。
5. 以最高 priority 订阅各 daemon `subscribe_events`；启动 swarm.sock 监听。
6. 主循环：in 任务事件 → 建 task（UUID）→ 建 terminal（orca）→ 等 ready → 写 loopback/in →
   out 事件 → 回调转发（parent 计数器 `-1`）→ 回收判定 → 关闭 terminal（杀 pi 进程，
   orca 自动回收 terminal）。

## 3. 任务状态机

```text
pending → running → replied → closed
   │          │          │
   │          ├─ early-exit（无 out）→ failed → 回调发起方 → closed
   │          └─ cancel → cancelled → 回调发起方 → closed
   └─ cancel → cancelled → closed
```

- `pending`：任务已建档，无可调度 terminal（等待 orca create 或 ready）。
- `running`：已投递 loopback/in，terminal 存活。子任务发出时 parent `pending_replies + 1`。
- `replied`：收到含本 `task_id` 的 out 事件。回调转发后 parent `pending_replies - 1`。
- 回收条件：自身 `replied`（或顶层无 parent 视为可直接回收）且 `pending_replies == 0`
  → 结束 pi 进程 → orca 回收 terminal → `closed`。
- 挂起等待回调的发起 session 常驻 running，不接受新任务；回调经 pi-onlyne 回复通道
  followUp 插入（回调是原子执行的唯一例外）。
- 回调无法投递（发起方 workspace/terminal 缺失）：进 dead_letter 表，TUI 可见。

## 4. ready 池（瞬态）

- `swarm_ready` 载荷：`{workspace, terminal_handle, ts}`。句柄只做展示与 kill 用，
  路由按 `workspace` 路径匹配 pending 任务（同路径亲和）。
- ready 到达时若有同路径 pending：直接投递（不建新 terminal）。
- ready 到达时无 pending：入瞬态池，默认 60s 无人认领即回收（杀 pi 进程）。
- 建 terminal 后默认 120s 无 ready：杀 terminal，任务回 pending，TUI 计数告警。

## 5. onlyne 本体改动（唯一一处）

`subscribe_events` 订阅者携带 `priority: u32`（缺省 0 保持兼容）。
daemon 按 priority 从高到低投递事件；任一订阅者返回 `{"consumed": true}` 即停止
向更低优先级传递。调度器用 `u32::MAX`，TUI 调试订阅用 `u32::MAX - 1`。
仅含 swarm 头的消息进入优先级链；无头消息走原有广播路径。
