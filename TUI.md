# TUI 监控面板（TUI）

ratatui 实现的 swarm.sock 客户端（2s 全量轮询 + 事件驱动刷新）。`onlyne-swarm tui` 启动（需调度器在运行，
否则提示先 `run`）。只读展示 + 两个动作键（cancel、toggle_swarm）。
告警只标红展示，不得干扰正常运行。

## 1. 布局

```text
┌─ workspaces ────────┬─ tasks ───────────────────────────┐
│ . (root) ●          │ task_id  from → to  att state     │
│ ├─ planner ●        │ aaaabbbb . → planner  1 running   │
│ │  └─ worker ○      │ ccccdddd planner → worker 1 replied│
│ └─ reviewer ● ╴╴╴╴╴ │ …                                 │
│    ╰╴ planner（回边）│                                   │
├─ daemons/terminals ─┴─ dead-letter ─────────────────────┤
│ planner: daemon ready, terminal abc123 running          │
│ worker: daemon starting…                                │
│ dead-letter: 2（task …原因…）                            │
└─ [c]ancel [t]oggle-swarm [q]uit ────────────────────────┘
```

- 左树：workspace 层级；`●` daemon 在线，`○` 离线；`back_edges` 用虚线 + `╰╴` 标注。
- 右上任务表：`task_id` 短显（前 8）、`from → to`、`attempt`、`state`
 （pending/running/replied/failed/cancelled/closed，closed 默认折叠）。
- 右下：daemon/terminal 存活、`swarm_ready` 超时计数、dead-letter 列表。
- 状态栏：`c` 取消选中任务族，`t` 切换选中 workspace 的 swarm 开关，`q` 退出。

## 2. 数据源

2s 间隔全量 `list_workspaces` + `list_tasks` 轮询刷新（`subscribe` 流为后续增量源，当前版本轮询已满足排障需求）。按键 `↑↓/jk` 选任务，`c` 取消选中任务族，`t` 切换 root swarm 开关，`q` 退出。

## 3. 开关语义（toggle）

关闭某 workspace 的 swarm 开关：排空后停止监听。存量任务执行到 out 写出，
新 in 任务不再建 session，daemon 恢复通用 in/out 自动处理，
状态写入该 workspace `[swarm] enabled = false`。首次宽容；排空中重复 toggle
要求则强制杀该 workspace 全部 swarm terminal（pending 进 dead-letter）。
