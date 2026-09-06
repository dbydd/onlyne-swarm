# TUI 监控面板（TUI）

ratatui 实现的 swarm.sock 订阅客户端。`onlyne-swarm tui` 启动（需调度器在运行，
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

全部经 swarm.sock `subscribe` 事件增量更新，启动时 `list_workspaces` +
`list_tasks` 拉全量。断线 1s 后重连并重拉全量。

## 3. 开关语义（toggle）

关闭某 workspace 的 swarm 开关：排空后停止监听。存量任务执行到 out 写出，
新 in 任务不再建 session，daemon 恢复通用 in/out 自动处理，
状态写入该 workspace `[swarm] enabled = false`。首次宽容；排空中重复 toggle
要求则强制杀该 workspace 全部 swarm terminal（pending 进 dead-letter）。
