# TUI 监控面板（TUI，修订一 2026-09-06；层级视图 2026-09-07）

ratatui 实现的 swarm.sock 客户端（2s 全量轮询 + 事件驱动刷新）。`onlyne-swarm tui` 启动（需调度器在运行，
否则提示先 `run`）。只读展示 + 三个动作键（focus-tab、cancel、toggle_swarm）。
告警只标红展示，不得干扰正常运行。

## 1. 布局

```text
┌─ workspaces ────────┬─ tasks ─────────────────────────────────┐
│ . (root) ●          │ task_id  from → to  att state   xfer    │
│ ├─ planner ●        │ aaaabbbb . → planner  1 closed  -       │
│ │  · aaaabbbb [closed]                                        │
│ │  └─ worker ○      │ ccccdddd planner → worker 1 done cccc…  │
│ │    ▶ ccccdddd [running] ◉                                   │
│ └─ reviewer ● ╴╴╴╴╴ │ …                                       │
│    ╰╴ planner（回边）│                                         │
├─ ledger ────────────┴─────────────────────────────────────────┤
│ tasks: {"closed":2,"running":1}                               │
│ ledger (latest first):                                        │
│ ccccdddd worker done <out head 摘要…>                         │
└─ [f]ocus tab [c]ancel [t]oggle-swarm [q]uit ──────────────────┘
```

- 左树：workspace 层级；`●` daemon 在线，`○` 离线；`back_edges` 用虚线 + `╰╴` 标注。
  每个节点下挂该 workspace 的 hop 行（`· id8 [state]`），选中行标 `▶`，带真实
  terminal 的行加 `◉`。这是 Orca 侧没有的东西的 TUI 替代：folder-kind 节点
  在 Orca 里不可见（只剩 Unknown 幽灵分组），这里按 `.ws/` 树把同 workspace
  的 hop 收成同组行，同节点的多个 `◉` 行对应 Orca 根 worktree 下标题相同的
  多个兄弟 tab。
  格式化逻辑在 `tui.rs::tree_tab_lines`，纯函数，有单测覆盖。
- 右上任务表：`task_id` 短显（前 8）、`from → to`、`attempt`、`state`
 （pending/running/done/failed/cancelled/closed）、`xfer` 血缘短 id。
- 右下 ledger：状态计数 + 最近 8 条终态行（`id8 to state` + out 摘要或失败原因）。
  早期版本这里直接 dump `status` JSON，现在换成可读摘要。
- 状态栏：`f`/Enter 聚焦、`c` 取消血缘族、`t` 切换 root swarm 开关、`q` 退出。

## 2. 聚焦 Orca tab

选中 task 后按 `f` 或 Enter，TUI 取该 task 的 terminal handle 调
`orca terminal switch --terminal <handle>`，把 Orca UI 切到对应 hop tab
（等价于手点 tab）。三种情况只写状态行、不中断 TUI：

- handle 为空：任务还没建 terminal（pending 或已回收），报 `no live tab for <id8>`
- handle 以 `stub-` 开头：headless stub 会话，无真实 tab
- Orca 报 `terminal_handle_stale` 等错误：会话已关闭，原样回显错误

## 3. 数据源

2s 间隔全量 `list_workspaces` + `list_tasks` 轮询刷新（`subscribe` 流为后续增量源，当前版本轮询已满足排障需求）。按键 `↑↓/jk` 选任务，`f`/Enter 切 Orca tab，`c` 取消选中任务血缘族，`t` 切换 root swarm 开关，`q` 退出。

## 4. 开关语义（toggle）

关闭某 workspace 的 swarm 开关：排空后停止监听。存量任务执行到 out 写出，
新 in 任务不再建 session，daemon 恢复通用 in/out 自动处理，
状态写入该 workspace `[swarm] enabled = false`。首次宽容；排空中重复 toggle
要求则强制杀该 workspace 全部 swarm terminal（running 记 failed 台账行）。
