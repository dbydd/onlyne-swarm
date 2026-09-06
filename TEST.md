# 验证场景（TEST）

脚手架完成后按顺序跑通三个端到端场景。测试树统一建在临时目录（非本仓库），
root 下 `.agents/.schedule/{a,b}/template.workspace.jsonc`，role 为回显固定文本，
model 指向本地可用的 pi 模型。

## 1. 单链双节点（submit → ready → 投递 → 回调 → 回收）

1. `onlyne-swarm run` 启动，`sync` 生成 `_onlyne_workspaces/a`，daemon 上线。
2. `onlyne-swarm submit --to a --payload task1.md` 返回 `task_id`。
3. 断言：a 的 terminal 建立，`swarm_ready` 到达后 loopback/in 收到带 swarm 头的任务；
   pi 回复写出 out 后调度器转发回调到 root；root 收到回调；
   a 的 terminal 回收（orca 侧无残留），任务状态 `closed`。
4. `onlyne-swarm list --tasks` 全 `closed`，`status` 无 orphan/dangling。

## 2. 一发多收扇出（pending_replies 记账）

1. root 提交父任务到 a，a 的 role 要求它向 b 与 c 各发一个子任务后汇总。
2. 断言：父 task `pending_replies` 经历 `0 → 2 → 1 → 0`；
   两个子回调都转发回 a 的同一挂起 session（followUp 插入）；
   父 out 写出后 terminal 回收；三个任务全 `closed`。

## 3. 回边自激发循环（环路 + 手动终结）

1. a 与 b 的 `back_edges` 互指，对方收到任务即回发新任务（role 约定，载荷递增计数器）。
2. 断言：任务族持续增长，TUI 任务表滚动，`status` 计数告警标红但运行不受干扰。
3. `onlyne-swarm cancel <族task_id>` 后：两 terminal 回收，发起方收到 cancelled 回调，
   无新任务产生，TUI 无残留 running。

## 4. 通用断言（每场景）

- `task_id` 唯一，无重复投递建双 session。
- 无 swarm 头的消息不建 session（普通 loopback 行为不变）。
- 调度器重启后 pending 任务继续，丢失 terminal 的任务走失败回调。
