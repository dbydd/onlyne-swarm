# 验证场景（TEST）

脚手架完成后按顺序跑通三个端到端场景。测试树统一建在临时目录（非本仓库），
root 下 `.agents/.schedule/{a,b}/template.workspace.jsonc`，role 为回显固定文本，
model 指向本地可用的 pi 模型。

> 2026-09-06 实测状态：三场景已用 headless stub agent（`SWARM_STUB_AGENT=1` +
> python 驱动 `swarm_ready` / history 轮询 / `send_message` 回复）全跑通；真实 pi
> 会话因模型侧 429 余额不足暂未跑，待模型恢复后用 `SWARM_PI_EXT` 指向的本地
> pi-onlyne（swarm-mode 分支）复测。stub 脚本见 `/tmp/stub_*.py`（测试机本地）。
> 实测中修掉的真 bug：事件嵌套 envelope 未解包、`/tmp` vs `/private/tmp`
> 路径映射、并发回调 FIFO 合并丢失（进程级写锁）、重复投递二次转发
>（终态守卫）。回放时注意先清残留 daemon（`--workspace` 用 canonical 路径匹配
> pkill）与残留 stub 进程。

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

## 2b. 并发回调（FIFO 写锁回归）

1. 父任务扇出两个子任务，子任务无延迟同时回复。
2. 断言：两个回调都到达父 workspace（无合并丢失），父 `pending_replies`
   归零，父子全 `closed`。无写锁时此场景稳定复现丢一个回调。

## 3. 回边自激发循环（环路 + 手动终结）

1. a 与 b 的 `back_edges` 互指，对方收到任务即回发新任务（role 约定，载荷递增计数器）。
2. 断言：任务族持续增长，TUI 任务表滚动，`status` 计数告警标红但运行不受干扰。
3. `onlyne-swarm cancel <族task_id>` 后：两 terminal 回收，发起方收到 cancelled 回调，
   无新任务产生，TUI 无残留 running。

## 4. 通用断言（每场景）

- `task_id` 唯一，无重复投递建双 session。
- 无 swarm 头的消息不建 session（普通 loopback 行为不变）。
- 调度器重启后 pending 任务继续，丢失 terminal 的任务走失败回调。
