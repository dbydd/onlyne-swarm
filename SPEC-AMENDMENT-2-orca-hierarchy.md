# SPEC 修正案 2 — swarm 树到 Orca 的层级映射（替换 205288e 的 (a) 方案上限）

状态：待实现。提出：supervisor 现场 + 用户 Orca 实操验证。
背景缺陷报告：本文件 §1。本文给出 CLI 抓手与映射设计。

## 1. 事实纠正

`205288e` 的结论"Orca CLI 没有 folder/层级控制方式，(b) 无实现抓手"不成立。
Orca 有一等公民的多级工作树编队，CLI 全部暴露：

- `orca worktree show --worktree <selector> --json` 返回 `parentWorktreeId`、`childWorktreeIds`、`lineage` 三个字段——父子链是持久化元数据。
- `orca worktree set --worktree <selector> --parent-worktree <selector>`——给已存在的 worktree 挂父；`--no-parent` 解除。这就是 Orca UI 里右键"为它添加父工作树"的 CLI 等价物（用户实操确认 UI 路径存在）。
- `orca worktree create --parent-worktree <selector>`——新建时直接声明父。
- `orca project setup-existing-folder --project <id> --host local --path <绝对路径> --kind folder`——把任意现有目录以非 git 形态注册成 project 的 host setup。`--kind git|folder` 两式，folder 式专为无 git 目录设计。
- `orca repo add --path <绝对路径>`——按路径注册项目。
- `orca terminal create --worktree path:<绝对路径> --title ... --command ...`——terminal 可以挂到 selector 指定的任意 worktree。

本机实证（`orca worktree list --json`）：16 个 worktree 中 7 个带 `parentWorktreeId`，ReportHumanSpeech 的 scout-/writing-/archive- 子节点全部挂在根下——多级编队在生产中就是这么用的。swarm 现在把这一切绕开了：所有 hop terminal 共享根 `worktreeId`（`bb5f9e0d::...qwen38-27b-sft-workspace`），swarm 树在 Orca 工作区结构中不可见。

## 2. 目标

`onlyne-swarm sync` 之后，swarm 树的每个 workspace 在 Orca 中表现为根 worktree 的一个子节点；每次 hop 的 terminal 挂在自己 workspace 的节点下。Orca 侧栏从"根 + 一堆平铺 tab"变成与 `.ws/` 树同构的层级。git 形态保持不变：`.ws/*` 继续是同一 checkout 内的普通目录，swarm 不制造 git worktree（SPEC §12 的"无 git 映射"承诺保留——本修正只动 Orca 元数据层）。

## 3. 映射设计

### 3.1 sync 时登记（幂等）

对根 workspace 与每个子 workspace `W`（目录 `<root>/.ws/<name>`，根则 `W=<root>`）：

1. 探测：`orca worktree show --worktree path:<W> --json`。
   - `ok:true` → 已登记，跳到步骤 4。
   - `selector_not_found` → 继续。
2. 确保 project 存在：swarm root 复用其 git origin 推导的 project id（qwen 现场即 `github:dbydd/research-flywheel`，Orca 已有）；无需新建。
3. 注册目录：`orca project setup-existing-folder --project <project-id> --host local --path <W> --kind folder --display-name swarm:<tree-path> --json`。
   - 若 folder kind 生成的实体不能被 `worktree show --worktree path:` 解析（selector 空间不同），fallback：`orca repo add --path <W>`（见 §5 验证清单 V2，先测后写码）。
4. 挂父：`orca worktree set --worktree path:<W> --parent-worktree path:<P> --json`，P 为该 workspace 在 swarm 树中的父目录（根无父，跳过）。display-name 已是 `swarm:model` 样式，满足可搜索性。
5. 全部命令 best-effort：Orca 不可达/失败只记日志，sync 不中断（与 `205288e` 的 focus/rename 同风格）。

### 3.2 hop terminal 归属

`orca_term::create()` 增加 `--worktree path:<workspace dir>`。效果：

- hop tab 出现在对应子节点下（层级可见性达成）；
- 子节点 path 即 session cwd，`--command` 里的 `cd` 冗余但保留；
- 并发同名 hop（同 role 两任务）各归其节点的多个 tab，无需新机制。

`SWARM_FOCUS` 语义不变。`swarm:*` 标题钉选（`43d7a80`）不变，与层级互补。

### 3.3 回收与清理

- 现状：terminal 由调度器 close。workspace 节点是长生的（role 是编制，不随 hop 消亡），sync 只增不删。
- `onlyne-swarm` 删除子 workspace 实例时（目前不存在此操作，留接口位）：`worktree set --no-parent` + 解除注册（若 CLI 有 `project setup-delete`，用其现名）。

### 3.4 关闭开关

`swarm.workspace.jsonc` 的 orca 段增加 `hierarchy: true|false`（默认 true），false 时退回 `205288e` 行为。给无 Orca/非 macOS 环境省事：sync 探测到 `orca status` 不可达即整体跳过，与现有 terminal 策略一致。

## 4. SPEC 文本改动

- §5"Orca 映射"改写：单 worktree + 平铺 tab 的表述替换为 §3 的层级映射；删掉与 §12 互相打架的历史残句。
- §12 Non-goals："不碰 git/worktree 语义"保留并澄清：Orca 元数据层的父子编队属于展示映射，git 工作树语义不受影响。
- skill（`src/skill.rs`，`be1779c` 所在）同步：workspace 节点登记后，找 hop 的方式从"根 worktree 下搜 `swarm:` tab"改为"侧栏按 `.ws/` 树展开或搜 `swarm:` display-name"。

## 5. 验证清单（实现前手测）

- V1：对 `.ws/model` 跑 `setup-existing-folder --kind folder`，再 `worktree show --worktree path:.ws/model`。ok → folder kind 进 selector 空间，主路径成立。
- V2：V1 失败则 `repo add --path .ws/model` 后重试 show；记录哪种注册产生可寻址实体。
- V3：show 通过后 `worktree set --parent-worktree path:<root>`，`worktree list --json` 断言 `parentWorktreeId` 指向根；Orca UI 目测侧栏层级。
- V4：`terminal create --worktree path:.ws/model` 起一个 shell，确认 tab 落在 model 节点下且 cwd 正确；然后 close 并删除注册（`worktree rm` 或对应清理命令），恢复现场。
- V5：确认对 `.ws/*` 的注册不会把 `.ws` 内容算进 git 状态（`.gitignore` 已忽略，应无影响）。

## 6. 边界与风险

- `.ws/model` 位于外层 git repo 工作树内。Orca 把它注册成"独立 project/文件夹"后，其 git 解析可能向上冒泡到外层 repo（repoId 相同或错乱）。V1–V3 若出现 git 字段异常，展示层接受，但 terminal 的 worktree 归属仍要正确——以 V4 为准绳。
- project id 命名：v3 模板与 qwen 实例共享 origin `research-flywheel`，子节点挂哪个 project 要跟根一致，避免同一目录树出现两个 project 记账。
- 磁盘上 `.ws/<name>` 目录在首次投递前不存在；sync 已建目录，登记顺序保持 sync 内部：先 mkdir，后 Orca 注册。
- 幂等性以 `worktree show` 探测为准；Orca 无 upsert 语义时，重复 `setup-existing-folder` 的报错（already exists 类）按成功处理。

完成标准：V1–V5 通过；qwen 现场重启调度器 + sync 后，Orca 侧栏出现 root 下五个 `swarm:<role>` 子节点；跑一跳 model 任务，其 tab 在 `swarm:model` 下可见，标题 `swarm:model:<id8>`。
