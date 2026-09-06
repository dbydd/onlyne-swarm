# onlyne-swarm

Reactive multi-agent directed cyclic graph workflow scheduler derived from Onlyne.

- Workspace = agent, one agent session = one task invocation.
- Scheduler daemon (`onlyne-swarm`) manages nested workspaces from `.agents/.schedule`.
- Inter-agent transport: loopback channel + `onlyne_in/<target>/` symlinks.
- Stack: Rust. TUI included. IPC via workspace-local `swarm.sock`.

See `SPEC.md` (to be added) for the frozen design memo.
