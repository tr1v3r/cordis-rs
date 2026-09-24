# cordis-rs

[![ci](https://github.com/tr1v3r/cordis-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/tr1v3r/cordis-rs/actions/workflows/ci.yml)
[![integration](https://github.com/tr1v3r/cordis-rs/actions/workflows/integration.yml/badge.svg)](https://github.com/tr1v3r/cordis-rs/actions/workflows/integration.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**cordis v4 标准插件运行时的 Rust 实现。内核（协调器/effect/服务/事件）与 JSON loader/reconcile 已经实现并通过测试；HMR（P9）尚未实现。**

Cordis 的三个核心承诺：

> 插件实例有清晰的生命周期；通过上下文注册的副作用有明确归属且可回收；服务依赖的出现、消失和换代驱动实例重新协调。

技术路线：stable Rust（edition 2024，MSRV 1.85）+ Tokio、类型化公开 API、内部类型擦除、显式 effect 作用域、串行状态协调器 + 并发用户任务、`#![forbid(unsafe_code)]`。WASM 代码替换是独立扩展（P9），不是核心启动条件。

## 功能矩阵

| 阶段 | 内容 | 状态 |
| --- | --- | --- |
| P0–P1 | Cargo workspace、toolchain 锁定、ID newtype、`DefinitionId`、错误 enum、`App`/`Context` | ✅ 已实现（`cordis-core`） |
| P2 | 串行协调器、有界邮箱 + 独立完成通道、异步生命周期状态机（Pending/Starting/Active/Stopping/Failed/Quarantined/Disposed）、latest-wins 修订、迟到结果核验 | ✅ 已实现 |
| P3 | effect 系统：显式作用域、同步接纳 gate、可等待清理（`on_dispose`/`effect`/`spawn_prepare`/`spawn_on_activate`）、panic/超时隔离、子 fiber 归属 | ✅ 已实现 |
| P4 | 服务注册：typed `ServiceKey`、`BindingId` 身份、lease/`set`/`set_available`、managed 服务、依赖 epoch 反向索引驱动的自动重载、`fork`/`isolate`/`isolate_shared` 命名空间 | ✅ 已实现 |
| P5 | typed 事件总线：emit/bail/serial/parallel/waterfall 五模式、move-only `Next`、`once`/`prepend`/`global`、scoped 派发、有界重入、结构化诊断广播、可选 `tracing` feature | ✅ 已实现 |
| P6 | JSON loader：base/patch 层解析、严格 config 语义（拒绝 null）、大整数范围守卫、compose + provenance、redact dump、typed registry、group fiber、预解码、all-or-nothing 挂载恢复 | ✅ 已实现（`cordis-loader`） |
| P7 | reconcile：纯计划（keep/update/recreate/remove/insert）、dry-run 渲染、revision 绑定 apply、per-node 报告 | ✅ 已实现 |
| P8 | 发布前加固 | ◐ 部分：CI 门禁（fmt/clippy/测试矩阵 ×3 OS ×2 工具链、MSRV、3× 全量测试 + rustdoc + 覆盖率）已就位；rustdoc 与示例见本仓库；**稳态资源基线（V41）、性能基准、crates.io 发布未做** |
| P9 | HMR（WASM/子进程/原生动态库） | ❌ 未实现，相关验收（V54–V61）不适用 |

验收矩阵（docs/07）中内核与 loader 语义（V01–V40、V42–V53）已由 `crates/*/tests/` 的集成测试锚定；少数验收项（如邮箱饱和、JoinHandle 窗口）以等价场景测试覆盖而非逐条 V 编号命名。loader 的 compose/dump 行为与 Go 基线做了 golden 差分对拍（`crates/cordis-loader/tests/golden.rs`）。

## Quick start

```sh
git clone https://github.com/tr1v3r/cordis-rs && cd cordis-rs
cargo run --example fibers -p cordis-core    # 最小插件生命周期
cargo run --example services -p cordis-core  # 依赖驱动重载
cargo run --example events -p cordis-core    # typed 事件总线
cargo run --example loader -p cordis-loader  # JSON 配置挂载 + reconcile
cargo test --workspace                       # 全部测试
```

最小可编译示例（与 `crates/cordis-core/examples/fibers.rs` 一致）：

```rust
use std::sync::Arc;
use std::time::Duration;
use cordis_core::{App, OperationOutcome, ShutdownOptions, define};

struct Config { name: String }

#[tokio::main]
async fn main() {
    let app = App::builder().name("host").build().expect("app builds");
    let root = app.context();

    let plugin = define("greeting", |ctx, cfg: Arc<Config>| async move {
        let name = cfg.name.clone();
        ctx.on_dispose("log", move || async move {
            println!("goodbye, {name}");
            Ok(())
        })
        .await?;
        Ok(())
    });

    // 同一定义加载两次：共享一个 runtime，得到两个独立配置的 fiber。
    let first = root.load(&plugin, Config { name: "alpha".into() }).await.expect("load");
    let second = root
        .load(&plugin.clone(), Config { name: "beta".into() })
        .await
        .expect("load");
    assert_eq!(first.fiber.runtime_id(), second.fiber.runtime_id());
    assert_ne!(first.fiber.fiber_id(), second.fiber.fiber_id());

    // load 返回即接纳；operation 回执在本次激活落定时解决。
    let outcome = first.operation.wait().await.expect("settles");
    assert!(matches!(&*outcome, OperationOutcome::Active { .. }));

    // 显式、可等待、可观测的关闭；丢弃句柄不等于卸载。
    let options = ShutdownOptions { timeout: Some(Duration::from_secs(5)) };
    let report = app.shutdown(options).await.expect("shutdown");
    println!("{} disposed, {} runtimes dropped", report.fibers_disposed, report.runtimes_dropped);
}
```

## 设计要点

- **身份四分离**：definition（`Plugin<C>` + `DefinitionId`）、runtime（按 definition 每 app 一个）、fiber（每次 load 一个，配置独立）、generation（每次激活一代）。同定义多次加载 = 一个 runtime 多个 fiber。
- **协调器不执行用户代码**：单个 actor 串行决策，用户 apply/cleanup/handler 一律在受监督 worker 上运行；worker 持有 `JoinHandle` 直到 join，drop 句柄不等于取消。
- **三重核验**：generation + revision + `BindingId` 同时匹配才发布；旧 Context 注册新代返回 `StaleGeneration`，迟到成功不会提交。
- **清理是显式 async 事实**：`on_dispose`/effect 清理进入 ledger，teardown 先静默子树再按 owner-先、子逆序执行；无法确认释放的资源进 `Quarantined`，绝不谎报 `Disposed`。
- **依赖变化驱动重载**：反向依赖索引 + 依赖世界哈希 stamp；provider 换代/下线使消费者世代失效，消费者收敛到 `Pending`（等待态）并在新绑定出现时换新代激活。
- **事件即 effect**：listener 由注册作用域拥有，teardown 时退订并在途派发收尾；每事件名的模式与类型在首次注册时固定，杜绝静默混用。
- **core 不碰 serde**：serde/serde_json 只进 loader；typed API + 私有擦除边界。

一张图理解（完整语义见 [docs/01-architecture.md](docs/01-architecture.md)）：

```text
【宿主应用】App / root Context ── 显式 shutdown().await
  ├─ Plugin<C>（不可变定义，require 声明依赖）── load ─▶ PluginRuntime（按 DefinitionId 共享）
  │                                                        └─ Fiber×N（各有配置/状态/世代）
  ├─ Context 视图树 ── fork / isolate / isolate_shared ── 服务命名空间链
  └─ Coordinator（单 actor，串行决策）
       │ 有界外部邮箱 + 独立完成通道；不 await 用户代码
       ▼
【Fiber 一代】Pending ─ Starting ─ Active ─ Stopping ─ Disposed/Failed/Quarantined
  ├─ apply 在受监督 worker 运行 ─▶ 经 ctx 注册：provide/on_*/effect/spawn/子 fiber
  └─ teardown：先静默子树 → owner 先、子逆序清理 → 聚合 CleanupReport

【依赖驱动重载】provider 换代/下线
  → reverse dependency index 命中消费者 → 依赖 stamp 失效
  → 消费者旧代停止+清理 → Pending（等待态）→ 新绑定出现 → 新代激活

【配置装配】Layer(base/patch) ─ compose ─ Tree ─ mount(Registry) ─ MountedTree
  → patch 后 compose 新 Tree ─ plan（纯，keep/update/recreate/remove/insert）
  → dry-run 渲染 ─ reconcile（绑定 revision，逐节点真实结果）
```

## 与 TS / Go 版的关系

- 概念源头是 [cordis](https://cordis.moe)（TypeScript，MIT）：插件生命周期、context 树、可逆 effect、服务驱动的重新协调。
- 语义基线是兄弟项目 `../cordis-go`（基线 commit `d076943`）：生命周期状态机、依赖 epoch 传播、effect 归属及其回归测试锚点。loader 的 compose/dump 与 Go 版做了 golden 差分对拍。
- cordis-rs 是独立的 Rust 实现（未逐行翻译、未引入对方代码）；有意分歧（move-only `Next`、显式 async 清理屏障、每代取消令牌等）记录在 [docs/08-decisions.md](docs/08-decisions.md) 与 [THIRD_PARTY_NOTES.md](THIRD_PARTY_NOTES.md)。

## 仓库结构

```text
crates/cordis-core     内核：身份/错误、协调器、effect、服务、事件（Tokio，无 serde）
crates/cordis-loader   JSON 配置装配：compose/dump/registry/mount/plan/reconcile
examples/              四个可运行示例（fibers/services/events/loader，经 [[example]] 声明）
docs/                  设计文档（架构/API/运行时/服务事件loader/HMR/分期/验收/ADR）
.github/workflows      ci.yml（快速门禁）+ integration.yml（3× 全量测试、rustdoc、覆盖率）
```

## 测试与 CI

```sh
cargo fmt --all -- --check          # CI: fmt
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked     # 单元 + 集成（含 compile-fail 与 golden 差分）
cargo test --workspace --doc        # doctest
cargo doc --workspace --no-deps     # rustdoc（broken intra-doc link 视为失败）
```

CI（徽章见文首，仓库公开后生效）：`ci` 在 Linux/macOS/Windows × stable/1.85.0 上跑 fmt + clippy(-D warnings) + 全量测试；`integration` 连续 3 次全量测试（暴露 flaky）+ rustdoc 构建 + cargo-llvm-cov 覆盖率。测试约定：确定性 barrier 优先于 sleep；不合作代码用子进程/compile-fail 测试。

## 与设计文档的偏差（README 版）

设计文档（docs/01–08）保持不动；实现与草案的已知偏差：

1. **`Plugin::require`（单数）**：docs/02 草案写作 `.requires(key.dependency())`；实现为 `plugin.require(key)`，一次声明一个，重复声明在接纳时折叠。
2. **无独立 `validate` API**：docs/02 草案的 `.validate(...)` 未实现为独立方法；配置校验由插件 apply 自行完成，loader 侧由 predecode 在挂载前验证解码。
3. **无公开 `generation_cancelled()`/`fiber_cancelled()`**：取消是状态机内部事实，外部通过状态流（`watch_state`）、操作回执与 `Pending` 原因观察，不暴露额外轮询 API。
4. **统一错误 enum**：docs/02 草案的 `ServiceError` 等并入 `cordis_core::Error`；`ServiceLease::snapshot` 等返回统一 `Error`。
5. **loader 节点字段**：实现用 `name`（docs/04 草案称 `plugin`）；草案的 `isolate` 节点字段未实现——命名空间隔离目前是 core 的 `Context::isolate`/`isolate_shared` API，未接入 JSON 模型。
6. **dump 差异**（有意，golden 已固化）：dump 头行标注实现名；敏感值默认 redact 仅 Rust 版实现（Go 基线无 redaction）。
7. **dry-run 渲染**：`plan_report` 渲染节点/动作/原因/已 redact 的配置，尚未包含草案设想的"受影响依赖与停机风险"分析。

## 阅读顺序

| 文档 | 回答的问题 |
| --- | --- |
| [设计思想与架构](docs/01-architecture.md) | 为什么这样做，哪些语义保留、哪些有意不同？ |
| [Rust API 与类型边界](docs/02-api.md) | 用户怎么定义插件、拿服务、注册 effect；内部如何擦除类型？ |
| [运行时与并发协议](docs/03-runtime.md) | 加载、重载、销毁、取消和迟到结果如何协调？ |
| [服务、事件与配置装配](docs/04-services-events-loader.md) | 依赖 epoch、scope、事件五种模式、配置 patch 与 reconcile 如何实现？ |
| [HMR 与扩展运行时](docs/05-hmr.md) | 生命周期重载与代码替换有什么区别；WASM/子进程如何接入？ |
| [分阶段实现计划](docs/06-implementation-plan.md) | 从 P0 到 P9 先做什么，每步交什么？ |
| [测试与验收规格](docs/07-validation.md) | 哪些不变量必须用测试证明，如何重现异步竞态？ |
| [决策记录与未决问题](docs/08-decisions.md) | 哪些默认方案已选定，哪些必须经原型决策？ |
| [后续实现指南](AGENTS.md) | 边界与代码约定。 |
| [研究记录](findings.md) / [交付进度](progress.md) | 事实来源；已做和未做。 |

## License

MIT（见 [LICENSE](LICENSE)）。第三方概念来源与许可见 [THIRD_PARTY_NOTES.md](THIRD_PARTY_NOTES.md)。
