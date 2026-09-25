# 06 · 分阶段实现计划

> **全部代码阶段尚未开始。** 下面的“完成定义”是未来验收要求，不是本轮验证结果。不要用本文复选框的规划完成状态代替 Cargo/测试证据。

## 1. 执行原则与依赖图

```text
P0 原型与语义冻结
  → P1 骨架 / 身份 / 错误 / CI
    → P2 最小状态机 / receipts / supervisor
      → P3 effect / 显式 scope / async cleanup
        → P4 services / dependency epoch / isolation
          → P5 typed events / diagnostics → 内核 MVP
            → P6 JSON loader / group / 初次 mount
              → P7 reconcile plan / 增量应用 → 配置版
                → P8 加固 / 文档 / 平台与性能 → 发布候选
                  → P9 可选 WASM / process / dev HMR
```

P2/P3 有接口耦合：P2 先做最小 generation journal，P3 扩展嵌套 effects，不能在 P2 无监督地 spawn 到处运行再等 P3 收拾。

每阶段遵循：写验收失败用例 → 最小实现 → 属性/竞态测试 → 示例与文档 → 独立检查。一个提交一项逻辑变更，提交时应可独立 build/test；公开 push/PR/release 前再确认。不在文档任务阶段提前执行这些动作。

## 2. P0：先证明可实现，再铺功能

**目标**：消除 dyn/Send、重入、取消与状态语义的不确定性。

任务卡：
- P0.1 确认项目命名、许可；核对 Go/TS 上游源码的授权，记录版本/commit 和允许借鉴/复制方式。尚未确认前只借鉴概念，不整段复制实现。
- P0.2 选择 stable toolchain、edition、MSRV、Tokio/futures 等精确版本；无 Wasmtime 的 core 能独立 build。
- P0.3 编译 typed `Plugin<C>` builder→private erased adapter→boxed future 原型；两种配置通过同一 erased 调度，错类型受控拒绝。
- P0.4 编译 ServiceKey/lease/newtype dyn-service 示例；证明不会要求 future: Sync，也不会强制 typed config: Deserialize。
- P0.5 30–100 行规模的 actor+worker 原型：apply await ctx.provide 时 actor 仍回复 inspect；同步 future factory 不在 actor 执行。
- P0.6 演示 callback 自停只拿 receipt，外部可等完成；callback 内 wait 返回 WouldDeadlock。
- P0.7 原型 generation 失效、迟到 cleanup、取消 caller 等接纳后资源所有权；监督器保留 JoinHandle。
- P0.8 给 user Drop 退役路径做概念验证；测试用阻塞 Drop 在子进程执行，actor 仍能检查健康但 retirement 不伪完成。
- P0.9 冻结事件模式的 typed signature 与 scoped key 模型；列出 Go→Rust 有意差异。

**交付**：临时 spike 测试/示例、toolchain/依赖锁定、更新 ADR/API；prototype 不被误当完整生产模块。
**退出标准**：API compile-pass/compile-fail、至少一次 stale completion 和 self-stop 确定性测试通过；没有依赖此阶段“以后再解决”的核心死锁问题。
**规模参考**：中等不确定性；先 timebox 2–4 个专注工作日作为估算，超时就缩小 API，不同时扩展 HMR。

## 3. P1：工程骨架与稳定身份

- P1.1 创建 Cargo workspace（先 core crate）、fmt/clippy/test/doc CI，加入 stable 与选定 MSRV。
- P1.2 ID newtypes、DefinitionId 分配、Fiber/Generation/Effect/Binding/Operation ID；Debug 不泄漏配置。
- P1.3 错误 enum 与 CleanupReport/OperationOutcome 草案；建立文档示例测试。
- P1.4 App builder/root/Context view/Weak handle；明确显式 shutdown 与 Drop best-effort。
- P1.5 Definition builder 不可变，clone 同身份；同名不同 definition 的两个 runtime 测试。

**验收**：V01–V03、V40 部分（见测试矩阵）；core 无 serde/wasmtime/notify 强依赖；无 runtime单例。
**规模**：小；基础模块不要先写宏、插件 discovery 或 CLI。

## 4. P2：串行协调与异步生命周期

- P2.1 bounded 外部 mailbox、独立内部 completion、dirty 去重队列、公平 reconcile budget。
- P2.2 load/update/restart/dispose、desired revision、operation receipt、Pending/Starting/Active/Stopping/Failed/Quarantined/Disposed。
- P2.3 activation worker 与监督器；factory/validate/poll panic边界；每代 token/lifetime token。
- P2.4 staging/commit、迟到完成核验；dispose 优先与 update latest-wins。
- P2.5 完成等待与 callback-origin 防自锁；root shutdown。
- P2.6 最小任务/cleanup 账本与 retirement lane，避免任意用户 Drop 占 actor。

**验收**：V04–V10、V15、V36；并发 100 个独立受控启动不混淆身份；同 fiber 同时 active generations <=1。
**演示**：load→active→restart→dispose，连续 update 后只最新目标提交；资源计数回零。
**规模**：大，高风险；不要因吞吐优化跳过确定性测试。

## 5. P3：effect、作用域与可等待清理

- P3.1 EffectEntry 先发布后 setup，Preparing/Sealed/Disposing/Disposed 与只执行一次状态。
- P3.2 显式派生 scope、sealed 后拒绝注册、原 Context 平级注册、nested LIFO。
- P3.3 cleanup 返回错误聚合、panic记录、不同阶段 timeout 报告；未停止进入 Quarantined。
- P3.4 ctx.spawn 先登记后启动、task 生命周期、正常退出与 Err/panic 策略；不能丢 JoinHandle。
- P3.5 child fiber 归属与父代取消；fork/isolate 只改变视图。
- P3.6 effect caller drop/registration ack 丢失/late setup completion 回收。

**验收**：V11–V18、V37–V39；每种交错有人工 barrier；不合作原生任务由子进程测试进程强制超时。
**演示**：outer effect 创建 listener、child、task，自主释放与父代销毁都只清理一次。
**规模**：大，高风险；上线前必须审查所有 cancellation safe 点。

## 6. P4：服务与依赖驱动重载

- P4.1 `(ServiceName,ScopeId)` 槽位，type check，BindingId/value revision。
- P4.2 typed get/lease.snapshot/set；动态 lookup 单独名字；自有与未声明读权限。
- P4.3 staged provide、active publication、explicit availability、managed start/stop。
- P4.4 reverse dependency index、required epoch、ABA、发布前核验、Failed 重试政策。
- P4.5 isolate/shared/fork、隔离域缺服务不回退、namespace诊断。
- P4.6 scope/parent/dependency 参数变化用 recreate，不直接修改活动 ctx。

**验收**：V19–V27；依赖链 A→B→C 的可用性收敛，旧 cleanup 能容忍依赖失活；不宣称 provider drain 的全图顺序。
**演示**：DB provider + consumer，消失→Pending，重新提供→新代；Set 不重跑 consumer。
**规模**：大；重点 epoch/lease语义而非容器 get 的语法糖。

## 7. P5：事件与核心诊断

- P5.1 typed EventKey/QueryKey/WaterfallKey，注册期模式/type 冲突检查。
- P5.2 emit/bail/serial/parallel/waterfall；ControlFlow、move-only Next，固定异常策略。
- P5.3 admission/once claim、订阅生命周期、in-flight drain、过期快照拦截。
- P5.4 Global/prepend/scoped、嵌套分发深度与 permit 无死锁规则。
- P5.5 inspect/report、结构化 tracing、状态 watch 与 lag 说明。

**验收**：V28–V35、V40–V41；并发 once 至多一次；Disposed clean 后无 managed callback 在运行。
**演示**：typed request bus + dependency reload，多层 middleware 短路与返回值包裹。
**内核 MVP 完成定义**：P0–P5 所有必测通过，包含失败路径；不能只以 demo 能跑判定完成。
**规模**：中到大。

## 8. P6：JSON loader 与完整挂载

- P6.1 建 loader crate，JSON node/layer/patch model、严格 validation。
- P6.2 纯 compose/provenance/dump；大整数、config 整块替换、insert/删除/顺序对拍。
- P6.3 typed register closure、predecode，未知插件/坏配置不得先改运行树。
- P6.4 group internal plugin、稳定 NodeId、MountedTree、全量 unmount 完成报告。
- P6.5 mount 部分失败清理，root 保持可继续使用；disabled group 不挂子树。
- P6.6 可选最小 CLI dump/check，仅本地解析，默认不执行命令。

**验收**：V42–V48；golden fixture 对照 Go，并列出有意变化；core 不依赖 loader。
**演示**：base/profile 两层组合+group挂载+卸载。
**规模**：中；reconcile 暂不放进这一提交序列。

## 9. P7：配置 reconcile

- P7.1 纯 plan，明确 keep/update/recreate/remove/insert；NodeId/parent/scope/inject/definition 比较规则。
- P7.2 dry-run 展示变更原因与影响范围；敏感值脱敏。
- P7.3 apply 绑定 tree revision，序列化/取代并发 plan，操作完成与 report 对齐。
- P7.4 unchanged fiber identity 保持；group内部变更可只动变动孩子。
- P7.5 失败报告、上个配置重部署、quarantine阻止错误复用；不宣称跨插件事务回滚。

**验收**：V49–V53；未变插件不重启；坏配置不改 desired；运行期失败报告真实状态。
**规模**：中到大。

## 10. P8：加固与发布候选

- P8.1 current-thread/multi-thread Tokio、Linux/macOS/Windows、MSRV/stable CI。
- P8.2 属性测试生成 operation 序列；Loom 只测必要的 admission/once/read-cell 原子边界。
- P8.3 多次 reload/dispose 稳态资源基线；负载下 mailbox/worker/retirement 有界。
- P8.4 benchmark：启动/停止/Set/依赖级联/事件吞吐与 tail latency；记录硬件、toolchain、版本，不设置虚构性能指标。
- P8.5 公开 rustdoc、最小示例、migration/兼容清单、错误与隔离运维指南。
- P8.6 license/third-party notice/crate命名/供应链检查，再提发布确认。

**验收**：所有 MVP 与 loader 必测通过，无 unresolved critical issue；P8 报告注明未经覆盖的平台/特性。
**规模**：中；观察到瓶颈再优化 actor 读路径，不预先引入无锁复杂度。

## 11. P9：可选 HMR

可分开交付，不要求全部做：
- P9.1 静态宿主 dev 重启：watch→cargo build→成功后替换进程；名称明确不是进程内 HMR。
- P9.2 Wasmtime ABI spike：选 core wasm/Component版本、锁依赖，init/invoke/shutdown+capability。
- P9.3 每代 Store 与先停后启替换；配额、guest trap、取消、旧 handle拒绝、cache预算。
- P9.4 无状态稳定 endpoint 蓝绿：候选失败保留旧版、流量切换、在途 drain。
- P9.5 有状态协议：schema、snapshot一致性、restore失败、去重与fencing；独立于P9.4验收。
- P9.6 process adapter：握手、断连、kill/wait/reap、重启和状态策略。
- P9.7 native动态库仅实验，默认 no-unload；是否投入必须另行立项。

**验收**：V54–V61；不以 add.wasm 演示替代所有生命周期、权限、错误场景。
**规模**：WASM基础中到大，生产级蓝绿/迁移大且不确定；不是“加个libloading依赖”规模。

## 12. 下一位实现者的首个任务

可直接使用下面的任务描述：

> 阅读 README、01–04、07–08；只实施 P0 的 typed/erased API、actor重入、旧代完成核验三个 spike。不要实现 watcher、原生HMR或完整loader。先确认 toolchain/MSRV/许可，再给每个 spike 编译与确定性测试。完成后更新 ADR 与 API，报告成立的假设、被否定的假设及下一阶段最小范围。未经批准不 push、不发布。

## 13. 进度维护

未来实现应在 progress.md 新开“代码实现 P0”章节，列实际 commit、命令与结果；此文任务状态应另建实现 checklist 或按阶段追加，不覆盖本轮规划交付记录。每阶段有变更时同步测试 ID 与 ADR，避免 API 文档和实现各走一套。
