# 01 · 设计思想与架构

> 本文为目标设计，非已实现能力。关键语义的权威细节在 [运行时协议](03-runtime.md)；API 的示意声明尚待 P0 编译验证。

## 1. 问题定义

普通 DI 容器回答「现在怎样取得服务」，插件框架还要回答：

1. 依赖没准备好，插件是失败还是等待？
2. 提供者变了，消费者是否继续拿着旧对象运行？
3. 插件卸载时，监听器、后台任务、子插件、连接和定时器由谁回收？
4. 配置变了，哪些实例需要重建，怎样解释失败？
5. 异步初始化尚未结束时收到 dispose，晚到结果还能不能发布服务？

cordis-rs 将生命周期、依赖与副作用归属作为第一等模型，而不是用一个全局 HashMap 加几个回调拼装。

## 2. 五条设计原则

### 2.1 显式拥有，而非全局隐式收集

每个框架资源绑定 `(FiberId, GenerationId, EffectOwnerId)`。作用域由传入的 Context 决定，不用 thread-local 的「当前插件」归属并发任务。task-local 只允许用于诊断/重入检测，不能代替显式所有权。

### 2.2 配置目标与实际运行状态分离

`desired_revision/config/enabled/dependencies` 是目标，`generation/state/effects` 是现状。命令修改目标；reconcile 驱动状态向目标靠拢。用户代码返回成功不等于可以发布，仍需检查是否是当前目标。

### 2.3 内核掌握元数据，不执行不受控代码

一个 App 一个 Coordinator，串行修改拓扑和注册表；用户初始化、验证器、事件处理器、cleanup 在 worker 运行。不能持锁 await，不能在 Coordinator 内直接构造用户 future，也不能在它内部执行可能任意阻塞的用户对象析构。

后者是实现风险而非一句口号：设计 `RetireBatch` 把包含用户 Arc/闭包/配置的所有权转给专门 retirement worker，再由其 Drop；审查容器 remove/replace、错误返回及 shutdown 路径的最后一次释放。只保证框架自有路径，不能控制用户主动执行的析构。

### 2.4 保留强类型，不假装运行期不存在

调用处用 `Plugin<C>`、`ServiceKey<T>`、`EventKey<E>`。异构注册表必须擦除类型；将擦除收在私有边界，运行时 downcast 失败返回带上下文的错误，不使用 unchecked cast。

### 2.5 不作不可兑现的热替换承诺

Arc 不是权限撤销，Drop 不是 async shutdown，Tokio abort 不是强杀线程，WASM 不是自动迁移状态。生命周期内核先支持诚实的失败状态，再谈热替换。

## 3. 逻辑模型

| 概念 | 身份/所有权 | 不是什么 |
| --- | --- | --- |
| App | 宿主显式持有；拥有 coordinator、任务监督与 root fiber | 不是进程级单例；不是 Tokio runtime 本身 |
| Context | 不可变可克隆的能力句柄：App 弱引用、fiber/generation、scope 链、effect owner | 不是所有状态的可变大对象 |
| Plugin<C> | 冻结的定义；创建时分配 DefinitionId；clone 保持身份 | 不是插件运行实例，也不是按名称或 TypeId 合并 |
| PluginRuntime | App 内某 DefinitionId 的 live fiber 集合及元信息 | 不自动共享插件实例状态 |
| Fiber | 一次 load 对应一个；保存 desired config 与诊断；可经历多代 | 不是系统线程或 Rust Future |
| Generation | 一轮有效激活：依赖快照、任务、effects、取消 token | 不跨 restart 重用 |
| EffectEntry | 一个登记过的可逆副作用，有父 owner、状态、cleanup 与孩子 | 不等于任意 Rust 对象的 Drop |
| ServiceBinding | `(name, scope)` 槽位上的一次注册，具有唯一 BindingId | 值更新不一定等于身份更新 |

Context 树、Fiber 父子树、Effect 树、服务依赖图分别建模。Context fork 不创建 Fiber；load 创建 Fiber，并登记为父 owner 的 child effect；isolate 改服务命名空间，不自动创建生命周期。

## 4. 模块与依赖建议

先按逻辑模块实现，等边界通过测试再拆 crate，避免过早形成循环依赖。

```text
【P1–P5】
cordis-core（暂定包名；发布前查重）
  ├─ id / error / plugin / context / handles
  ├─ coordinator / state / generation / supervisor
  ├─ effect / task / retirement
  ├─ service / scope / dependency
  └─ event / diagnostics

【P6–P8】
cordis-loader ── depends on core
  ├─ config model / layer / patch / compose / provenance
  └─ registry / mount / diff / reconcile
cordis-cli ── depends on loader（默认只 dump/check，不执行配置中的 shell）

【P9 可选】
cordis-dev  ── watcher / build job / artifact publication
cordis-wasm ── depends on core + Wasmtime；不依赖 dev
cordis-process ── depends on core；IPC 协议，不依赖 dev
```

建议依赖角色：
- Tokio：调度、channels、watch、测试时钟；tokio-util：CancellationToken / 任务管理候选。
- futures-util：boxed future、并发集合、Future panic 边界；tracing：结构化诊断。
- thiserror：稳定错误种类；不要把 anyhow 变成全部公开错误契约。
- serde/serde_json：留在 loader，core 的 typed config 不强制实现 Deserialize。
- notify、Wasmtime、clap 等在各自后期模块中，核心默认依赖不携带它们。
- 实现前锁定 stable toolchain 与 MSRV；edition 2024 是建议，MSRV 不低于它及所选依赖实际要求。不从此文推断某个未验证版本能编译。

## 5. 兼容矩阵

“保留”表示行为目标，不宣称二进制或 API 兼容。

| 能力 | Rust 决定 | 说明 |
| --- | --- | --- |
| Context/Fiber/effect/依赖传播 | 保留概念 | 异步收敛替代 Go 同步 load |
| 同 definition 多 fiber | 保留 | ID 由定义句柄产生，不用插件名/配置类型代替 |
| provider UID + binding seq | 保留并结构化 | 不拼字符串，不仅比较 provider ID |
| Set 值不重载 | 保留 | lease 每次读取最新值；旧 Arc 仍指旧值 |
| 一般 get 未声明依赖 | 有意收紧 | 默认只读声明快照/自有绑定；动态观察另名暴露且不追踪 |
| Go snapshot 的祖先隐式回退 | 不保留 | child 必须声明自己的依赖，避免隐藏重载边 |
| scope 槽位只以 label 索引 | 有意改进 | `(ServiceName, ScopeId)`，不同服务不会因 label 相同冲突 |
| reload 不取消 Go fiber token | 有意改进 | generation token 每代取消，lifetime token 仅 dispose |
| Go Dispose 返回不代表清理完 | 有意改进 | request receipt 与完成等待分离；完成有清理报告 |
| effects 顶层 LIFO，owner cleanup 先于孩子 | 保留 | 但 admission 撤销/quiesce 在此之前 |
| effect body 返回后 scope 不再收养 | 保留 | 旧 scope 只能进行获准的清理/读取，禁止新资源 |
| Go 泛型方法 + 包级孪生 | 不机械复刻 | Rust 默认方法式；有真实高阶函数需要再加自由函数 |
| emit / bail / serial / parallel / waterfall | 保留用途，类型化返回 | 错误策略、同步/异步边界必须明确；见模块规格 |
| loader config 整块覆盖与来源追踪 | 保留 | JSON MVP；YAML 为后续适配器 |
| Go group 无独立 fiber | 有意改进 | group 对应内部组合插件实例，P6 实现 |
| ctx.foo / Proxy / decorator / prototype | 不复刻 | 显式 key 与方法；不先写宏模拟 JS |
| JS 插件生态与任意 npm 包 | 不兼容 | 不嵌入 JS runtime；若未来需要另立适配器 |
| 原生 Rust 代码 HMR | 非核心目标 | 不把 Rust trait object 跨动态库当安全 ABI |

## 6. 并发与生命周期的边界

MVP 支持多线程 Tokio；插件定义 `Send + Sync`，运行 future 为 `Send`。这不代表一个插件对象可以任意并行变更；每代一次 activation，框架控制的 callback 按事件模式调度。

只提供一个执行模式，暂不同时维护 `Rc/LocalSet/!Send` 版本。计算密集或阻塞插件必须使用显式适配器；它们的强终止需求走进程隔离，而不是扩展 `unsafe`。

不默认承诺依赖者已全部清理后 provider 才 Stop，也不保证 reload 零停机。插件清理必须容忍依赖正在退役。对数据库、HTTP route 等需要 drain 的场景，由服务协议/代理层提供 admission 与 lease，不让核心猜业务对象的安全停机方法。

## 7. 非目标

- 构造函数注入宏框架、全能 Web 框架、调度器或持久化工作流引擎。
- 静态 Rust 代码的任意模块热替换、任意内存状态迁移。
- 对恶意 native 插件的内存/CPU 隔离。
- 编译期证明配置中的名字依赖一定可满足。
- 恢复已发生的外部写入；卸载 effect 只回收已登记资源，不自动反转数据库事务、邮件或网络请求。

## 8. 设计变更规则

语义、类型边界和完成定义的变化必须同时更新 ADR、API、测试矩阵。P0 的原型允许修订本文，但不能以“Rust 比较难”静默弱化隔离与清理契约。尚未解决的矛盾应先标记、设计实验，再实现依赖它的功能。
