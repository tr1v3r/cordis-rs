# 05 · HMR 与扩展运行时

## 1. 先准确命名能力

HMR = Hot Module Replacement（模块热替换）。它不是“重新执行同一段 apply”的别名。

| 能力 | 是否换代码 | 宿主进程 | 本项目定位 |
| --- | --- | --- | --- |
| Fiber restart / config update | 不换静态二进制代码 | 不重启 | P2–P4 核心能力 |
| 源码 watcher + cargo build + restart | 换整个宿主二进制 | 重启 | P9 dev 工具可选 |
| WASM generation replacement | 换 guest 代码与实例 | 不重启 | P9 优先实验 |
| 插件子进程替换 | 换 child 进程代码 | 不重启宿主 | P9 可选，适合复杂 I/O/native 库 |
| 原生 .so/.dylib/.dll 替换 | 换本机插件代码 | 不重启 | 实验性，非默认承诺 |

Rust 官方不提供一套通用 Cordis HMR；选择 Rust 的理由应是类型/所有权/异步生态，而不是“Rust 天生支持安全热卸载”。Go 宿主配 wazero 同样可实施 WASM 插件热替换。

## 2. 内核与扩展的分界

内核只认识 Plugin、Fiber、Generation、effect，不认识文件 watcher 或编译命令。adapter 提供普通静态插件定义，内部持有某版本 artifact 的执行实例。

`cordis-dev`：源文件监听、防抖、构建队列、可取消构建、artifact hash、原子发布、构建日志。
`cordis-wasm/process`：加载 artifact、建立协议、执行、退出与资源限额。
`cordis-loader`：比较 desired artifact hash/config，决定重建实例。

编译失败不动当前活跃插件；半写文件不可加载。使用 content-addressed artifact 和原子 rename 发布，manifest 记录 hash、ABI/schema 版本、目标规格。不要直接覆盖正在使用的原生库路径。

## 3. WASM 首个实现切片

### 3.1 运行时与生命周期

建议 Wasmtime；锁稳定版本并复核 feature/MSRV。区分 Engine、compiled Module/Component、Store、Instance：
- Engine 可 App 级共享。
- 编译产物可按 artifact hash + runtime/version/config 缓存，缓存必须有容量/淘汰。
- **每个插件 generation 独立 Store**，由专门 worker 拥有。
- 不能在全局 Store 不断创建新版 Instance 后只 drop 句柄，普通实例资源通常要到 Store drop 才释放。
- 调用不能把 Store 内引用或 guest memory slice 留到下一次 await/调用；尽早复制到有界 host buffer。
- 同一个 Store 的调用由 adapter 序列化；吞吐不足时用独立 Store 池，不绕开 Wasmtime 借用规则。
- 关闭先撤销入口、drain/中断调用、释放 host capabilities，再丢 Store；compiled artifact 缓存按引用计数与预算管理。

这仍有 CPU/内存成本，不承诺与 native 插件等速，也不做未经测量的 Go/Rust 性能优劣结论。

### 3.2 ABI 与 WIT

P9 第一项原型可从 core wasm 的固定 `alloc/call/free` 字节 ABI 开始验证生命周期，但对外正式协议优先评估 Component Model + WIT。

最小逻辑接口（不是已经验证的 WIT 语法）：
- guest exports：`metadata`、`init(config)`、`invoke(method,payload)`、`shutdown`。
- 可选：`snapshot -> {schema,bytes}`、`restore(schema,bytes)`。
- host imports：`log`、受限 `service-call`、`event-publish/subscribe`、可控 clock/random；需要时才提供文件、网络、secret 代理。
- 错误、取消、deadline、payload 上限、request ID、resource handle lifetime 都属于协议，不能靠 Any/TypeId 自动推断。

不能跨 WASM 边界传 `Arc<T>`、Rust trait object、closure 或宿主裸指针。Rust typed 服务与 WASM 服务间必须有 codec/adapter。跨 Go wazero 运行时能否复用 artifact，取决于 core wasm / WASI / component 等能力组合，不能以“都是 wasm”保证通用。

### 3.3 安全边界

默认不给文件系统、网络和任意 WASI capability。WASM 隔离不能修复过宽 host import：若暴露 `read_any_file`，沙箱也会照做。

限制 guest memory/table/stack、hostcall payload、资源句柄数、并发、wall-clock deadline。配置 fuel 或 epoch interruption 处理 CPU 循环；hostcall 自己也要支持 deadline/cancel，guest fuel 不会强行停止阻塞 hostcall。

预编译缓存只接受自己可信生成的 artifact；不要把来路不明的 Wasmtime 原生序列化产物当普通可安全验证 `.wasm` 直接 unsafe deserialize。

## 4. 两种升级策略，不能混为一谈

### 4.1 基础策略：先停后启

1. dev 预编译并验证新 artifact 格式/协议。
2. 请求 adapter fiber 更新 artifact/config。
3. 旧 generation 按内核协议撤销、drain、清理 Store。
4. 新 generation 建 Store、init、publish。
5. 若启动失败且旧代/候选代清理均成功，fiber Failed；若任务未退出或清理错误则 Quarantined。可在允许重试时再部署旧 artifact，但状态不会自动恢复。

**优点**：复用内核单活世代规则，不需要稳定业务代理。
**代价**：有不可用窗口，consumer 会因 binding 身份变化重载；不是零停机 HMR。

### 4.2 高级策略：稳定代理下的蓝绿切换

需要一个长期存活的 `PluginEndpoint` 服务作为明确路由边界，旧/新 guest 是该代理内部的执行实例，而**不是同一 Fiber 同时存在两个 Active generation**。

```text
【准备】
构建 v2 → 校验 ABI/schema → 独立 Store 初始化 → 不发布真实订阅/定时写入
  └─ 失败：销毁候选；v1 仍服务

【切换】
无状态：actor/endpoint 原子切换 admission target 到 v2
有状态：阻止新入口 → drain v1 → snapshot → restore v2 → 校验 → 切换
  └─ restore 失败且尚未提交：按协议恢复 v1 admission

【退役】
已获准的 v1 调用完成或按策略取消
  → 解绑 v1 的 capability/event/timer
  → shutdown v1 → drop v1 Store
```

有状态方案的 snapshot 必须定义一致性点；不能一边继续写 v1、一边导出快照并承诺无丢写。跨数据库的写入、事件去重、幂等 key、主从 fencing 均由业务协议处理。

准备阶段的副作用隔离非常重要：如果 v2 init 已经发送请求/订阅写入，保留 v1 并不等于没有双写。协议至少区分 prepare/activate，或限制候选初始化纯净。

### 4.3 消费者是否重载

稳定 endpoint 的 BindingId 不变时，backend 替换默认不使 consumer reload；调用看到新实现。这只适用于协议向后兼容且消费者不持有 guest 内部对象。

接口/schema 破坏性变化需要发布新 binding 或新服务版本，触发 consumer 重建/拒绝升级。此语义类似 core 的 Set，不可同时承诺“身份不变”和“依赖自动认为换了 provider”。

## 5. 原生动态库为何不作为默认方案

`libloading` 提供平台加载接口，不提供版本协调、调用 drain 或 Rust 稳定 ABI。
- Rust 原生 ABI、trait object/vtable、泛型布局不是稳定跨版本插件协议。
- 若要稳定边界，使用 `extern "C"` + `#[repr(C)]` + 版本化函数表 + opaque handle。
- 分配/释放成对在约定的一侧执行，不能随意让两个库的 allocator 相互释放。
- panic 不得跨不允许 unwind 的 FFI 边界；必须在边界收敛为错误。
- 线程、TLS、callback、函数指针、Drop/vtable、异步任务任一还指向旧库，就不能安全卸载。
- OS 的 dlclose/FreeLibrary 也不保证立刻回收所有映射；Rust borrow checker不能证明任意外部线程都结束。
- `abi_stable` 一类工具可降低接口布局风险，但不是整个系统安全热卸载的保证。

可选原型应先采用 no-unload（旧库保留至进程退出）并明确内存增长限制，或直接用子进程。禁止通过 unsafe 伪造 'static 函数指针来“解决”库 lifetime。

## 6. 子进程适配器

适合需要 native 依赖、复杂网络 I/O、阻塞代码或更强终止能力的插件。
- RPC 版本握手、健康检查、stdin/stdout 或 Unix socket/命名管道协议。
- request ID + deadline + cancellation；payload 有界；断线使 provider unavailable。
- 持有 child handle，停止时 terminate→grace period→kill→wait/reap，不能只 kill 不回收僵尸。
- 进程崩溃影响实例，不直接破坏宿主堆；但子进程不是天然权限沙箱，文件/网络能力仍需要 OS 级限制。
- 新旧进程切换同样需要 admission、drain、state schema；状态持久化最好由宿主服务或外部存储负责。

## 7. P9 的成功定义

完成一条最小真实链路：Rust/小 WAT guest v1/v2 → 构建 artifact → adapter 运行 → 替换后新请求得到新行为 → 旧代资源归零 → 错误版本不冒充成功。

基础阶段只要求先停后启；蓝绿无状态、有状态迁移分别设置独立验收，不能把一个 `add(1,2)` 例子当作所有 HMR 问题已解决。测试矩阵见 [07](07-validation.md)。

## 8. 参考

- [Wasmtime 嵌入指南](https://docs.wasmtime.dev/)
- [Store 生命周期](https://docs.wasmtime.dev/api/wasmtime/struct.Store.html)
- [Component Model](https://component-model.bytecodealliance.org/)
- [libloading](https://docs.rs/libloading/latest/libloading/)
- [Go plugin 契约](https://pkg.go.dev/plugin) / [wazero](https://wazero.io/)

参考网页随版本变化，P0/P9 需要锁版本再次核对。上述链接提供背景，不意味着相关 Rust 原型已经编译或测试。
