# 02 · Rust API 与类型边界

> 所有代码块都是**待 P0 验证的 API 草案**，不是可以直接 cargo run 的示例。错误类型、receipt 类型、构造器名称可以在 P0 调整，但应保留本文契约。

## 1. 对使用者的心智模型

1. 创建 App，取得 root Context。
2. 定义不可变的 `Plugin<C>`，声明所需服务。
3. load 返回已登记的 Fiber 和操作回执，不保证已经 active。
4. 插件启动代码通过本代 Context 注册服务、事件和清理器，然后返回。
5. 长期工作用受监督的 `ctx.spawn`，不能让 activation 永不返回。
6. 宿主显式 `shutdown().await`；丢弃一个 Context 或 FiberHandle 不等于卸载插件。

## 2. 类型化插件定义

建议公开 builder，先不要求使用者实现复杂的 async trait：

```rust
// API sketch only; support types are defined by the future implementation.
pub struct Plugin<C> { /* Arc-backed immutable definition */ }
pub struct FiberHandle<C> { /* id + Weak<AppInner> + PhantomData */ }

pub fn define<C, F, Fut>(name: impl Into<String>, apply: F) -> Plugin<C>
where
    C: Send + Sync + 'static,
    F: Fn(Context, Arc<C>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), PluginError>> + Send + 'static;
```

- Config 为 `Arc<C>`：每次 activation 使用同 revision 的不可变配置；避免无谓 Clone 约束，也不暴露可变 config 供不同 fiber 竞争。
- `.requires(key.dependency())` 声明 typed 依赖；builder 去重时保留名字与预期 TypeId，冲突报错。
- `.validate(...)` 在 worker 内每次 activation 前运行；验证不原地修改配置。规范化由 loader decoder 或调用者完成。
- define 产生新 DefinitionId；clone 不产生新身份。两个同名 define 仍是两个 runtime。
- `Plugin<C>` 内部持有 `Arc<dyn ErasedPlugin>` 与 phantom marker，但外部不能构造未校验的 erased 插件。

## 3. 加载与等待分离

```rust
// Representative method signatures, not a complete trait.
async fn load<C>(&self, plugin: &Plugin<C>, config: C) -> Result<LoadReceipt<C>, Error>;
async fn update(&self, config: C) -> Result<Operation, Error>; // on FiberHandle<C>
async fn restart(&self) -> Result<Operation, Error>;
async fn dispose(&self) -> Result<Operation, Error>;
async fn wait(&self) -> OperationOutcome;                    // on Operation
async fn wait_active(&self, deadline: Instant) -> Result<GenerationId, Error>;
```

`LoadReceipt<C>` 含 `{fiber: FiberHandle<C>, operation: Operation}`。
- `.load().await` 表示请求已被 Coordinator 登记并取得身份；父 owner 已关闭/宿主关闭等在此报错。
- `operation.wait().await` 返回此请求的结果，如 `Active{generation}`、`Pending{missing}`、`Failed{error}`、`Superseded{by_revision}`、`Disposed{cleanup}`、`Quarantined{cleanup}`。Pending 是稳定等待状态，不是启动错误。
- `wait_active` 才会跨 Pending 等未来依赖；必须允许 deadline/cancel，避免依赖永久缺失时无限等待。
- update/restart 提交新的 desired revision；被后来请求覆盖的回执必须明确 `Superseded`，不能悬挂。
- `dispose` 幂等且终态目标优先；重复调用共享或观察同一次完成报告。
- drop Operation 仅取消观察，不撤销已经接纳的变更；内部仍负责收敛、结果清理和 waiter 删除。
- **生命周期回调内禁止等待涉及自身完成的 operation**。可以提交 restart/dispose，但不要 await operation.wait；具体防死锁规则见运行时文档。

宿主调用体验示意：

```rust
let app = App::builder().build()?;
let root = app.context();
let plugin = define("metrics", |ctx, cfg: Arc<MetricsConfig>| async move {
    ctx.on_dispose("flush", move || async move { flush_metrics().await }).await?;
    Ok(())
});
let receipt = root.load(&plugin, MetricsConfig::default()).await?;
match receipt.operation.wait().await {
    OperationOutcome::Active { .. } => { /* ready */ }
    OperationOutcome::Pending { .. } => { /* dependencies not ready yet */ }
    other => { /* inspect failure or supersession */ }
}
// Later: explicit, observable shutdown.
let report = app.shutdown(ShutdownOptions::default()).await;
```

不要把该示意当已编译的接口测试；P0 必须将最终版本转为可编译示例。

## 4. 公开泛型与内部擦除

```rust
// Internal sketch. All user invocations happen on supervised workers.
type AnyConfig = Arc<dyn Any + Send + Sync>;
type PluginFuture = Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + 'static>>;

trait ErasedPlugin: Send + Sync {
    fn meta(&self) -> &PluginMeta;
    fn config_type(&self) -> TypeId;
    fn validate(&self, config: &AnyConfig) -> Result<(), PluginError>;
    fn activate(self: Arc<Self>, ctx: Context, config: AnyConfig) -> PluginFuture;
}
```

- trait 私有，方法没有类型泛型，返回 boxed Future；不把 `async fn` 或泛型 `run<C>` 直接塞到 dyn trait。
- `meta/config_type` 仅为内核生成的纯元数据；用户 validator 与 activate（含构造 future 的同步前半段）都在 worker。
- Generic adapter 检查 config TypeId，然后安全 downcast `Arc<C>`，克隆持有用户闭包，进入 async move。
- Future 必须拥有配置和 Context，避免借用 Coordinator arena 引用穿过 `.await`。
- loader 注册时建立 `Value -> C -> typed load` 的 decoder 闭包；不能根据字符串临时“生成 Rust 泛型类型”。
- core 不强制 serde；loader 的 register 才要求 `C: DeserializeOwned + Send + Sync + 'static`。
- 不必像 Go 版本一样复制一整套包级函数孪生。适配高阶函数使用闭包即可，确有重复需求再补。

## 5. 服务 API 与 Arc 约束

```rust
pub struct ServiceKey<T> { /* name + PhantomData<fn() -> T> */ }
pub struct ServiceLease<T> { /* pinned BindingId + shared value cell */ }

async fn provide<T>(&self, key: ServiceKey<T>, value: Arc<T>) -> Result<Registration, Error>
where T: Send + Sync + 'static;

async fn get<T>(&self, key: ServiceKey<T>) -> Result<ServiceLease<T>, Error>
where T: Send + Sync + 'static;

fn snapshot(&self) -> Result<Arc<T>, ServiceError>; // on ServiceLease<T>
async fn set<T>(&self, key: ServiceKey<T>, value: Arc<T>) -> Result<(), Error>
where T: Send + Sync + 'static;
```

第一版 `T` 为 Sized。需要服务接口对象时，把 `Arc<dyn Database + Send + Sync>` 装进一个具体 newtype（如 `DatabaseService`）后注册；不要声称 `Arc<dyn Any>` 能自动 downcast 成任意另一种 dyn trait。

`get` 默认只允许声明过的依赖或自有绑定，返回本代 pinned Binding 的 lease。`snapshot()` 读该 Binding 当前值，所以 Set 后再 snapshot 能见新值；已取出的 Arc 不会被原地替换，更不会被吊销。

另设 `lookup_dynamic(name)` 返回擦除的观察句柄：
- 仅供管理/诊断或主动接受动态行为的调用者。
- 不隐式新增依赖，也不自动触发使用者 reload。
- TypeId 不是可持久化 schema id，不用于 WASM 或跨进程协议。

生命周期资源服务推荐 `ManagedService` adapter（start/stop boxed future），或者用户显式 effect + provide。不得在 Rust 用反射猜对象是否实现 Starter/Stopper。接口与 shutdown 行为在 P4 原型落实。

## 6. effect、清理与任务

```rust
async fn on_dispose<F, Fut>(&self, label: &str, cleanup: F) -> Result<Registration, Error>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), CleanupError>> + Send + 'static;

async fn effect<F, Fut>(&self, label: &str, setup: F) -> Result<Registration, Error>
where
    F: FnOnce(Context) -> Fut + Send + 'static,
    Fut: Future<Output = Result<Cleanup, PluginError>> + Send + 'static;
```

- Cleanup 内部是可消费一次的 boxed closure，生成 Send Future；实现明确 success/no-op cleanup 构造器。
- setup 接收到派生 Context；通过它创建的资源是 effect children。通过原 Context 创建的资源仍是同级。
- effect body 完成时 worker 同步关闭共享注册 gate；此后 owner 不再允许新增 child，即使 Coordinator 尚未处理 SetupFinished。已接纳的子插件、listener 等继续活到释放。
- Registration 是显式控制句柄，**drop 不注销**。scope/世代才是拥有者；这样忽略 `on(...)` 返回句柄不致立刻丢订阅。
- Registration.dispose 提交目标 effect 子树的 quiesce+回收（与 Fiber 卸载同协议），外部可等完成；调用自身或祖先 cleanup 的完成等待会被拒绝，不让它变成循环等待。
- 接纳前构造的资源仍由用户局部 RAII 所有；只有登记成功后框架接管。对于获取资源后下一次 await 被取消的窗口，推荐先登记子 cleanup 或用同步 Drop 的资源 guard；框架不能回收未登记且没有 Drop 的外部副作用。

`ctx.spawn_prepare(label, task_factory)` 仅供初始化期确需运行的受控任务；`ctx.spawn_on_activate(label, task_factory)` 在接纳后登记 task effect，但直到 Active 提交才启动业务任务。两者均先登记后执行，提供本代 token，监督器持有 JoinHandle，取消时 cancel + join，必要时 abort 后仍 join。setup 不得 await 一个尚未提交 Active 才启动的 task，否则形成等待环。默认任务 Err/panic 使所属代失败并请求 teardown；正常完成不令 fiber 失败。允许以后加显式 noncritical 策略，MVP 不把后台异常静默丢弃。

不提供“在 generation Context 上启动跨 reload 的常驻 task”便利 API。长期状态放 root 插件/宿主服务，避免旧代 Context 被常驻任务反复使用。

## 7. Context、句柄与所有权

- `Context::clone/fork/isolate` 创建不可变视图，不凭 clone 延长 fiber 活动期。
- generation Context 在 unload 后 `provide/on/load/spawn` 返回 `StaleGeneration`；不会悄悄转向新代。
- `generation_cancelled()` 与 `fiber_cancelled()` 分离，命名不能含糊。
- `App` 必须被宿主持有；Context/handle 不通过强引用让 App 永远活着。
- App Drop 最多发起尽力停止并诊断未 shutdown；不 block_on，也不承诺 async cleanup 完成。
- FiberHandle 可以类型擦除为 ErasedFiberHandle 做列表/诊断；擦除句柄不提供无类型 config update。loader 保留自己的 typed updater closure。

## 8. 错误与可观察性

错误至少区分：`HostClosed`、`InactiveScope`、`StaleGeneration`、`InvalidConfig`、`UndeclaredDependency`、`ServiceMissing`、`ServiceTypeMismatch`、`ServiceExists`、`InvalidOwner`、`WouldDeadlock`、`ActivationFailed`、`TaskFailed`、`CleanupFailed`、`Quarantined`。

状态诊断含 fiber/definition/generation/revision、依赖缺失原因、最后错误、effect 数、worker 数。清理错误聚合，不因一个 disposer 返回 Err 而跳过其他独立资源。panic=unwind 可在 worker 边界报告；panic=abort、OOM、进程信号不是可恢复的插件错误。
