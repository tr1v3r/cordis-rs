# 03 · 运行时、状态机与并发协议

> 这是核心实施规格。没有实现或基准结果。若 P0 发现不可实现的签名，应修订 API，不应绕过本文件的不变量。

## 1. 不变量清单

- **I01 单写者**：同 App 的生命周期、拓扑、注册表发布由一个 Coordinator 串行决定。
- **I02 不运行用户代码**：Coordinator 不执行 apply、decoder、validator、cleanup、handler、future factory 或用户对象的最后一次 Drop；不持有全局锁调用用户函数。
- **I03 每代独占**：同 Fiber 最多一个可用 generation；旧代所有框架任务已退出、清理器均返回成功前不启动新代。未知清理结果必须隔离，不能把“已返回”误作“已释放”。不同 fiber 可以并发。
- **I04 三重核验**：worker 结果必须匹配 FiberId + GenerationId + desired revision，且依赖 BindingId snapshot 仍有效。
- **I05 明确拥有**：副作用先登记 owner 再执行 setup；所有成功接纳的资源最终回收或进入可见的隔离报告。
- **I06 旧代无写权限**：旧 Context、sealed effect scope、取消代的迟到注册不能进入当前代。
- **I07 不丢清理器**：撤销登记与 setup 返回 cleanup 的竞态，不能导致 cleanup 永不执行。
- **I08 定义身份**：同 DefinitionId 的多个实例共享一条 PluginRuntime 记录，不共享可变配置或每代状态。
- **I09 依赖身份**：provider/generation/binding 注册身份变化引发重载；仅 value_revision 变化不引发重载。
- **I10 幂等**：一个 effect cleanup 至多启动一次；一个 operation 的所有等待者看到一致终态结果。
- **I11 如实完成**：取消、abort 请求、超时均不等于资源已经停止；未停止时不谎报 Disposed。
- **I12 终态优先**：dispose 目标不能被后到的 restart、worker success 或 update 复活。
- **I13 有界 admission**：外部命令、待启动任务、事件 in-flight 有明确限额；内部完成/退役通知不能被业务队列饿死。

## 2. 状态与目标分离

公开运行状态建议：

| State | 含义 | 可采取的下一步 |
| --- | --- | --- |
| Pending | 依赖/父 owner 未 active，当前无活跃代 | 服务变化后启动，或 dispose |
| Starting | 本代正在验证配置/初始化，资源只暂存 | 成功发布；失效/错误后进入 Stopping |
| Active | 本代已发布，可接收事件及提供服务 | 依赖/配置变化或 dispose → Stopping |
| Stopping | gate 已关闭，正在等 in-flight 和清理账本 | Pending / Starting / Failed / Disposed |
| Failed | 启动失败且框架拥有的资源均已确认释放，保存启动错误 | 新 revision、依赖身份变化或显式 restart 后重试 |
| Quarantined | 任务尚未退出，或 cleanup 超时/返回错误/panic 后资源释放状态未知；禁止自动开新代 | 输出诊断、继续监督尚在运行的工作；已返回错误的清理不能自动重试 |
| Disposed | 终态，所有框架拥有的任务已退出、清理器已成功返回 | 不可再 update/restart |

另存 `Desired{revision, enabled, config, dispose_requested}`，不把一连串命令当作必须逐项执行的动作日志。短时间三次 update 可以 latest-wins，前两次 operation 必须返回 Superseded。

`Failed` 不立即自动死循环重试。相同 `(revision, dependency_stamp)` 下启动失败只执行一次；restart 新增 revision，或依赖身份/可用性世代改变才重试。可用性世代计入启动 stamp，而不只用于唤醒；不会靠高频轮询刷 apply。

Quarantined 不是 Disposed，也不是启动新代的许可。若仅有未退出任务，任务真正结束后监督器继续清理；仅当每项 cleanup 成功才可继续向目标收敛。cleanup 返回 Err/panic 代表结果未知，不能靠 Future 已完成证明外部资源已释放，也不能擅自重试 FnOnce；默认保持隔离，交由宿主显式人工恢复/重启进程。原先超时 operation 的 Quarantined 报告不可重写，后续变化通过状态流观察。

## 3. 一次完整启动

```text
【接纳】
Context.load(plugin, cfg)
  → admission permit（限额）
  → Coordinator 验证 parent owner + generation + App 存活
  → 创建 FiberId、desired revision、runtime 关联、parent child effect
  → 发布状态 Pending + 返回 receipt

【协调】
检查 parent fiber active / effect owner 未关闭
  → 解析 required services（按 key 排序，固定 BindingId + availability generation）
  → 父 owner 尚未 active 或依赖缺失：Pending，记录父/依赖原因；load operation 可完成为 Pending
  → 就绪：分配 GenerationId、token、effect ledger，状态 Starting
  → 启动 worker（构造用户 future 和配置验证也在 worker 内）

【执行】
worker validate → apply(ctx_for_generation, Arc<C>)
  ├─ Provide/On：登记暂存资源，先不对外开放
  ├─ spawn_prepare：登记后受监督启动；spawn_on_activate：登记但等 Active 再启动
  ├─ load child：只接纳；child 等 parent active，不在这里等 active
  └─ 返回 ActivationFinished(fiber, generation, revision, result)

【提交】
Coordinator 验证三重身份 + 完整依赖 stamp（BindingId、availability generation）+ owner 状态
  ├─ 全部有效且成功：原子发布本代 bindings/listeners → Active
  │                       → enqueue dependent refresh / child refresh
  └─ 失败或过时：关闭本代 gate → Stopping → 回收部分成功资源
```

MVP 不允许 Starting 父插件等待其 child active：child 的发布依赖父 active，这会形成环。插件体只等待 child 的**接纳**，然后返回；需要子插件 ready 的业务检查放在 parent active 后的外部协调逻辑中。

`apply` 不是业务主循环；若永不返回，该代不会 active。后台持续任务一般登记 `spawn_on_activate`，确实需要支撑初始化的受控任务使用 `spawn_prepare`；不得让 apply 等自己尚未启动的 task。初始化期间外部副作用不会神奇变成事务，失败时只能按用户登记的 cleanup 补偿。

## 4. Actor 命令与 worker 协议

外部命令示意：`Load`、`Update`、`Restart`、`Dispose`、`RegisterEffect`、`Provide`、`SetValue`、`SetAvailability`、`RegisterListener`、`Dispatch`、`Inspect`、`Shutdown`。
内部消息示意：`ActivationFinished`、`EffectSetupFinished`、`InvocationFinished`、`TaskFinished`、`CleanupFinished`、`TimeoutObserved`、`RetirementFinished`。

每个异步结果携带 `{operation_id?, fiber_id, generation_id, desired_revision?, owner_id?}`。查不到活 owner 或匹配失败时进入退役路径，不把结果里的资源直接 drop 在 Coordinator 上。

### 4.1 避免 actor 自锁

**错误**：actor 收到 Load 后 await plugin.apply；apply 又 await ctx.provide；provide 要等 actor 回复。

**正确**：actor 只发起 worker并返回事件循环，worker 可以向它提交命令并等短期接纳回执。所有用户回调和清理都在 actor 外。

这一规则连 callback future 的构造也适用：`let f = user_fn(...)` 可能在返回 Future 前就 panic/阻塞；factory 本身必须放入 worker 边界。

### 4.2 队列与背压

- 外部 command channel 有界；提交 API 可 await 容量，不能默默 drop 变更。
- 任务总数、effect 数、pending Fiber 数有预算；达到上限返回 `CapacityExceeded` 或排队原因。
- completion/control lane 与外部 lane 分离。推荐内部无界通知配合严格有界的已接纳工作总数；每个工作仅一条最终完成通知，不允许进度消息无限堆积。若改为有界内部 lane，要证明 cleanup 和 shutdown 不会等满队列。
- dirty fiber 用队列 + set 去重，每轮处理有限数量再让出；服务依赖级联用迭代队列，不能递归压爆栈。
- 大 config/事件 payload 必须有入站大小限制，不能只数消息个数。

### 4.3 API future 被丢弃

这是框架最容易漏资源的窗口：命令已接纳，但调用方在拿回 receipt 前被取消。
- load/update/dispose：操作仍归 App/parent scope，继续执行；无人等待不是孤儿资源许可。
- register effect/provide/on：登记后资源归 owner，返回接收端关闭时仍随 owner 清理。
- effect setup/spawn：**由框架持有 worker**，不能依赖等待 caller future 驱动它结束。
- 尚未接纳的 payload 释放走 worker/retirement 路径；不在 actor 释放用户对象最后引用。
- 发起事件后 caller 取消：P5 默认分发继续受监督直到完成/所属代撤销，不直接 detach；最大 deadline 控制生命周期。未来如支持 cancel-on-drop 必须独立声明。

## 5. 重载与销毁

```text
【目标改变】
Update / Restart / 依赖 epoch 改变 / Dispose
  → desired revision/dirty 标记
  → 关闭 generation 和嵌套 owner 的 admission gate
  → 从发现表撤销服务、停止选择新监听调用、标记后代不可新激活
  → notify 下游（逻辑不可用即时可见）
  → cancel generation task / activation / callback tokens

【静默期 quiesce】
等所有框架管理的 setup、activation、callback、task 停止
  ├─ 协作结束：继续
  ├─ 可取消 async task：abort 后仍 await JoinHandle 完成
  └─ 不 yield / 已运行 blocking / 清理挂起：Quarantined，不开新代

【清理】
按 ledger 顶层逆登记顺序处理
  → 每项先 own cleanup
  → 再逆序处理 children
  → child Fiber disposal 的完成也属于这里的 barrier
  → 一个返回 Err/panic 的 cleanup：记录未知释放状态；只继续已证明独立的资源
  → 所有 task 退出且 cleanup 都成功后，清空 pinned bindings/config/effects，退役用户对象

【收敛】
任一任务未退出 / cleanup 未成功 → Quarantined（禁止启动下一代）
否则 dispose_requested → Disposed
否则有启动失败 → Failed
否则父/依赖缺失 → Pending
否则 → 分配下一代 Starting
```

quiesce 的入口关闭和服务发现撤销不属于 LIFO disposer 顺序，必须先发生。LIFO 指**资源清理账本**，不是所有物理行为严格逆序。

任一 cleanup **未返回/返回错误或 panic/无法确认释放**时，不能强行执行依赖它的 child 清理并声称安全。默认停在 Quarantined；仅可继续已证明独立的资源清理。cleanup 已返回错误与仍在运行是不同诊断，但两者均阻止 Disposed/下一代；错误结果不可无条件重试。

不承诺 provider 全图逆拓扑停止。依赖服务可能已不可用，用户 cleanup 应用先前取得的 Arc 做自身清理，但要容忍对端已 Stop。强 drain 服务需要接口自带 in-flight gate，见 HMR 文档。

## 6. effect setup 与 teardown 竞态

EffectEntry 内部建议状态：`Preparing -> Sealed -> Disposing -> Disposed`，并记录 `dispose_requested`、`setup_worker_id`、`cleanup: Option<Cleanup>`、`children`。

1. actor 先建立 entry 与 owner 边，才启动 setup worker。
2. setup 期间经派生 scope 登记 children；**worker 在返回/异常/被 abort 的退出路径上先同步关闭共享 admission gate，再发送 SetupFinished**。Coordinator 接纳注册时也检查该 gate；即使注册请求先入队后处理，只要处理时 gate 已关闭就必须拒绝。Sealed 是 actor 观察到完成消息后的账本状态，不是这道同步 gate 的替代品。
3. 若 dispose 先到，则关闭 gate、取消 setup 并等待它真正停止。
4. setup 自然返回 cleanup 时，即使对应 generation 过时也必须接入旧账本执行一次。
5. setup 若被成功 abort，Future 内部资源靠同步 Drop 回收；已登记 children 由账本回收。没有产生的“最终 cleanup”无法凭空执行，因此重要资源应尽早登记子 cleanup。
6. scope body 返回后的新注册（以关闭 gate 为线性化点）返回 InactiveScope，不回退到父 owner。
7. 手动 dispose 与整体 unload 共享同一个 once 状态；只提交一次 cleanup worker，其余等待者观察同一完成。

**局部手动 dispose 必须与整代卸载使用同一子树 quiesce 协议**：递归关闭目标 effect 及 children 的注册/事件/服务入口，取消并等待该子树的 setup、child Fiber、callback 与 task 停止；随后才执行目标 effect 的 own cleanup，再按 children 逆序执行 cleanup。这里等的是子节点的在途工作静默，不提前运行子节点 cleanup；所以仍保留 Go 可观察的 own-cleanup→children LIFO。若任一子工作无法退出，整个子树隔离，不先释放父资源。关闭 generation 时同样递归撤销仍 Starting 的 child 权限，避免父清理期间子插件新发布。

## 7. 重入与等待环

框架 callback 范围包括 activation、effect setup/cleanup、event handler、受监督 spawn task。
- 范围内允许 `.load/.provide/.on/.dispose().await` 等**接纳级 await**。
- 范围内禁止 `Operation::wait`、`Fiber::wait_active`、`App::shutdown` 等生命周期完成等待，统一返回 `WouldDeadlock`。需要等待的 orchestration 放到宿主外部任务。
- cleanup 自己要求 dispose 自己/祖先可被视为幂等请求，但不能等待自身完成。
- 事件 callback 递归 emit 由框架 invocation lineage 检测深度；在事件 worker 容量耗尽且嵌套事件会等待自己释放 permit 时，要报错或借用受控嵌套预算，不能永远排队。P5 默认限制递归深度并使用明确的 nested-dispatch 策略。
- task-local marker 是诊断与保守拒绝手段，不是安全沙箱；裸 tokio::spawn 不继承 marker 的情况属于用户逃逸。框架不保证检测任意应用等待图，因此宿主必须设置 deadline。

## 8. 任务监督、timeout 与公平性

Tokio `JoinHandle` Drop 会 detach。监督器必须持有 handle，正常/取消/异常都发送最终结果并 join。abort 已提交不能立刻删除记录。

timeout 的含义是「截止时间到了」，不是「操作停止了」。纯 Rust 不 yield 的代码、FFI、运行中的 spawn_blocking 可能持续工作；Quarantined 报告应含 task/effect/operation IDs、最后状态与建议宿主重启或进程隔离。

deadline 可配置并分层：startup、quiesce、单项 cleanup、overall shutdown。不得把取消了的 generation token 原样用作 cleanup 的立即取消信号；清理要用独立 deadline/token。

监督器的完成路径不能丢失：panic=unwind、factory 同步 panic、Future poll panic、worker 取消均要转换为完成消息。panic=abort 无恢复承诺。

## 9. 内存、注册表与 shutdown

- Coordinator arena 用稳定 ID，删除后不重用同一逻辑身份；如采用 slab/generational index，应将槽位代号纳入 ID。
- root 是内部常驻 fiber，App shutdown 时同样走 gate→quiesce→cleanup。
- PluginRuntime 最后一个 fiber **和未完成的接纳/retirement 关联**都消失后才删除；不能只按当前 active count 删除。
- 反向依赖索引、listener 索引、operation waiter、effect 索引都必须有释放路径和计数断言。
- 外部持有的 ServiceLease/Arc 可能延长内存寿命，但不延长服务逻辑可用性；报告区分 owner 清理完成与第三方 Arc 仍存在。
- inspect 只返回不可变元数据，不返回 coordinator 的 map 锁、用户对象或可变内部状态。
- shutdown 结束应关闭外部 admission、完成 waiter、监督 retirement；剩余未终止 native 工作则返回 Quarantined/timeout 报告，而不是伪造成功。

## 10. 第一批需要模型测试的线性化点

load 的 parent-owner 接纳、generation publication、binding retirement、effect once claim、once listener invocation claim、dispose 目标提交、operation outcome 发布。P0/P2 先以手工 barrier 和小状态机证明，再引入 Loom 对必要的原子/读快照边界做局部验证。
