# 04 · 服务、事件与配置装配

## 1. 服务键、作用域与身份

### 1.1 键与隔离

`ServiceKey<T>` 携带 ServiceName 与预期 TypeId。注册表索引为 `(ServiceName, ScopeId)`；TypeId 是校验值，**不加入允许同名异型共存的索引**，避免不同调用者因为类型不同看到两个“同名服务”。

ScopeId 使用不可混淆的结构化变体：`Default(ServiceName)`、`Unique(ScopeNonce)`、`Shared(Label)`。因此共享标签不会意外撞上默认作用域或内部生成字符串。

Context 保存不可变的服务名到 ScopeId 映射链：最近祖先映射获胜。`fork` 继承视图；`isolate(name)` 创建新 Unique；`isolate_shared(name,label)` 合并指定 name 的命名空间。

**不同于 Go 基线**：Go 只按 label 占槽；Rust 用复合键，同一 label 可用于不同服务，互不冲突。isolate/fork 不新建 fiber、也不自行拥有新生命周期，仍受原 owner/generation 限制。

### 1.2 BindingId 与 value revision

一个 Binding 含：
- `BindingId = (ProviderFiberId, ProviderGenerationId, RegistrationSeq)`；seq 在 App 内单调唯一。
- name/scope/type，当前 `Arc<dyn Any + Send + Sync>` 值，value_revision。
- provider readiness、显式 availability、单调递增的 availability generation、retired gate、effect owner。

generation 的 dependency stamp 是按键排序的 `(BindingId, availability generation)` 向量；比较 binding 身份以决定重绑重载，比较 availability generation 防 false→true 快速翻转后旧 Starting 代越过校验。required 缺失时为 `Unavailable{reasons}`，不是空向量；零依赖的可用 stamp 则为空向量，二者必须可区分。value_revision 仅表示 Set 值变化，不参与重载 stamp。

| 变更 | dependency stamp | 结果 |
| --- | --- | --- |
| provider 换 Fiber | 改变 | consumer 重载 |
| 同 Fiber restart | generation/seq 改变 | consumer 重载 |
| 解除注册后再 provide | seq 改变 | consumer 重载 |
| 同 binding set 新值 | 不变 | 新 snapshot 见新值，不重载 |
| availability false | 不可用，generation 增一 | 同一 actor 转移中使相关 Starting ticket 失效、关闭 active consumer gate，驱动停止并 Pending |
| availability true | 同 BindingId 但 generation 再增一 | 旧 Starting 结果与当前 stamp 不符，不可发布；旧代清理后创建新代 |

### 1.3 发布、读与撤销

- Starting 时 provide 只预留槽位，外部不可见；已占槽拒绝重复 provider，不隐式栈覆盖。
- activation 成功且身份复核通过时，整代暂存的 bindings/listeners 一次发布；失败统一回滚。
- Active/root 新建 managed service 时，要等该 service 的 start 成功再 publish，避免 Go Serve 在 active owner 下先通知后 Start 的窗口。
- get 用本代 required snapshot；禁止“同一代突然查到另一个 provider”。child 不自动借用父 generation 的注入快照。
- lookup_dynamic 查实时 registry，明确没有依赖追踪保证。
- Binding retirement 先撤销发现与 snapshot 新获取许可，再做物理清理。Rust 默认比 Go pinned snapshot 更严格：retired 后 lease.snapshot 返回 ServiceRetired；以前取出的 Arc 不能撤销。
- set 只允许当前 owner fiber+generation、同 TypeId；notify 可更新诊断但 epoch 不变。重载旧 Context 无权 set 新 binding。

## 2. 依赖协调

required 依赖先由 Plugin builder 声明；loader 可追加 name-only 依赖，注册元数据里已知类型时补齐校验。重复名字期望不同类型必须报 InvalidDependency，不能取最后一个。

维护 reverse index：`(name,scope) -> FiberId set`，包括 Pending fiber。provider 可用性/身份变化时将消费者放入 dirty 队列；**availability=false 的命令接纳时即 bump generation 并同步标记所有引用旧 stamp 的 Starting/Active consumer 无效及关闭其新工作 gate，不能等 dirty 队列重算才关**。每轮重新解析全部依赖，防止只改一项却漏掉另一个同时变化的 binding。

发布前再次核验 provider active、availability 和完整 `(BindingId, availability generation)` 向量；验证与正式发布在 coordinator 的同一次串行转移中完成。worker 内初始化期间允许依赖 false→true，但旧结果始终与更新后的 stamp 不符并被清理，不能成为 Active。

循环依赖 A→B→A：两者 Pending，报告依赖环/缺失路径，不递归调用 apply。v0.1 不自动处理 optional dependency；循环检查仅诊断，不把“未来可由外部 provider 填补”的环判成永久不可恢复错误。

`provide_checked(check_closure)` 暂不复刻：用户可用性回调不能放 actor 内。用显式 `set_available(bool)` 发命令，可观测且能主动触发依赖刷新。需要外部健康检测时让受监督 task 调此 API。

## 3. 事件模型

### 3.1 类型与五种模式

普通事件键 `EventKey<E>`，带返回值事件键 `QueryKey<E,R>`，middleware 键 `WaterfallKey<E,R>`。同一事件名必须登记一致的模式与 payload/response TypeId，不接受等到调用时才随机 panic。

| 模式 | handler 与调度 | 返回与异常策略 |
| --- | --- | --- |
| emit | 同步 handler，在 worker 按登记顺序运行，公开分发 API 可 async 等结果 | 收集错误后继续其余 listener，返回 DispatchReport |
| bail | 同步 handler，顺序调用 | `ControlFlow::Continue(())` 继续；`Break(R)` 停止；Err/panic 立即返回错误 |
| serial | async handler，按登记顺序 await | 同 bail 的 typed ControlFlow，不使用 JS truthiness |
| parallel | async handlers，并发上限受控，等待本次已启动工作全部完成 | 聚合每 handler 结果/错误；结果按登记顺序整理而非完成顺序 |
| waterfall | async middleware，拿到只能消费一次的 `Next<E,R>` | 可修改 E 后 next.run(E).await，可不调用以短路；final 最多一次；Err 向外传播 |

Rust `Next` move-only 比 Go “重复 next 返回 settled result”更严格；P5 用 compile-fail 测试保证调用两次不成立。水流链是 middleware，不简化为普通 fold。

同步 handler 仍可能阻塞，worker 隔离只保护 actor，不会强制安全终止；必须遵守非阻塞约定。故意阻塞/无限循环 native handler 不在可靠 shutdown 保证内。

### 3.2 登记与快照

- listener 是 effect，受 owner/generation 的 gate 管理。
- 新 generation listener 暂存，到 Active 才被选择；Active owner 的登记在接纳后生效。
- dispatch 由 coordinator 选取有序 ID 快照；运行前仍须经 invocation admission claim，防止快照后 dispose 又新起 callback。
- 一次 dispatch 中新登记 listener 不进入已有快照；prepend 最近一次放在队首，行为有测试。
- `once` 在实际 invocation admission 时原子占用，**不是取快照时提前消费**。多个 dispatch 竞争只允许一个进入；开始前撤销则不执行。
- callback 已经获准运行时 dispose 不会让它凭空消失；它计入 in-flight，quiesce 等它退出。成功 Dispose 完成后不应再有该 owner 的 managed callback 运行。
- 这是与 Go 基线事件快照语义的明确差异：Go 的旧快照可能在 owner Disposed 后才执行。Rust 更强完成屏障必须测出，不可只从 map 删除 listener。
- once handler 结束/失败后从 listener 表和 effect 账本清理；不能只解绑 bus 留幽灵 effect。

### 3.3 作用域

默认非 scoped dispatch 选择该 App 同名事件的所有活 listener；scoped 模式用当前 Context 对**事件名**的 scope 映射筛选；listener 的 Global 标志允许越过 scoped 过滤。服务与事件使用不同注册表，即使名称相同也不争用槽位。

Nested event 递归有最大深度，使用 invocation lineage 传递；并发 permit 不得导致嵌套 emit 等待被外层占用的唯一 worker。MVP 可选择预留有限 nested permit + 超限返回 ReentrantDispatchLimit，不进行无界 spawn。

### 3.4 内部事件

公开诊断流输出 FiberCreated / StateChanged / ServicePublished / GenerationRetired 等结构化事件，不让用户回调在 actor 内同步执行。watch 只保留最新状态，broadcast 可丢历史并报告 lag；它们不是持久审计日志。

依赖传播直接由 registry 触发，不能依赖用户能过滤/取消的公共事件。`internal/plugin` 的 Go 顺序只是参考，不宣称 Rust 同步回调兼容。

## 4. loader：纯配置与执行分离

### 4.1 配置模型

第一版 JSON，单个插件配置保持对象语义，与 Go 参考对齐；空配置 `{}`，明确拒绝 null/标量，不偷偷把字段缺失解释成清空。

节点至少含 `id`、`plugin`（group 可无）、`config`、`inject`、`disabled`、`isolate`、`children`。字段名称在 P6 以 golden fixtures 固化，若与 Go 的 name/group 语法不同，要提供显式迁移规则。

- NodeId 必须全树唯一；duplicate 是错误，不能最后一个覆盖。
- base layer 建树；patch layer 按 id 修改已有节点或显式 insert 新节点。
- config 整对象替换，不字段级 merge；未出现 config 表示保持原值。
- 一切未命中的 target 都进 warnings；strict 模式错误。
- 删除目标与 insert 的冲突、重复 insert ID、插入到不存在父节点、同层操作顺序必须确定：**按文档列表顺序执行**，每步以当时树为基准，末尾再做全树校验。
- 保留 provenance：每字段来源层与 patch 操作位置；dump 输出最终值与来源，但敏感值默认 redact。
- JSON 数值不能无意转 f64 丢失大整数；P6 选择 serde_json arbitrary_precision 或明确数值范围并拒绝溢出，至少对拍 Go `2^53+1`、i64/u64 边界。

`parse -> compose -> validate -> dump` 为纯数据路径；不加载插件，不开任务，不执行配置中的命令，不读取任意环境变量。后续环境插值/secret provider 是独立显式功能。

### 4.2 注册与加载

- loader Registry 持有名字到已注册 typed load/update/decode 闭包的映射。
- 重复 plugin 名默认错误；别把静态注册表当运行中的 PluginRuntime registry。
- 预解码全树配置在 worker 执行；unknown plugin/invalid config 在变更运行树前报告。
- group 用内置 composition plugin 创建独立 fiber；group 配置保存子树信息，子 fiber 挂在 group generation 的 owner 下。
- empty group 可 Active；disabled group 保留配置节点但没有 live children。
- 挂载返回 MountedTree，保存 NodeId→fiber/updater/provenance；删除必须执行显式 dispose 并等完成。

### 4.3 P6 与 P7 分开

P6 只需 compose + 初次 mount + 全量 unmount；先保证失败回收，不一开始追求 hot reconcile。

P7 才实现 `plan(old_tree,new_tree) -> ReconcilePlan` 与 apply：
- 不变节点不重启；config 变且插件身份不变走 update。
- 插件定义、inject、scope、parent 变化默认 dispose/recreate，不能静默改活动 Context。
- 同一 group 未变则保留其 identity，独立调子条目；group 边界变化才替换子树。
- disabled 切换是卸载/加载；删除顺序是 child-first 请求并等待，添加是 parent-first 接纳。
- 每次 apply 绑定树 revision；并发新计划使旧计划明确 Superseded，不能混用两套节点映射。
- dry-run 展示 node、原因、受影响依赖、停机风险，不只输出字符串 diff。
- 启动/清理部分失败返回 per-node report；不会把旧 config 又写回就声称外部副作用回滚成功。

MVP reconcile 是**非事务性的、默认先停后启**。预验证减少失败，但初始化仍可能失败；可重新部署上个配置，不保证状态/外部效果恢复。WASM 蓝绿切换是后续具备稳定代理边界的独立功能。

## 5. 诊断与运维 API

Inspect 至少能解释：某 fiber 为什么 Pending，正在等哪个 key/scope/type，绑定的是哪个 provider/generation；为何最近重载；有哪些活跃 effect/task；为什么 Quarantined。

给每次收敛分配 trace/span，携带 definition/fiber/generation/revision/operation。默认不输出完整 config、认证信息、事件 payload 或 WASM hostcall 数据。

P8 再加 JSON snapshot/dump、运行时计数器和 benchmark；不要用日志替代用户可检查的 OperationOutcome。
