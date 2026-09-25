# 07 · 测试与验收规格

> **所有测试均为计划，尚未运行。** 测试名为建议；Vxx 为稳定验收编号，可用于 issue/PR/阶段报告。Go 测试锚点只是行为参考，不表示 Rust 必须复制全部差异。

## 1. 身份与生命周期（P1–P2）

| ID | 场景 | 断言 |
| --- | --- | --- |
| V01 | 同定义 clone 两次 load，不同配置 | 1 runtime/2 fibers；配置与scope独立 |
| V02 | 同名同Rust类型的两次独立 define | 2 DefinitionId；不错误合并 |
| V03 | App隔离、旧/已删ID、Context clone/drop | 不串App；旧ID拒绝；drop句柄不卸载 |
| V04 | 正常 load、restart、dispose | receipt接纳与完成分离；状态序列正确 |
| V05 | 连续update覆盖尚未完成启动 | 旧op Superseded；只有新revision可提交 |
| V06 | Starting期间dispose、旧成功/失败晚到 | 不复活；旧资源清理一次 |
| V07 | actor外apply await ctx.provide/inspect | 无actor自锁；其他fiber继续前进 |
| V08 | 自停、自重载、callback等自身完成 | 提交可行；完成wait返回WouldDeadlock |
| V09 | 部分启动失败/validator错误 | 资源清理；failed可检查、可显式重试 |
| V10 | 同一revision/dependency stamp反复dirty | 不无限重复失败启动；新revision或stamp变化才重试 |

## 2. effect与任务（P2–P3）

| ID | 场景 | 断言 |
| --- | --- | --- |
| V11 | 顶层e1/e2/e3，e2有a/b children | 清理e3→e2 own→b→a→e1 |
| V12 | scope注册与原ctx平级注册交错 | owner准确，无并发互相收养 |
| V13 | body退出关闭gate后、SetupFinished入队前抢先处理注册；旧generationctx注册 | 前者InactiveScope，后者StaleGeneration；不能落到父/新代 |
| V14 | setup等待，dispose先到，然后返回cleanup；另测手动dispose父effect且child callback仍在途 | cleanup仍被执行一次；子树先quiesce，自己的cleanup先于children cleanup；不能在子回调运行时先释放父资源 |
| V15 | caller拿receipt前/等effect时取消 | 接纳后资源仍有owner与supervisor，无孤儿worker |
| V16 | 两个请求同时dispose同effect | 同一清理完成报告，不仅“claim过就返回” |
| V17 | cleanup已返回Err/panic，另一个已证明独立项正常 | 聚合诊断并清理独立项；进入Quarantined，不转Failed/Disposed/新代且不自动重试FnOnce |
| V18 | root/parent取消时child还在Starting | 禁止child发布；父完成包含child回收 |

## 3. 服务与依赖（P4）

| ID | 场景 | 断言 |
| --- | --- | --- |
| V19 | required缺失、出现、消失、恢复 | Pending→Active→Pending→新代Active |
| V20 | 同provider解绑再provide，ABA | seq变；consumer重载；不复用旧epoch |
| V21 | 同Binding Set新Arc | consumer不重载；下一snapshot新值；旧Arc仍旧值 |
| V22 | provider在consumer启动中失活 | 旧start结果不发布；部分effects回收 |
| V23 | 两namespace、shared label、空隔离域 | 不越域fallback；同label不同服务可共存 |
| V24 | 同名异型key/get未声明依赖/set他人服务 | 明确type/权限错误，不panic或偷偷读取 |
| V25 | managed service start等待/失败 | start前不可见；失败不留占槽 |
| V26 | explicit availability false→true 在旧Starting未完成时快速翻转；依赖环 | false立即令旧票失效并关闭active gate，true bump stamp；旧成功结果不可提交；环停Pending且无忙循环 |
| V27 | lease retire后取snapshot、既有Arc继续存在 | 新获取拒绝；不承诺既有Arc能撤销或仍可业务调用 |

Go参考：`service_binding_test.go`、`fiber_epoch_test.go`、`lookup_snapshot_test.go`、`service_isolation_test.go`。V23/V24/V27部分是有意强化，不要求对拍旧行为。

## 4. 事件（P5）

| ID | 场景 | 断言 |
| --- | --- | --- |
| V28 | emit顺序、prepend、dispatch中新增listener | 快照稳定；新listener下一轮才参与 |
| V29 | bail/serial返回Continue/Break，发生Err | 按模式短路；没有JS truthiness歧义 |
| V30 | parallel乱序完成、一个Err/panic | 受控并发；收集所有启动结果；报告排序确定 |
| V31 | waterfall改payload/包返回/不调Next | around middleware语义；final最多一次 |
| V32 | 多dispatch同时抢once、注册ack尚未返回 | once最多一次；listener和effect都回收 |
| V33 | 取snapshot后dispose、callback已获准后dispose | 前者拒新callback；后者等待drain，clean完成后无回调 |
| V34 | scoped + Global + 同名不同type/mode | scope过滤正确；冲突在登记时拒绝 |
| V35 | handler嵌套emit、自己退订、递归深度超限 | 不因permit自锁；只等接纳自退订；超限报错 |

Go参考：`events_once_test.go`、`events_waterfall_state_test.go`、`integration_events_test.go`。Rust不支持重复/保存后跨生命周期调用Next，V31增加compile-fail；V33与Go旧快照行为不同。

## 5. 失败隔离与资源（跨阶段）

| ID | 场景 | 断言 |
| --- | --- | --- |
| V36 | 业务队列满同时activation/cleanup完成 | completion lane不饿死；内存/任务有预算 |
| V37 | 协作任务cancel、abort请求尚未完成、JoinHandle drop窗口 | 监督器持续持有；真正join才算退出 |
| V38 | native不yield、已运行blocking、阻塞Drop | 子进程测试有总超时；Quarantined不冒充Disposed |
| V39 | cleanup挂起、超时、随后迟到结束 | 不危险释放依赖children；隔离可观察；晚到结果被收割 |
| V40 | 错config/key/Rc/!Send future/Next二次调用 | compile-fail；runtime decode错型另受控报告 |
| V41 | 反复1000轮reload/注册/once/dispose | 内部fiber/effect/listener/waiter/worker/retirement计数恢复基线；诊断不泄露secret |

不能仅用进程RSS完全相等作为泄漏判据：allocator/编译缓存可能保留内存。先断言活对象计数，再观察多批次稳态增长，并记录缓存预算和外部Arc持有情况。

## 6. loader（P6–P7）

| ID | 场景 | 断言 |
| --- | --- | --- |
| V42 | base/profile多层，config/inject替换 | 整体替换；未出现保持，null/标量按规则拒绝 |
| V43 | 未知id、duplicate、insert/remove先后、非法group | warning/strict明确；插后lookup反映当时树 |
| V44 | 2^53+1、i64/u64边界、超范围 | 不无声丢精度；拒绝或无损保存有fixture |
| V45 | provenance/dump/{}自定义decoder/来源容器修改 | 来源正确、{}走decode、无意外别名 |
| V46 | unknown plugin、坏config、重复register | mount前发现；已有运行树不变 |
| V47 | group实例、disabled子树、group unmount | 独立fiber与所有权正确，等孩子清理 |
| V48 | 树中后项启动失败 | 失败项和已挂项均清理；host仍可再次mount；报告保留failed诊断 |
| V49 | 仅改一个node config | unchanged fiber id不变，变项走update |
| V50 | parent/scope/inject/definition改变 | recreate而非修改旧Context |
| V51 | 连续两次plan/apply竞争 | tree revision严格，旧plan Superseded |
| V52 | 运行期失败/cleanup quarantine | per-node真实结果，不谎报全局回滚 |
| V53 | dry-run后不apply、敏感config | 无运行期副作用，默认脱敏 |

Go参考：`loader/loader_test.go`、`loader/loader_decode_test.go`、`loader/consistency_test.go`。group变为独立fiber属于有意变化。

## 7. 扩展与HMR（P9，可选）

| ID | 场景 | 断言 |
| --- | --- | --- |
| V54 | 构建失败/半写artifact/hash不符 | 不加载，不干扰当前服务 |
| V55 | WASM v1→v2先停后启 | 新行为生效；旧Store与host资源可回收；报告短暂不可用 |
| V56 | trap/无限循环/内存超限/阻塞hostcall | 各有独立限制；不把fuel当hostcall强杀 |
| V57 | 过期guest resource handle/越权import | generation gate拒绝；不给未授予能力 |
| V58 | 无状态蓝绿候选失败/切换时旧调用在途 | 失败保留旧版；切换后新call走新版，旧call有drain策略 |
| V59 | 有状态snapshot/restore/schema不匹配/并发写 | 一致性点明确；拒绝不兼容；不丢写不双写的保证仅限实现的协议 |
| V60 | 子进程崩溃/超时/断连/强杀 | provider失活；kill后wait/reap；不会遗留僵尸 |
| V61 | native旧库还持callback/thread或不合作 | 不执行不安全卸载；no-unload有预算与宿主重启说明 |

## 8. 测试方法

### 确定性竞态

用 oneshot/barrier/Notify 挡住 factory、apply、provide、commit、cleanup等具体步骤，测试控制确切顺序。不要 `sleep(50ms)` 碰运气。
- Tokio paused time 只用于协作定时器；它不能控制同步阻塞或外部线程。
- 同一测试分别跑 current_thread 与 multi_thread（>=2 workers）以暴露不同调度假设。
- 以计数器/状态和受控信号证明发生，不拿“没有超时”当唯一证明。

### 属性测试与小模型

生成有界 operation 序列：load/update/restart/provide/set/remove/dispose。每步检查 I01–I13，最终 shutdown 计数恢复基线；失败 seed 保存成回归 fixture。

Loom 只覆盖真实共享的 admission/once/value cell/notification边界，不声称把整个Tokio App塞入Loom就形式化验证所有行为。核心状态机可做不依赖executor的纯reduce模型，用于对照实际事件记录。

### compile-fail与示例

trybuild或rustdoc compile_fail保证错误config、Rc服务、!Send future、Next重用不被接受；正向示例包含dyn-service newtype与无需Deserialize的typed config。

文档示意在P0定型后转换成可编译示例；以后API变动与docs同PR。

## 9. CI建议命令

仅在真正创建Cargo workspace后执行，目前本目录运行以下命令会因无Cargo.toml失败：

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo test --workspace --doc
cargo doc --workspace --no-deps
```

按阶段增加feature矩阵；Wasmtime等可选依赖用独立job，不能只跑`--all-features`掩盖默认core不可独立编译。MSRV/stable、Linux/macOS/Windows分别验证。Miri/unsafe检查视具体模块而定，不能期待所有FFI/JIT测试都能运行。

## 10. 评审与完成报告模板

每阶段报告至少有：实现的Vxx、实际命令与结果、未通过项、资源/并发边界变化、ADR/API同步状态、下一阶段前置条件。任何测试未运行必须明确写出，不能用“已加测试”替换“测试通过”。
