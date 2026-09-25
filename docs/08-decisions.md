# 08 · 决策记录与风险登记

> 状态“选定默认”表示后续实现的起点，不表示代码已验证。P0 可用原型证据修订；变更需同步API/运行时/验收。

## ADR 摘要

| ID | 决策 | 状态 | 原因/代价 |
| --- | --- | --- | --- |
| D01 | stable Rust + Tokio多线程；不先做!Send模式 | 选定默认 | 一个清晰Send边界，减少双份内核 |
| D02 | `Plugin<C>` builder公开；private boxed erased trait内部 | 选定默认，P0编译 | 不暴露Any，避免dyn async trait误用；擦除处有分配 |
| D03 | 一个App一个Coordinator，worker执行用户逻辑 | 选定默认，P0验证 | 串行提交可解释；吞吐靠测量，不预做无锁 |
| D04 | Context/handles弱持有App，显式shutdown | 选定默认 | 防callback强引用环；宿主必须持有App |
| D05 | definition clone同身份，名称/类型不代替身份 | 选定默认 | 保留Go重要语义 |
| D06 | 每代独立token与admission；revision、BindingId及availability generation复核 | 选定默认 | 旧ctx无法污染新代，false→true也不能复活旧结果 |
| D07 | 接纳与完成分开；callback内禁生命周期wait | 选定默认，P0验证 | 防自停止死锁；orchestration留宿主 |
| D08 | 显式async cleanup；Registration Drop不注销 | 选定默认 | 资源归scope而非返回句柄，Drop不能await |
| D09 | 整代或手动effect子树均先quiesce，再执行ledger LIFO；owner cleanup先于children cleanup | 选定默认 | 子节点静默后保留可观察回收顺序；取消不冒充清理 |
| D10 | `Quarantined`暴露未退出任务或未知释放结果，默认不重启同fiber | 选定默认 | native不能强杀，cleanup Err/panic亦不证明资源已释放 |
| D11 | typed key，`(name,scope)`槽位，BindingId不随Set变 | 选定默认 | 修正label跨名字冲突；保留Set/rebind区别 |
| D12 | get仅声明snapshot/自有服务；动态lookup单列 | 选定默认 | 依赖图完整性优先；明确不兼容Go自由Lookup |
| D13 | 不提供MVP全图逆拓扑清理或cleanup lane | 选定默认 | Arc不能证明调用全部受控；清理需容忍依赖停止 |
| D14 | 强撤销/强drain使用不暴露Arc的proxy | 后置adapter能力 | 不能由普通Arc泛型服务自动提供 |
| D15 | typed五模式事件；move-only Next | 选定默认，P0签名 | 不复刻JS truthiness/延迟可复用next |
| D16 | core不依赖serde；loader JSON先行 | 选定默认 | typed程序不应承担配置文件约束 |
| D17 | group独立fiber；P6 mount与P7 reconcile分开 | 选定默认 | 清晰子树生命周期，降低首次实现复杂度 |
| D18 | 默认break-before-make，无跨插件事务承诺 | 选定默认 | 任意effect不能自动安全蓝绿/回滚 |
| D19 | WASM按generation独立Store，Engine/cache可共享 | P9原型前重验 | 内存生命周期可控；不共享guest globals |
| D20 | 原生动态库HMR非内核，默认no-unload/process | 选定默认 | ABI与安全卸载不能靠borrow checker自动证明 |
| D21 | 默认runtime只有记录，不加隐式Shared init生命周期 | 选定默认 | 共享业务状态显式放root/service，避免额外失败层次 |
| D22 | 用户对象最后Drop走retirement worker | P0高风险门槛 | 防隐式用户代码卡actor；需审查每个错误分支 |

## 1. 三个容易被误读的决策

### 不实现全图逆拓扑，不代表没有依赖传播

consumer仍会在provider失活后停止并Pending；只是不保证consumer cleanup能继续调用正在关闭的provider。若未来需要这种保证，必须设计正常调用gate与cleanup capability lane，并证明反向依赖图及循环处理；不能只拿旧Arc就声称依赖可操作。

### Quarantined 与 Failed/Disposed 的区别

Failed/Disposed仅在框架管理的工作已退出且所有 cleanup 均成功返回时使用。cleanup 返回 Err/panic 的结果未知，与仍在运行的 cleanup/任务一样都进入 Quarantined，不能叫清理完成或自动叠加新代；报告仍区分“已返回但结果未知”与“尚未退出”，并允许完成已证明独立的资源清理。只有应用明确的人工恢复/重启流程能解除终端错误隔离，不自动重试 FnOnce。

### staging不是业务事务

框架可以推迟服务与listener发布，但无法回滚apply已经写出的数据库记录或外部请求。候选HMR初始化不双写，要靠prepare/activate协议与业务幂等，不是给generation加个标志就解决。

## 2. 实施前需要验证/决定的问题

| 问题 | 默认方向 | 验证时点与证据 |
| --- | --- | --- |
| crate/仓库名称是否可用、license如何选 | 本地叫cordis-rs，不代表发布名 | P0人工确认+上游许可核对 |
| MSRV与依赖版本 | stable、edition2024，锁实际版本 | P0 cargo check + MSRV CI |
| builder/typed handles是否易用 | config Arc<C>，无Clone/Deserialize强制 | P0可编译完整示例与trybuild |
| Operation等待与origin传播 | callback全部禁生命周期wait | P0 factory/task/event/cleanup原型 |
| retirement worker调度/最后Drop审计 | 专门隔离lane，带pending计数 | P0/P2错误路径测试；阻塞Drop子进程 |
| body返回与scope sealed线性化 | worker退出前同步关闭共享admission gate；actor接纳注册时检查gate，SetupFinished只更新账本状态 | P3确定性竞态，排队注册不得越过关闭点 |
| managed task默认启动时机 | init可启动受控准备任务；业务任务显式等active | P3防init等自身任务与提前消费测试 |
| service read cell实现 | Arc+短锁，先简单后优化 | P4无锁跨await；Set/drop旧值退役验证 |
| query/middleware具体泛型签名 | owned payload + boxed Future + consuming Next | P0/P5编译与嵌套调用测试 |
| JSON数字策略 | arbitrary_precision或明确拒绝超范围 | P6 golden大整数与自定义decoder |
| watcher构建命令权限 | 只执行显式允许的本地配置，不自动信任下载插件manifest | P9威胁模型 |
| WASI/core/component能力组合 | 锁版本+最小WIT contract | P9真实guest/host测试，不从扩展名推断 |
| 蓝绿state schema/失败补偿 | 基础先停后启，不预承诺全自动迁移 | P9.4/P9.5分开验收 |

## 3. 风险与缓解

| 风险 | 严重性 | 缓解与验收 |
| --- | --- | --- |
| actor等待用户future形成死锁 | 高 | 所有用户逻辑worker化，V07/V08 |
| 接纳后caller取消导致资源无owner | 高 | supervisor持有，V14/V15 |
| 旧generation结果覆盖当前状态 | 高 | ticket三重核验，V05/V06/V22 |
| timeout后危险释放/假成功 | 高 | Quarantined、独立cleanup预算，V37–V39 |
| Drop隐式卡actor或泄漏强引用 | 高 | retirement lane、Weak句柄、V38/V41 |
| Arc与业务可用性混为一谈 | 高 | snapshot限制/proxy边界，V21/V27 |
| 事件旧快照在已清理owner上执行 | 高 | invocation gate/in-flight barrier，V33 |
| once effect残留/多次执行 | 中高 | actor claim +统一回收，V32 |
| loader部分失败被误报原子回滚 | 中高 | per-node report与重部署语义，V48/V52 |
| 项目范围过大，HMR拖住MVP | 高 | P0–P5先交core，P9独立后置 |
| Rust ABI卸载悬垂指针 | 高 | 不默认支持，P9.7独立立项 |
| wasm hostcall权限过宽/缓存注入 | 高 | capability/限额/可信缓存，V56/V57 |
| 上游差异被误称兼容 | 中 | 基线commit+矩阵+P0补TS审计 |

## 4. 以后可以再考虑，不先实现

optional dependencies（reactive/snapshot分开）、服务强drain cleanup lane、LocalRuntime、proc macro、动态JS宿主、持久化operation日志、跨App服务、通用事务升级、全图拓扑停机、自动状态迁移、native安全卸载证明。

每增加一项先回答：是否改变generation/ownership契约？能否独立adapter实现？如何验收失败路径？是否让core强依赖更重运行时？
