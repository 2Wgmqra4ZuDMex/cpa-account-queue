# CPA Account Queue

本地模式的 CPA 上游账号并发与有限排队插件。**开发预览，尚未获准生产部署。**

## Web 安装

公开自定义插件源（Linux amd64，开发预览）：

```text
https://raw.githubusercontent.com/2Wgmqra4ZuDMex/cpa-account-queue/main/registry.json
```

安装步骤、默认限额和版本固定方式见 [WEB-INSTALL.md](WEB-INSTALL.md)。
此源不属于 CPA 官方商店；安装不代表已完成真实账号生产验收。

## 已实现的设计

- 每个上游凭证账号独立设置并发、队列容量、等待超时、暂停接单。
- 未设置的账号继承全局默认值；无隐藏的缓存预留名额。
- 同账号 FIFO，有界等待；全局最多 1024 个等待请求。
- 同一请求重复回调幂等；切换账号先释放旧占用。
- 请求完成、失败或取消通过 `request.complete` 释放占用。
- 不在响应开始时释放流式请求，不按固定 TTL 清空正在执行的请求。
- 管理页面编辑配置，持久化保存，revision 防止覆盖其他页面的更新。
- 管理状态包含进程内脱敏诊断：累计接纳/完成/取消/超时/队列满计数，以及最近 256 条生命周期事件；不记录请求正文、密钥或原始账号/请求 ID。
- 诊断事件同时异步追加到 state 目录的 `diagnostics.jsonl`，单文件 20 MiB、最多保留 5 个轮转文件；写入失败不会阻塞请求调度。
- 账号由本插件在 `scheduler.pick` 阶段选择：有空闲名额的账号优先，其中 CPA 凭据 `priority` 高者优先，其次负载低者、已绑定会话少者；都没有空闲名额时进入相对最短的队列。
- 会话亲和（默认开启）：同一会话的请求优先回到上次使用的账号，只要该账号有空闲名额，不论其优先级。原账号满载、暂停或未被 CPA 提供（冷却、不可用）时改用其他账号，会话随之改绑。会话标识取自 CPA 在选择前生成的 `canonical_session_id` / `derived_session_id`（会话头、`prompt_cache_key`、或由消息历史推导），按模型区分；插件只保存其单向哈希。
- `pinned_full_behavior` 决定最高优先级账号都满载时的行为：`queue`（默认）只在最高优先级账号中选择并排队，与 CPA 自身的优先级规则一致；`fallback` 溢出到较低优先级账号，高优先级账号空出后恢复优先使用。两种模式都不改变单账号并发上限。
- `request.intercept_after` 只对 CPA 已选定的账号做接纳或排队，不改选账号：该阶段无法改变 CPA 实际执行的账号。
- 选号方式可在插件页面切换（默认“插件选号”）。选“CPA 自带选号”后，插件对每次选号回答“不处理”，由 CPA 配置的选择器（`routing.strategy`、`routing.session-affinity`，含压缩后续接和子代理跟随）决定账号，插件只在该账号上限并发和排队。代价：CPA 不知道哪个账号已满，请求会排在忙的账号后面，即使其他账号空闲；最高优先级账号都忙时也不会溢出到较低优先级。此模式下“满载时行为”和“会话亲和”两项设置不起作用。
- 会话改绑时在诊断中记录原因：原账号满载（`affinity_moved_full`）、被暂停（`affinity_moved_paused`）、CPA 未提供原账号，即冷却、额度耗尽或不可用（`affinity_moved_not_offered`）。排队的请求如果在等待期间有同一次选号提供的其他账号空出名额，计入 `queued_other_free`。
- 有请求在执行或排队时 CPA 要求替换插件（商店升级）：当前版本拒绝并照常限流、排队，不拒绝任何请求；CPA 保留当前版本，下一次配置重载时再试，空闲时完成替换。空闲时同意替换，从同意到 CPA 切到新版本之间（实测不超过 0.5 秒）到达的请求直接放行、不限流，计入 `handover_passed`。
- 账号名称旁显示 CPA 凭据平台标签（例如 Antigravity、Claude、Kimi）；同邮箱账号可按平台区分，缺少平台元数据时显示“未知平台”，不按邮箱或模型名猜测。
- 静态公开资源不包含账号信息；管理 API 由 CPA 管理鉴权保护。
- 管理密钥默认仅在页面内存；可勾选“记住本次会话”使用 sessionStorage，刷新后自动连接；清除密钥或鉴权失败会移除会话密钥，不使用 localStorage。
- 页面跟随同源 CPA 主面板的主题变量和主题切换；独立打开时使用官方配色并跟随系统明暗。
- 默认每 5 秒自动刷新，间隔可设 1–300 秒或关闭；刷新不覆盖未保存的策略编辑，隐藏页面暂停轮询。
- 独立插件 ID：`cpa-account-queue`。不注册响应块拦截。

## 状态与限制

- 不依赖 Redis；只约束**当前 CPA 进程**。多个 CPA 共用账号时不能提供全局上限。
- 以下三条说的是默认的“插件选号”；选“CPA 自带选号”时由 CPA 的选择器和它自己的会话亲和负责。
- 启用本插件后，CPA 内置的选择器不再参与普通请求的账号选择：`routing.strategy`（含加权轮询的权重）和 CPA 自身的 `routing.session-affinity` 对这些请求不生效。会话亲和由本插件另行实现（见上），与 CPA 的实现不完全相同；凭据 `priority`、冷却和不可用状态仍由 CPA 过滤候选账号。
- 插件的会话亲和与 CPA 内置实现的差别：绑定保存在插件进程内存，CPA 重启或插件重载后清空；空闲 1 小时后解除，不读取 `routing.session-affinity-ttl`；最多保留 8192 个会话；没有 CPA 的前缀匹配（LCP），上下文压缩改写了首条用户消息且客户端未提供会话标识时，会被当作新会话；子代理不继承父会话的账号。
- 已绑定的会话优先于账号优先级：溢出到较低优先级账号的会话，在高优先级账号空出后仍留在原账号，直到该账号满载或会话空闲超时。新会话仍先用高优先级账号。
- 同一优先级内多个账号都空闲且负载相同时，新会话落在已绑定会话较少的账号；关闭会话亲和后，固定选择 CPA 候选顺序中的第一个（按账号 ID 排序），不做轮询。加权轮询的权重始终不生效。
- 诊断计数和最近事件仅保存在当前进程内，CPA/插件重启后清空；它们不是持久化审计日志，也不能回溯重启前的请求。
- 账号以 CPA 的 `selected_auth_id` 识别，不是 New API 用户或下游 API Key。
- 配置 JSON 通过同目录临时文件替换；应将整个 state 目录持久化挂载。
- 排队 HTTP 请求不会在进程重启后恢复；客户端需重试。
- 当前原生 ABI 没有向插件直接传递取消 context。本地引擎支持 context 取消，原生桥接依赖 CPA 的终止通知与最长等待时间。已在固定版本 CPA v8.0.48 中验证：排队客户端断开后，队列在 2 秒验收窗口内移除该请求，且它未进入模拟上游。其他宿主版本仍需回归验证。
- 等待期间不发送 SSE 心跳。客户端及反代超时必须覆盖排队等待，且大量大上下文请求会占用宿主内存。
- 调低队列上限不删除已在等待的请求；只限制新请求。已有等待保持入队时的截止时间。
- 不自动清理无完成通知的执行占用，避免上游仍在运行时超额放行。管理状态可显示占用；异常恢复必须先确认请求结束。
- 插件被关闭（shutdown）后仍有在途请求时，同一版本不能靠重新启用恢复：宿主卸载后不再送达这些请求的完成通知，占用无法释放。等请求结束、或重启 CPA。
- 替换成新版本后，新版本从零开始计数：替换只在旧版本没有任何占用时发生；切换瞬间放行的请求（`handover_passed`）不计入任何一边，它们在上游结束前可能让账号短暂超过上限。
- 0.2.0-dev 及更早版本没有上述替换保护：从这些版本升级的那一次，如果当时有请求在执行，插件会变成未注册（不再限流），关开一次插件开关或重启 CPA 后加载新版本。
- CPA 宿主在插件加载失败或熔断时是否继续放行由宿主决定。本插件不宣称能在被宿主移除后仍维持硬上限。
- 原 v0.1.8 官方 Windows DLL 已复现“活动请求时 shutdown 后重新注册成功，但新请求全拒绝”的序列；还不能断言生产 Linux 问题完全同源。见 [验收记录](TESTING.md)。

## 开发

```sh
go test ./...
go test -race ./...
go vet ./...
CGO_ENABLED=1 go build -buildmode=c-shared -o dist/cpa-account-queue.so ./cmd/plugin
python3 tests/native_smoke.py dist/cpa-account-queue.so
```

Go 1.26+；构建 native library 需要 C 编译器。Windows 无 C 编译器时可以先：

```powershell
$env:CGO_ENABLED='0'
go test ./...
```

GitHub Actions 会在 Linux 进行竞态测试、动态库构建和 C ABI 模拟宿主测试，
并用官方发布的 CPA v8.0.62 和 v8.0.67（校验 SHA256）跑全部真实宿主测试：

```sh
bash scripts/build_for_host_tests.sh dist
host="$(bash scripts/fetch_host.sh 8.0.67 host)"
bash scripts/host_tests.sh "$host" dist/cpa-account-queue.so dist/cpa-account-queue-upgrade-target.so
```

每天还会用 CPA 的最新发布版跑一遍同样的测试（`host-latest`），新版本改变了插件依赖的行为时当天失败。
这些测试使用假凭据和 loopback 假上游，不调用真实模型、不读取生产凭证。

管理页面的浏览器测试不在 GitHub Actions 里运行（在 CI 机器上安装浏览器多次因下载系统包失败），改为每次发布前在本地运行：

```sh
pip install playwright==1.64.1 && playwright install chromium
python tests/ui_browser.py
```

页面改动没有经过这一步就不要发布。

## 配置示例（仅供隔离测试）

```yaml
plugins:
  enabled: true
  configs:
    cpa-account-queue:
      enabled: true
      state_path: /CLIProxyAPI/plugins/state/cpa-account-queue/policies.json
      defaults:
        max_concurrency: 2
        max_queue: 20
        wait_timeout_ms: 60000
        paused: false
```

这些默认值不是对订阅账号允许并发的承诺。按实际账号额度设置。
不要与旧 `cpa-account-concurrency` 插件同时启用。

管理页面：`/v0/resource/plugins/cpa-account-queue/ui`

管理接口（CPA 负责管理鉴权）：

- GET `/v0/management/plugins/cpa-account-queue/status`
- GET `/v0/management/plugins/cpa-account-queue/policies`
- PUT `/v0/management/plugins/cpa-account-queue/policies`

`/config` 是 CPA 的插件宿主保留接口，不能用于插件自定义策略。

状态和配置包含账号标识，仅应向管理员开放。不要将 state 文件或真实账号配置提交到仓库。

## 发布前门槛

- [x] 在原版官方 Windows DLL 中隔离复现特定生命周期全拒绝，记录与生产归因的边界。
- [x] 在 CPA v8.0.48、v8.0.62 和 v8.0.67 中验证加载、管理鉴权、调度、FIFO 排队、排队取消和流式结束；模拟上游峰值并发为 1。
- [ ] 验证正常 OAuth 账号及每账号策略与实际执行身份的映射。当前已验证禁用假凭证文件能被列出，不能据此宣称全账号执行路径已通过。
- [x] 验证容量拒绝、等待超时后的下一条请求能恢复；进一步多账号重试及冷却行为仍需验证。
- [x] 为队列生命周期增加脱敏诊断计数和最近事件环，覆盖超时、取消、完成回调及孤儿完成回调。
- [x] 在 CPA v8.0.62 中用两个不同优先级的假凭据端到端验证 `queue`/`fallback`：以模拟上游实际收到的凭据为准，核对溢出、单账号上限、全满排队、并发请求分散和恢复高优先级。
- [x] 在 CPA v8.0.62 中用两个同优先级的假凭据端到端验证会话亲和：会话头、`prompt_cache_key`、仅消息历史三种标识方式下，会话各自留在一个账号且分散到两个账号；原账号满载时改绑；关闭后恢复无亲和选择。
- [x] 在 CPA v8.0.62 和 v8.0.67 中端到端验证有请求时替换插件：当前版本继续限流排队、无请求失败、空闲时的配置重载完成替换；并用已发布的 0.2.0-dev 复现它在同一场景下变成未注册。
- [x] 在 CPA v8.0.62 和 v8.0.67 中端到端验证“CPA 自带选号”：会话留在原账号、子代理跟随父会话、无标识会话压缩后回到原账号，单账号上限仍然生效。
- [x] 在 CPA v8.0.62 和 v8.0.67 中端到端验证排队期间账号额度耗尽：请求被发往该账号一次、由 CPA 换号后在另一账号成功，流式和非流式、两种选号方式都覆盖。
- [x] 使用本机浏览器与模拟管理 API 验证连接、独立账号限额编辑保存及手机布局；尚未替代真实宿主管理页面测试。
- [ ] 在无生产流量的环境中演练安装及回退。

本项目为独立实现，来源说明见 [NOTICE.md](NOTICE.md)。
