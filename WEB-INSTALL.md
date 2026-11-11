# CPA Web 安装（开发预览）

当前发布：`v0.2.1-dev`，Linux amd64。支持 schema 2 自定义插件源的 CPA；
排队功能已在 v8.0.48、v8.0.49、v8.0.62 和 v8.0.67 上测试；跨优先级调度、会话亲和、CPA 自带选号和有请求时的升级在 v8.0.62 和 v8.0.67 上验证过。
宿主不支持 `scheduler_across_priorities` 时 `fallback` 不会溢出，行为与 `queue` 相同；
宿主不在调度请求中提供会话标识时，会话亲和不起作用（诊断中全部计为“无会话标识”）。不是官方商店收录项目。

## 添加插件源

在 CPA Web 配置编辑器中，为现有 `plugins` 添加 `store-sources`，保留其他配置：

```yaml
plugins:
  enabled: true
  store-sources:
    - https://raw.githubusercontent.com/2Wgmqra4ZuDMex/cpa-account-queue/main/registry.json
```

如已有其他源，将地址追加到原列表。不要覆盖已有 `configs` 或重复定义 `plugins`。
仓库及 Release 公开，不需要 GitHub Token。

进入 Web 插件商店，刷新，找到 **CPA Account Queue (Development Preview)**，
确认来源及版本 `0.2.1-dev` 后点击安装。CPA 下载 ZIP 并校验清单中的 SHA256。
此源使用 `direct` 固定版本下载，因此不依赖 GitHub 的 latest release（预发布不会自动成为 latest）。

可改用固定版本源，避免 main 指向后续版本：

```text
https://github.com/2Wgmqra4ZuDMex/cpa-account-queue/releases/download/v0.2.1-dev/registry.json
```

## 首次启用

- 在请求排空后操作；备份配置，旧 `cpa-account-concurrency` 保持关闭。
- 未配置的默认策略为并发 **2**、排队 **20**、最长等待 **60 秒**，不是无限并发。
- 默认策略文件：相对于 CPA 工作目录的 `plugins/state/cpa-account-queue/policies.json`。
  持久化挂载整个 plugins/state 目录（现有整个 plugins 挂载也可）。
- 插件页面：`/v0/resource/plugins/cpa-account-queue/ui`。
- 使用 CPA 管理密钥连接，先核对真实账号身份，再设置逐账号策略。
- 首次只做小范围试用；生产真实 OAuth 身份及插件级启停仍有待验收。
- 安装操作是否自动启用以 CPA 版本行为为准，请在空闲时安装并检查插件状态。

## 升级

- 从 0.2.1-dev 或更新版本升级：可以在有请求时安装。有请求在执行时当前版本继续限流，插件页面会提示新版本尚未生效；
  空闲时 CPA 的下一次配置重载或重启自动换成新版本，想立刻切换就关开一次插件开关。
- 从 0.2.0-dev 或更早版本升级到 0.2.1-dev 的这一次没有上述保护：安装时如果有请求在执行，插件会变成未注册、不再限流。
  安装后在 CPA 插件列表里确认状态，未注册就关开一次插件开关。

回退先停止新请求并等已有请求结束，再禁用新插件；不要通过盲目清零在途计数恢复服务。

## 后续发布

更新 `cmd/plugin/protocol.go` 的版本号，推送同名 `v<version>` 标签，
`release.yml` 会执行竞态测试、构建、ABI 验证、打包并发布预览资产。
每个版本都带有 `registry.json` 和 `checksums.txt`。安装源 main 的更新应在
该版本完成所需宿主联调后提交；工作流不会自动推广到 main 安装源。

第一版 0.1.0-dev 使用已通过 CI 和服务器隔离测试的原始二进制。
0.1.1-dev 更新管理页面：跟随官方主题、自动刷新间隔设置、可选记住本次会话；并发与排队引擎未改动。
0.1.2-dev 为账号名称增加凭据平台标签，自动刷新时同步更新平台和显示名称并保留策略草稿；未知平台保留原名，缺少元数据显示“未知平台”。并发与排队引擎未改动。
- 后续诊断版本将在不改变排队/账号选择语义的前提下，提供进程内累计计数和最近脱敏生命周期事件；安装前仍需按发布版本重新完成隔离验收。
0.1.3-dev 增加进程内累计诊断计数、最近脱敏生命周期事件和管理页面诊断展示；不改变账号选择与排队语义。
0.1.4-dev 增加固定账号满载时的 `queue`/`fallback` 配置和诊断事件；管理页面按账号主体分组、组内按账号类型排序。
0.1.5-dev 增加异步 JSONL 诊断日志、日志轮转、写入状态展示和 `/diagnostics` 管理接口。
0.1.6-dev 将诊断页面简化为日志摘要，实时计数和最近事件改为可折叠展示。
0.1.7-dev 修正账号表排序：平台优先，其次账号主体、账号类型和稳定账号 ID。
0.1.8-dev 在 request.intercept_after 阶段改选账号。该做法无效且会让记账与实际执行账号不一致，已在 0.1.9-dev 撤销，不要使用。
0.1.9-dev 重做账号选择：取得全部优先级的候选账号，`fallback` 改为“最高优先级账号都满载时溢出到较低优先级账号”，`queue` 保持只用最高优先级；诊断事件改为 `no_free_account` / `priority_overflow`。0.1.4 至 0.1.8 的 `fallback` 在生产中从未生效。
0.2.0-dev 增加插件内的会话亲和（默认开启，可在插件页面关闭）：同一会话优先回到上次使用的账号，原账号满载或不可用时改绑；同优先级的新会话分散到已绑定会话较少的账号。新增诊断计数 `affinity_kept` / `affinity_moved` / `affinity_new` / `affinity_none`。
回退到 0.1.9-dev 或更早版本之前，先在插件页面重新勾选“会话亲和”并保存：关闭亲和时策略文件会多出 `disable_session_affinity` 字段，旧版本遇到未知字段会拒绝加载。
0.2.1-dev 修复有请求时升级导致插件未注册的问题；新增选号方式开关（插件选号 / CPA 自带选号，默认插件选号）；诊断中记录会话改绑的原因（`affinity_moved_full` / `affinity_moved_paused` / `affinity_moved_not_offered`）以及排队期间是否有其他账号空出（`queued_other_free`）；插件页面显示运行中的版本。
回退到 0.2.0-dev 或更早版本之前，先把选号方式改回“插件选号”并保存：选“CPA 自带选号”时策略文件会多出 `host_selection` 字段，旧版本同样会拒绝加载。
勾选会话记忆后，密钥存于当前来源的 sessionStorage；同标签页刷新可恢复。
浏览器“恢复会话”功能可能保留会话存储，离开共享设备时请点击“清除密钥”。
