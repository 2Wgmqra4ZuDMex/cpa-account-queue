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
- 静态公开资源不包含账号信息；管理 API 由 CPA 管理鉴权保护。
- 管理密钥仅保存在页面内存，无 localStorage/sessionStorage。
- 独立插件 ID：`cpa-account-queue`。不注册响应块拦截。

## 状态与限制

- 不依赖 Redis；只约束**当前 CPA 进程**。多个 CPA 共用账号时不能提供全局上限。
- 账号以 CPA 的 `selected_auth_id` 识别，不是 New API 用户或下游 API Key。
- 配置 JSON 通过同目录临时文件替换；应将整个 state 目录持久化挂载。
- 排队 HTTP 请求不会在进程重启后恢复；客户端需重试。
- 当前原生 ABI 没有向插件直接传递取消 context。本地引擎支持 context 取消，原生桥接依赖 CPA 的终止通知与最长等待时间。已在固定版本 CPA v8.0.48 中验证：排队客户端断开后，队列在 2 秒验收窗口内移除该请求，且它未进入模拟上游。其他宿主版本仍需回归验证。
- 等待期间不发送 SSE 心跳。客户端及反代超时必须覆盖排队等待，且大量大上下文请求会占用宿主内存。
- 调低队列上限不删除已在等待的请求；只限制新请求。已有等待保持入队时的截止时间。
- 不自动清理无完成通知的执行占用，避免上游仍在运行时超额放行。管理状态可显示占用；异常恢复必须先确认请求结束。
- 本地 drain 成功后可重新注册；有在途请求时不允许用重新启用绕过 drain。
- 宿主强制卸载期间 completion 是否仍能送达、跨动态库版本的计数连续性，尚需真实宿主验证。**第一版禁止有在途请求时热替换。**
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

GitHub Actions 会在 Linux 进行竞态测试、动态库构建和 C ABI 模拟宿主测试。
这些测试不调用真实模型、不读取生产凭证，不等同于完整 CPA 实例联调。

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
- [x] 在 CPA v8.0.48 中验证加载、管理鉴权、调度、FIFO 排队、排队取消和流式结束；模拟上游峰值并发为 1。
- [ ] 验证正常 OAuth 账号及每账号策略与实际执行身份的映射。当前已验证禁用假凭证文件能被列出，不能据此宣称全账号执行路径已通过。
- [x] 验证容量拒绝、等待超时后的下一条请求能恢复；进一步多账号重试及冷却行为仍需验证。
- [x] 使用本机浏览器与模拟管理 API 验证连接、独立账号限额编辑保存及手机布局；尚未替代真实宿主管理页面测试。
- [ ] 在无生产流量的环境中演练安装及回退。

本项目为独立实现，来源说明见 [NOTICE.md](NOTICE.md)。
