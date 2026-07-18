# M11-08 五服务镜像入口证据

## 1. 结论与边界

PR-M11-08 删除在本包到期的组合 `docker/Dockerfile` 及唯一兼容例外，并在已固定 digest/toolchain/package
snapshot 的 `docker/Dockerfile.base` 中交付 Broker、NameServer、Controller、Proxy、MCP 五个显式 target。
每个 runtime 镜像只复制所属 crate 的一个 binary，使用直接 exec-form entrypoint；不存在
`ROCKETMQ_COMPONENT`、shell dispatcher 或 secret 命令行参数。

工作包完成后总进度为 **72/82**，剩余 10 个工作包：M11 4 个、M12 6 个；下一工作包为 PR-M11-09。
这里的“工作包完成”表示五镜像实现、策略守卫、动态验证脚本/workflow、信号接线和回滚边界已交付，不表示
Docker/Syft/Trivy/Cosign 动态套件已观察成功。M11-07 的远端 build 失败已定位并在本包修复，但修复后的动态
重跑结果仍未观察。M10 真实性能/HUMAN、M11 ARCH/安全默认 HUMAN、容器动态
`[TEST]`、M11、Phase 3 与最终目标态 Gate 均未签署。

| 项目 | 值 |
|---|---|
| Milestone | `M11` |
| 工作包 | `PR-M11-08` |
| GitHub Issue | `#8284` |
| 主要 owner | `docker/Dockerfile.base`、`docker/container-policy.json`、五服务 binary entrypoint、container guard/workflow |
| Cargo/public API | 无 manifest、依赖边或公共 API 变化；仅修改 3 个既有 binary 的信号等待调用 |
| 非目标 | Helm/Kustomize（M11-09）、probe/preStop/统一 drain（M11-10）、镜像发布与 Phase Gate 签署 |

## 2. 五服务冻结合同

| 服务 | crate / binary | target | config mount | data path | TCP port | 默认命令 |
|---|---|---|---|---|---|---|
| Broker | `rocketmq-broker` / `rocketmq-broker-rust` | `broker` | `/etc/rocketmq/broker.toml` | `/var/lib/rocketmq/broker` | 10911、10912 | `--configFile ...` |
| NameServer | `rocketmq-namesrv` / `rocketmq-namesrv-rust` | `namesrv` | `/etc/rocketmq/namesrv.toml` | `/var/lib/rocketmq/namesrv` | 9876 | `--configFile ...` |
| Controller | `rocketmq-controller` / `rocketmq-controller-rust` | `controller` | `/etc/rocketmq/controller.toml` | `/var/lib/rocketmq/controller` | 60109、60110 | `--config-file ...` |
| Proxy | `rocketmq-proxy` / `rocketmq-proxy-rust` | `proxy` | `/etc/rocketmq/proxy.toml` | `/var/lib/rocketmq/proxy` | 8080、8081 | `--config ...` |
| MCP | `rocketmq-mcp` / `rocketmq-mcp` | `mcp` | `/etc/rocketmq/mcp.toml` | `/var/lib/rocketmq/mcp` | 8089 | `--config ... --transport stdio` |

共同 runtime 合同为 UID/GID `10001:10001`、read-only rootfs、`/var/lib/rocketmq` volume、
`/tmp/rocketmq` tmpfs 和 `STOPSIGNAL SIGTERM`。配置路径只通过普通非敏感参数传入；证书、token、credential、
ACL material 均不进入 image command。MCP 默认保留 stdio 安全边界，声明 8089 供显式安全 HTTPS 配置使用，
不在镜像层内置开发 token 或 TLS 私钥。

无 `--target` 的普通 build 最终仍落到 `container-contract-default` 的 `/bin/true` foundation smoke，而不会
意外选择任一服务。五个发布镜像必须显式指定各自 target 和不可变 tag/digest。

## 3. 信号与配置失败边界

Broker 与 NameServer 已使用 `rocketmq-runtime::wait_for_signal`，在 Unix 同时接收 SIGINT/SIGTERM。M11-08
把 Controller、Proxy、MCP stdio 与 MCP HTTPS 从 Ctrl-C-only 等待改为
`rocketmq_runtime::wait_for_signal_result`：Docker `stop` 的 SIGTERM 由服务自身接收并进入已有 graceful shutdown，
不依赖 shell 转发。MCP stdio 在信号分支结束 transport future 后执行 audit/runtime drain；HTTPS 的 Axum
graceful-shutdown future 也使用相同信号边界。

每个镜像默认要求 `/etc/rocketmq/<service>.toml` 存在；缺失 mount 必须非零退出，不能带隐式开发配置继续启动。
仓库中的 `docker/smoke-config` 仅用于 CI 合同测试，未复制进 runtime 镜像。真实二进制解析已确认：Broker 与
MessageStore 使用 `/var/lib/rocketmq/broker`，NameServer 使用 9876，Controller 使用 60109 remoting、60110 Raft
和 Memory smoke store，
Proxy 使用 8081（可选 remoting 8080），MCP 完成配置/bootstrap 后因测试 stdin 关闭按协议结束。

M11-09 在把 Controller 接入三节点 StatefulSet 时发现，原 M11-08 镜像合同只暴露 `60109`，而 remoting
listener 与 OpenRaft gRPC listener 必须分端口。后续合同因此修正为 remoting `60109`、Raft `60110`，并增加
只覆盖本地 Raft bind address 的环境变量；本证据保留该追溯记录，不把原单端口声明继续当成正确生产合同。

## 4. Guard、动态 workflow 与供应链

`container_image_guard.py` 把五服务 package/binary/target/config/data/port/command 作为非自引用的精确基线，
拒绝缺失或额外 target、错误 owner binary、shell/component dispatch、secret 命令参数、Ctrl-C-only 回退、遗留
Dockerfile/例外复活及动态 smoke/scan/signature 弱化。9 组正向/故意违规测试覆盖这些失败面。

`service-image-contract.ps1` 在 Ubuntu workflow 中对每个服务执行：

1. 显式 target build，并检查数值用户、entrypoint、command、labels、ports 和 runtime 仅有一个 owner binary；
2. 缺配置 mount 必须失败；read-only rootfs 下 `/opt/rocketmq` 不可写、data volume/tmpfs 可写；
3. 挂载 smoke config 独立启动，发送真实 SIGTERM，30 秒内以 0 退出；
4. 逐镜像生成 CycloneDX、执行不忽略 unfixed 的零 CRITICAL Trivy、Cosign sign/verify SBOM bundle；
5. 生成含 source commit、image ID、immutable inputs 与 hash 的 provenance，并在所有路径删除 ephemeral private key。

workflow 保持 `contents: read`，不申请 `packages: write` 或 `id-token: write`，因此本包不会从 PR/push 路径发布
镜像。生产发布仍须使用策略允许的五服务 GHCR digest 和 keyless identity，由后续发布 Gate 显式触发。

M11-07 的两个远端 run 都在 `Build and verify runtime foundation` 失败：pinned `debian:bookworm-slim` 没有
CA bundle，直接访问 HTTPS snapshot 无法建立信任，因而 `ca-certificates`/`libssl3` 无候选。M11-08 不关闭
TLS 验证，也不退回 mutable mirror：两个 foundation stage 先通过同一 pinned Debian snapshot 的 HTTP transport
获取 Release/Packages，仍强制 Debian archive key 的 Release 签名与 package hash；只安装 `ca-certificates` 后立即
把 source 原子切回 HTTPS，再刷新同一 timestamp snapshot 并安装剩余 package。policy/guard 强制两个 stage
都保留这一 trust-bootstrap→HTTPS handoff，删除任一切换都会失败。

## 5. 验证矩阵与未观察项

| Gate | 命令/范围 | 结果 |
|---|---|---|
| Container guard | `python scripts/container_image_guard.py` | 通过：1 Dockerfile、0 exception、5 services、M11-08 |
| Positive/negative | `python -m unittest scripts.tests.test_m11_container_foundation -v` | 9/9 通过 |
| Python/PowerShell syntax | `py_compile`；两个 container PowerShell script AST | 通过 |
| Real config parse | Broker、NameServer、Controller、Proxy 的 print-config；MCP stdio bootstrap | 通过；MCP 测试 stdin 关闭后得到预期 `ConnectionClosed` |
| Rust formatting/build/lint | `cargo fmt --all -- --check`；Controller/Proxy binary test；workspace all-targets/all-features Clippy | 通过；binary 各 1/1，Clippy `-D warnings` |
| Runtime/MCP specialized | enforcing runtime audit；MCP check/default+all-features test/streamable-http Clippy/doc | 通过；MCP 82/82、104/104，本地 integration 通过，外部 cluster E2E 按既有条件忽略 |
| Workflow/Dockerfile lint | Actionlint v1.7.12；Hadolint v2.14.0 | 通过，无 finding |
| Architecture/routing/text | target/baseline/release guard、AGENTS routing、`git diff --check` | 通过：target 35、dev-only 3、release 32/32、standalone 4、Node 3、routes 8 |
| Dynamic five-image suite | Docker/Syft/Trivy/Cosign workflow | 已交付；本机/WSL 工具不可用且不等待 CI，保持 open `[TEST]` |

M11-07 push run `29633852084` 和 PR run `29633846604` 最终均失败，静态 contract step 成功，动态 foundation
build 因上述 CA bootstrap 问题失败且没有 artifact；本包已提交针对性修复，但按交付约定不等待新 CI，因此仍
没有可消费的同 commit 成功 artifact。本包不 push/sign/publish 五服务镜像，也不把 workflow 定义当作动态成功证据。远端状态若之后
变化，只能由对应 run URL、source commit、artifact hash 和 provenance 补签动态 `[TEST]`。

## 6. 回滚与剩余目标

生产回滚单位是单服务上一签名 image digest：Broker、NameServer、Controller、Proxy、MCP 可分别回滚，不要求
回滚其他四个服务，也不删除或降级 `/var/lib/rocketmq` 持久数据。源码层可整体 revert M11-08 以恢复开发构建，
但不得在生产重新启用组合镜像、root/可写 rootfs、mutable tag 或无 SBOM/scan/signature 的路径。

剩余 M11 目标为：PR-M11-09 Helm/Kustomize、M11-10 probe/preStop/drain、M11-11 Kind/K3d fault matrix、
M11-12 ArcMut/stable/SLO 收口。M12 仍有 6 个 AI Native 工作包；M10 仍缺真实固定硬件 baseline/candidate、
原始数据 hash 与 `[HUMAN]` Gate。
