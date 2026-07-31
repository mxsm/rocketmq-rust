# Control Plane 与 Connector mTLS 部署边界

## 目标

Phase 01 的 Connector 主动连接 Control Plane，通过 HTTP/2 长轮询接收只读证据查询。
非 loopback 地址必须使用 HTTPS，并同时提供服务端 CA 与 combined PEM 客户端身份。
部署层在 plain Axum 前放置专用 mTLS 代理，使证书身份成为 Connector channel 的可信
身份源，不信任请求方自行填写的 identity Header。

## 请求链路

```text
Connector
  ├─ 验证 Control Plane server CA 与 service DNS SAN
  ├─ 提交 Connector client certificate + Bearer token
  └─ HTTP/2 POST
       ↓
mTLS proxy :8444
  ├─ 仅允许 TLS 1.2/1.3
  ├─ client CA、EKU、固定 subject/issuer 校验
  ├─ 640 KiB body 上限、无 redirect、仅 Connector GET/POST 路径
  ├─ 覆盖 X-RocketMQ-Connector-Subject = $ssl_client_s_dn
  └─ 覆盖 X-RocketMQ-Connector-Issuer  = $ssl_client_i_dn
       ↓
Control Plane Connector-only Axum :8093
  ├─ constant-time Bearer token 校验
  ├─ request.subject 必须等于证书派生 subject
  └─ tenant、cluster、session、read-only capability fail closed
```

客户端证书主体固定为 `CN=rocketmq-sre-connector`，签发者固定为
`CN=RocketMQ SRE Connector Development CA`。Connector 的 request body subject
和本地配置使用同一值，因此代理覆盖 Header 后，伪造 Header 或错误证书都会在注册前
失败。Heartbeat、poll 与 response 继续绑定首次注册的证书主体、签发者和 session。
MCP capability handshake 上报及 Connector 的 cluster-state 检查使用同一
Connector-only 路径族和独立 mTLS HTTP client，绝不回退到公共 `8090` listener。

## Compose

- `sre-control-plane` 连接数据网络和内部 `control-plane-backend` 网络。
- 公共 API 使用 `8090`；Connector internal 路径只挂载在
  `127.0.0.1:8093` listener。
- `sre-control-plane-mtls` 与 Control Plane 共享 network namespace，在
  `8444` 监听；其他容器无法绕过代理直接连接 `8093`。
- Connector URL 为 `https://sre-control-plane:8444`，挂载 server CA 与
  combined client identity。
- backend 网络标记为 `internal: true`；Connector/MCP 网络不能解析或直连 Axum
  服务名。
- 代理以 UID/GID 10001 运行，root filesystem 只读，移除全部 Linux capabilities，
  只有 16 MiB 内存临时目录。

开发证书位于仓库 D 盘 worktree 的 `target/phase00-certs`，不会写入 C 盘编译目录。
`dev.ps1 -Action ChannelCerts` 只生成通道证书，不轮换现有 RocketMQ/MCP TLS 与 ACL
fixture；`dev.ps1 -Action Certs` 才会重新生成全部本地 fixture。

## Kind

- mTLS proxy 是 Control Plane Pod 的 sidecar，通过 loopback 转发到只监听
  `127.0.0.1:8093` 的 Connector-only Axum listener。
- `sre-control-plane` Service 同时声明 API `8090` 与 Connector mTLS `8444`。
- server secret 只进入 Control Plane namespace；client secret 只进入 Connector
  所在 RocketMQ namespace。
- NetworkPolicy 只允许带 Connector Pod label 的 Pod访问 `8444`，不允许其绕过代理
  访问 `8090`；bootstrap 和 smoke 仅获得各自需要的 API ingress。
- server certificate SAN 包含
  `sre-control-plane.rocketmq-sre.svc.cluster.local`。

## 生产要求

- 不得复用或分发 development CA、私钥、Bearer fixture。
- 使用短生命周期证书和独立 server/client trust root；私钥由集群 secret manager
  或 workload identity 设施注入。
- Axum connector internal 路径不得挂载到公共 API listener。Connector-only listener
  必须只接受来自 mTLS proxy 的网络流量；若部署环境没有 loopback、等价
  NetworkPolicy、service mesh authorization 或私网隔离，不得暴露该路径。
- 代理必须覆盖而不是追加 identity Header，且不得把证书、DN、token 写入应用日志。
- 不提供 `insecure_skip_verify`、普通 HTTP 降级或匿名重试开关。
- Bearer token 与 mTLS 是叠加校验；任何一层失败都 fail closed。

## 验证

```powershell
.\rocketmq-sre\scripts\dev.ps1 -Action ChannelCerts
.\rocketmq-sre\scripts\verify-mtls-deployment.ps1 -CheckCertificates
docker compose --project-directory .\rocketmq-sre\deploy\dev `
  --file .\rocketmq-sre\deploy\dev\compose.yaml `
  --profile observability config --quiet
kubectl kustomize .\rocketmq-sre\deploy\kind > $null
```

`verify-mtls-deployment.ps1` 检查代理的 TLS、body、redirect 和证书 Header 策略，
拒绝 Compose/Kind 中的 plain HTTP Connector channel，渲染 Compose/Kustomize，
并可验证 server/client EKU、service DNS SAN、combined private key 与两个 Nginx
配置。
