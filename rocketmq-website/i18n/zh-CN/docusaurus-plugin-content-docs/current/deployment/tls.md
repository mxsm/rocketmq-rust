---
title: "项目各组件的 TLS 与 mTLS 配置"
description: "RocketMQ-Rust 的证书准备、Kubernetes Secret 创建、组件配置、验证和轮换指南。"
---

TLS 需要按每条连接分别配置。本指南覆盖当前服务可执行程序、Rust 客户端、Proxy、Admin、Dashboard、MCP 和 AI SRE。TLS 加密连接并验证服务端，mTLS 还会验证客户端证书。证书验证不会授予 RocketMQ ACL 权限、OAuth scope 或 SRE 执行权限，仍需独立配置[认证与授权](security.md)。

示例使用 Linux、macOS 或 WSL 中的 Bash、OpenSSL 3；除非命令明确切换目录，否则从仓库根目录执行。请替换示例域名、地址、命名空间、Release 名称和路径。TOML 片段需要合入对应服务配置，并非完整启动文件。容器中的路径指向实际挂载到容器内的文件。

## 连接与组件支持范围 {#component-matrix}

| 连接 / 组件 | 配置入口 | 当前边界 |
| --- | --- | --- |
| 客户端 → NameServer remoting，通常为 9876 | NameServer TOML 根层的 `tls.*` 属性 | NameServer 默认包含 `tls` 编译 feature；监听器和所有调用方需要分别配置。 |
| 客户端 → Broker remoting，通常为 10911、VIP 10909 | `broker.brokerServerConfig.tlsConfig` | Broker 需编译 `tls`；主监听器和 fast/VIP 监听器继承服务端 TLS 配置。 |
| Producer、Push、LitePull、嵌入式 Rust Admin | `ClientConfig` 或 Admin builder 的 `use_tls` | 客户端 TLS 用于其 remoting 连接；仍需配置 CA 信任与可选客户端身份。 |
| 客户端 → Proxy v2 gRPC，通常为 8081 | `grpc.tls` | Proxy 的 `tls` feature 可选；此监听器支持 TLS 和 mTLS。 |
| 客户端 → Proxy remoting；Proxy Cluster → NameServer/Broker | 独立 remoting / 后端配置 | 当前 Proxy remoting 配置没有 TLS 身份字段，默认 Cluster 客户端未启用 TLS。`grpc.tls` 不配置这两条路径。 |
| Broker → NameServer/Controller/其他 Broker；NameServer 内部客户端 | 内部 remoting 客户端 | 当前启动路径使用默认明文客户端配置；监听配置中的 `tls.client.*` 不会配置另行构建的客户端。 |
| Broker HA 复制；Controller remoting 与 Raft | 独立 HA / Controller 传输 | 当前未接入完整的可执行程序级 TLS 配置。Controller Raft 客户端使用 `http://`；嵌入式 Controller 具有相同边界。 |
| MCP 客户端 → 查询 MCP | `server.http.tls` | Streamable HTTP 使用 HTTPS；stdio 没有网络 TLS。当前查询 MCP 集群配置没有出站 `use_tls` 选项。 |
| MCP 客户端 → MCP Control；Control → RocketMQ | `server.tls`；`clusters[].use_tls` | HTTPS 与出站 remoting 策略独立；入站 TLS 不会启用变更工具或授予变更权限。 |
| SRE Connector → MCP / Control Plane / Kubernetes | 独立 HTTPS CA 与 mTLS 身份设置 | Control Plane 通道由专用代理终止 mTLS，必须隔离 HTTP 后端。 |
| SRE Executor / Agent / Model Gateway | 工作负载 mTLS 代理或提供者传输 API | Executor/Agent 的 Axum 监听器不终止 TLS；Model Gateway HTTP 与 Provider SPI 客户端有独立 TLS 配置。 |
| Web / Tauri / GPUI Dashboard → RocketMQ | 连接 TLS 设置；Web 使用 `DASHBOARD_WEB_USE_TLS` | 不会配置浏览器 HTTPS 或 SQL TLS；桌面应用使用本地进程的信任配置。 |
| 浏览器 → Web Dashboard / SRE UI；SQL；遥测 | HTTPS 网关、数据库驱动、Collector 配置 | 独立信任与 Secret 所有权，没有统一 RocketMQ TLS 开关配置这些端点。 |

完整加密部署需要覆盖实际使用的**每条连接**。NameServer 强制 TLS 后，标准 Broker 明文注册客户端无法注册；Broker 强制 TLS 后，标准 Proxy Cluster 后端和查询 MCP 可能无法访问。对于这些组合，需要明确接入出站 TLS 适配器，或部署外部加密传输边界，并保持路由与访问限制一致。私有网络可以限制暴露，但本身不加密流量。现有 Core Helm TLS profile 不能称为全链路 TLS。

来源：[NameServer 启动入口](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-namesrv/src/bin/namesrv_bootstrap_server.rs)、[Broker 监听器](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-broker/src/broker_runtime/request_pipeline/startup.rs)、[Broker 出站装配](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-broker/src/broker_runtime/composition.rs)、[Proxy Cluster 客户端](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-proxy-cluster/src/cluster.rs)、[Controller 装配](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-controller/src/controller/controller_manager.rs)。

## 准备证书与信任根 {#certificates}

为各工作负载准备以下材料：

| 材料 | 使用方 | 要求 |
| --- | --- | --- |
| 服务端证书链与私钥 | TLS 监听器 | PEM 证书链，叶证书在前、中间证书在后；私钥匹配；包含 `serverAuth` EKU 与实际端点的 DNS/IP SAN。 |
| 服务端 CA bundle | 客户端 | 能验证服务端的公开 CA 证书，不包含 CA 私钥；remoting 显式信任 bundle 会替代系统信任根。 |
| 客户端证书链与私钥 | mTLS 客户端 | 使用独立工作负载身份，包含 `clientAuth` EKU，证书和私钥必须成对提供。 |
| 客户端 CA bundle | mTLS 服务端 | 允许签发客户端身份的公开 CA 证书。`optional` 模式仍验证实际提交的证书。 |

SAN 必须匹配客户端实际校验的地址，包括 NameServer 发现域名、路由通告的 Broker IP/域名、Proxy 外部域名，以及 Kubernetes Service/peer DNS。只有 `localhost` 的证书不覆盖 Service 名称；只有域名的证书不覆盖直接使用 IP 的连接。Core chart 部署前，先根据 Release 和命名空间渲染并确认 Service、peer 名称，再申请证书。

生产环境使用组织 CA 或证书控制器，CA 签名私钥不进入 Pod 或应用 Secret。按信任边界采用独立服务端/客户端 CA 或签发策略。不要复用 SRE 开发证书和示例凭据。

### 生成一次性本地 CA 与身份

仅在新的私有实验目录运行。示例生成未加密私钥，通过文件权限保护，不构成生产 CA 流程。用于实际端点前，应修改每个证书的 SAN。

```bash
umask 077
mkdir -p private
openssl req -x509 -newkey rsa:3072 -noenc -sha256 -days 365 -keyout private/ca.key -out private/ca.crt -subj "/CN=RocketMQ Local TLS CA" -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign"

for component in namesrv broker proxy mcp control; do
  openssl req -new -newkey rsa:3072 -noenc -keyout "private/$component.key" -out "private/$component.csr" -subj "/CN=$component.mq.example.test"
  cat > "private/$component.ext" <<EOF
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=DNS:$component.mq.example.test
EOF
  openssl x509 -req -in "private/$component.csr" -CA private/ca.crt -CAkey private/ca.key -CAcreateserial -days 30 -sha256 -extfile "private/$component.ext" -out "private/$component.crt"
done

openssl req -new -newkey rsa:3072 -noenc -keyout private/client.key -out private/client.csr -subj "/CN=docs-messaging-client"
cat > private/client.ext <<'EOF'
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature
extendedKeyUsage=clientAuth
EOF
openssl x509 -req -in private/client.csr -CA private/ca.crt -CAkey private/ca.key -CAcreateserial -days 30 -sha256 -extfile private/client.ext -out private/client.crt
openssl verify -CAfile private/ca.crt -purpose sslserver private/proxy.crt
openssl verify -CAfile private/ca.crt -purpose sslclient private/client.crt
```

应用、管理员和基础设施应分别签发客户端身份，上面的单个客户端只用于实验。证书链和私钥放在私有文件中，不提交到仓库，也不提交生成的 Secret YAML 或包含这些材料的诊断输出。传输层支持通过密码字段读取加密 PKCS#8 私钥，Proxy gRPC 对应字段为 `privateKeyPassword`；MCP 专用 TLS 配置没有私钥密码字段。密码不应写入公开 ConfigMap。

## 创建和挂载 Kubernetes Secret {#secrets}

先选择正确的 kubectl context，以下命令会在选定集群中创建资源。命名空间不存在时才执行 `kubectl create namespace rocketmq`。Proxy TLS Secret 的两种创建方式**选一种**：

```bash
kubectl create namespace rocketmq

# 仅验证服务端；配合 clientAuth: none。
kubectl -n rocketmq create secret tls rocketmq-proxy-tls --cert=private/proxy.crt --key=private/proxy.key
```

使用 mTLS 时，以包含客户端 CA 的以下命令创建同名 Secret，替代上面的 `create secret tls`：

```bash
kubectl -n rocketmq create secret generic rocketmq-proxy-tls --type=kubernetes.io/tls --from-file=tls.crt=private/proxy.crt --from-file=tls.key=private/proxy.key --from-file=ca.crt=private/ca.crt
```

服务端 Secret 中的 `ca.crt` 用于信任**客户端**证书；客户端验证服务端时可以使用另一套 CA。`kubectl create secret tls` 提供 `tls.crt` 和 `tls.key`，不会生成证书、附加客户端 CA 或启用监听器。

仅为实际部署的组件创建其他 Secret：

```bash
kubectl -n rocketmq create secret generic rocketmq-namesrv-tls --type=kubernetes.io/tls --from-file=tls.crt=private/namesrv.crt --from-file=tls.key=private/namesrv.key --from-file=ca.crt=private/ca.crt
kubectl -n rocketmq create secret generic rocketmq-broker-tls --type=kubernetes.io/tls --from-file=tls.crt=private/broker.crt --from-file=tls.key=private/broker.key --from-file=ca.crt=private/ca.crt
kubectl -n rocketmq create secret generic rocketmq-client-tls --from-file=ca.crt=private/ca.crt --from-file=client.crt=private/client.crt --from-file=client.key=private/client.key
kubectl -n rocketmq create secret tls rocketmq-mcp-tls --cert=private/mcp.crt --key=private/mcp.key
kubectl -n rocketmq create secret tls rocketmq-control-tls --cert=private/control.crt --key=private/control.key
kubectl -n rocketmq create secret generic rocketmq-issuer-ca --from-file=ca.crt=private/issuer-ca.crt
```

先从真实 OAuth issuer 获取 `private/issuer-ca.crt`，前面的实验 CA 不会自动成为 issuer 的 CA。`rocketmq-mcp-tls` 和 `rocketmq-control-tls` 是自定义工作负载的示例名称，Core chart 不会自动挂载它们。

ACL 与内部 RPC 凭据使用独立 Secret。按[部署安全](security.md)准备真实且权限受限的 ACL、JSON 凭据文件后：

```bash
kubectl -n rocketmq create secret generic rocketmq-namesrv-auth --from-file=plain_acl.yml=private/namesrv-acl.yml
kubectl -n rocketmq create secret generic rocketmq-broker-auth --from-file=plain_acl.yml=private/broker-acl.yml --from-file=inner-client.json=private/broker-inner-client.json
kubectl -n rocketmq create secret generic rocketmq-proxy-auth --from-file=plain_acl.yml=private/proxy-acl.yml --from-file=inner-client.json=private/proxy-inner-client.json
kubectl -n rocketmq create secret generic rocketmq-controller-auth --from-file=plain_acl.yml=private/controller-acl.yml
```

不要把实际秘密放入 `--from-literal` 示例或命令历史，使用秘密分发流程提供的文件。Core chart 的内部 JSON 字段为 `accessKey`、`secretKey`，以及可选的 `securityToken`；接收方服务必须为该身份授予所需权限。

### 自定义工作负载挂载

以下是 Linux Broker 容器的 **Pod spec 片段**，Deployment/StatefulSet 应合入 `spec.template.spec`。NameServer、MCP 和 Control 使用相同方式挂载各自的 Secret。客户端 Pod 挂载自己的客户端 Secret，不挂载服务端私钥。

```yaml
securityContext:
  runAsNonRoot: true
  runAsUser: 10001
  runAsGroup: 10001
  fsGroup: 10001
containers:
  - name: broker
    volumeMounts:
      - name: remoting-tls
        mountPath: /etc/rocketmq/tls
        readOnly: true
volumes:
  - name: remoting-tls
    secret:
      secretName: rocketmq-broker-tls
      defaultMode: 0440
      items:
        - {key: tls.crt, path: tls.crt}
        - {key: tls.key, path: tls.key}
        - {key: ca.crt, path: ca.crt}
```

可重载材料使用目录挂载。Secret 卷更新会延迟投射；`subPath` 挂载不会收到更新；通过 Secret 注入的环境变量不会在现有进程内自动变化。确认服务实际 UID 可以读取私钥，Windows 卷 ACL 需要对应平台部署方案。限制 Secret RBAC 和 Pod 访问，启用集群静态加密；base64 编码不是加密。参阅 [Kubernetes Secret 文档](https://kubernetes.io/docs/concepts/configuration/secret/)。

## NameServer 与 Broker remoting {#remoting}

在仓库根目录编译所需实现：

```bash
cargo build -p rocketmq-namesrv --bin rocketmq-namesrv-rust --features tls
cargo build -p rocketmq-broker --bin rocketmq-broker-rust --features tls
```

### NameServer

把以下**根层点分赋值**合入 `namesrv.toml`，放在所有 TOML 表头之前。启动器也通过 Java 兼容 TLS properties 解析器读取这些行。`[tls.server]` 嵌套表或带引号的 `"tls.enable"` 键，对该加载器不是等价启动配置。

```toml
tls.enable = true
tls.test.mode.enable = false
tls.server.mode = "enforcing"
tls.server.certPath = "/etc/rocketmq/tls/tls.crt"
tls.server.keyPath = "/etc/rocketmq/tls/tls.key"
tls.server.need.client.auth = "require"
tls.server.authClient = true
tls.server.trustCertPath = "/etc/rocketmq/tls/ca.crt"
```

启动参数为 `-c /etc/rocketmq/namesrv.toml`。已有 `configStorePath` 中的持久化 desired 属性在启动文件后应用；CLI 地址/端口参数覆盖对应字段。实际监听行为与文件不一致时，应检查有效配置。

### Broker

合入规范 Broker TOML：

```toml
[broker.brokerServerConfig.tlsConfig]
enable = true
testModeEnable = false

[broker.brokerServerConfig.tlsConfig.server]
mode = "enforcing"
certPath = "/etc/rocketmq/tls/tls.crt"
keyPath = "/etc/rocketmq/tls/tls.key"
needClientAuth = "require"
authClient = true
trustCertPath = "/etc/rocketmq/tls/ca.crt"
```

启动参数为 `-c /etc/rocketmq/broker.toml --config-format toml`，两个 remoting 监听器继承该策略。使用 VIP 端口的客户端需要相同证书信任和端点 SAN。这些配置不会加密 HA 复制，也不会配置 Broker 另行创建的出站客户端。

两种服务均应选择 `mode = "enforcing"` 拒绝明文；`permissive` 同时接收明文与 TLS，仅适合明确安排的迁移期；`disabled` 不提供 TLS。仅验证服务端、不使用 mTLS 时，选择 `needClientAuth = "none"`（NameServer 对应 `tls.server.need.client.auth`），省略客户端 CA/auth 字段。`optional` 和 `require` 都需要 `authClient = true` 与有效客户端 CA。

传输层可选 properties 文件默认为 `/etc/rocketmq/tls.properties`。Broker 在 `tlsConfig` 内通过 `configFile` 指定，NameServer 使用 `tls.config.file`。它可以覆盖证书、信任、验证和协议字段，但不能覆盖基础 `enable` 或服务端模式。需要显式协议选择时，在该文件使用 `tls.protocols=TLSv1.3,TLSv1.2`；旧协议版本会被拒绝。应明确管理这份覆盖配置，尤其是客户端使用的配置。

[安全部署 profile](security.md)会校验额外身份与策略材料，其中 `ROCKETMQ_SECURITY_TLS_CERT`、`ROCKETMQ_SECURITY_TLS_KEY` 不替代上述监听配置。NameServer 的 secure-enforced 启动路径还要求强制 TLS 监听和应用认证/授权。

## Rust 消息客户端与 Admin {#clients}

确保最终依赖图包含 `rocketmq-transport/tls`，`rocketmq-client-rust` 没有名为 `tls` 的 feature；生产构建不能依赖 dev-dependencies 提供 TLS。启动前把公共配置应用到实际客户端：

```rust
use rocketmq_client_rust::base::client_config::ClientConfig;
use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;

let common = ClientConfig::builder()
    .namesrv_addr("namesrv.mq.example.test:9876")
    .enable_tls(true)
    .tls_test_mode_enable(false)
    .tls_client_auth_server(true)
    .tls_client_trust_cert_path("/etc/rocketmq/client-tls/ca.crt")
    .tls_client_cert_path("/etc/rocketmq/client-tls/client.crt")
    .tls_client_key_path("/etc/rocketmq/client-tls/client.key")
    .build()?;

let producer = DefaultMQProducer::builder()
    .producer_group("docs_tls_producer")
    .client_config(common)
    .build();
```

此片段配置 Producer；按[客户端配置](../configuration/client-config.md)注入正常的受管理运行时，执行 start/send/shutdown。Push 同样接受 `client_config`；LitePull 接受 `client_options(ClientOptions::from(common))`，且需要额外设置 `use_tls(true)`，因为构建时会应用自身的 TLS 值。仅使用服务端 TLS 时，同时省略客户端证书和私钥两个字段。安全部署始终关闭测试模式并启用服务端验证。

客户端必须同时信任 NameServer 和所有路由选出的 Broker 证书。CA bundle 应包含所需全部签发者，路由地址必须可达，Broker SAN 必须包含路由通告的域名/IP。TLS 客户端不会对未配置 TLS 的对端自动回退明文。

Admin Core 的只读/变更 builder 提供 `use_tls(true)`。只暴露此布尔选项的外观对象，可以在 Admin 进程中明确挂载 `/etc/rocketmq/tls.properties`，提供信任和可选客户端身份：

```properties
tls.test.mode.enable=false
tls.client.authServer=true
tls.client.trustCertPath=/etc/rocketmq/client-tls/ca.crt
tls.client.certPath=/etc/rocketmq/client-tls/client.crt
tls.client.keyPath=/etc/rocketmq/client-tls/client.key
```

这些属性不会打开外观对象的 TLS 布尔开关。当前 Admin CLI 没有全局 `--tls`、CA 或客户端证书选项，不应编造此类命令，也不能认为在此文件设置 `tls.enable` 就会启用 CLI 会话 TLS。访问安全端点时使用明确配置 TLS 的 Admin 集成。参阅 [Admin 使用](../operations/admin.md)。

## Proxy gRPC 与 Core Helm chart {#proxy}

```bash
cargo build -p rocketmq-proxy --bin rocketmq-proxy-rust --no-default-features --features cluster-mode,tls
```

构建 Local 模式时改用 `local-mode,tls`。把以下片段合入对应 [Proxy 配置](proxy.md)：

```toml
[grpc.tls]
enabled = true
certificatePath = "/etc/rocketmq/tls/tls.crt"
privateKeyPath = "/etc/rocketmq/tls/tls.key"
clientAuth = "require"
clientCaPath = "/etc/rocketmq/tls/ca.crt"
reloadIntervalMs = 5000
```

`clientAuth` 可选 `none`、`optional`、`require`，后两种必须提供 `clientCaPath`；`reloadIntervalMs` 必须为正。内置监听器协商 HTTP/2。客户端需要服务端 CA、匹配的端点名称，mTLS 时还需自己的身份。前面的 remoting Rust Producer 不是 Proxy v2 gRPC 客户端。

Kubernetes 部署先准备 Proxy TLS 与 auth Secret，选择实际编译 `tls` 的镜像，再把以下内容与真实镜像覆盖配置一起写入 `core-site-values.yaml`：

```yaml
services:
  proxy:
    tls:
      enabled: true
      secretName: rocketmq-proxy-tls
      clientAuth: require
      clientCaKey: ca.crt
```

先渲染，再安装：

```bash
helm template core distribution/helm/rocketmq-rust-core -n rocketmq -f distribution/helm/rocketmq-rust-core/values-production-proxy-tls.yaml -f core-site-values.yaml > core-rendered.yaml
helm upgrade --install core distribution/helm/rocketmq-rust-core -n rocketmq -f distribution/helm/rocketmq-rust-core/values-production-proxy-tls.yaml -f core-site-values.yaml
```

Chart 挂载 `tls.crt`、`tls.key`；启用 mTLS 时，把 `clientCaKey` 指定的 Secret 键挂载为 `/etc/rocketmq/tls/ca.crt`。它只为 Proxy 渲染 `grpc.tls`，没有对应 NameServer/Broker TLS Secret values 或通用额外卷开关；这些挂载和配置需使用维护好的自定义 manifest/overlay。不要添加不存在的 `services.broker.tls` values 并期待生效。渲染文件应保持私有，因为其他 values 可能包含敏感材料。参阅 [Kubernetes 部署](kubernetes.md)。

## MCP HTTPS 与出站信任 {#mcp}

### 查询 MCP

在 `rocketmq-ai/rocketmq-mcp` 目录编译 `streamable-http` feature，把以下内容合入现有配置：

```toml
[server.http.tls]
cert_path = "/etc/rocketmq/tls/tls.crt"
key_path = "/etc/rocketmq/tls/tls.key"

[server.http.auth]
mode = "oauth-jwt"
issuer = "https://identity.example.test"
audience = "rocketmq-mcp"
required_scopes = ["rocketmq:read"]
jwt_algorithm = "rs256"
jwks_url = "https://identity.example.test/.well-known/jwks.json"
jwks_ca_path = "/etc/rocketmq/issuer/ca.crt"
```

同时将 `server.http.public_base_url` 和 allowed origins 设置为实际 HTTPS 部署，保留权限、审计配置，并用 `--config /etc/rocketmq/mcp.toml --transport streamable-http` 启动。CLI 默认传输为 stdio，仅在文件中选择 HTTP 不够。TLS、JWKS CA 路径相对配置文件解析，绝对容器路径可以避免歧义。Issuer 使用受支持的公开信任路径时，省略 `jwks_ca_path`。

专用 TLS 配置指定服务端证书，不构成客户端证书授权。生产认证使用 OAuth，回环 development-token fixture 不是生产身份提供者。查询 MCP 的集群配置没有 `use_tls` 字段，默认 Admin 会话也未开启出站 TLS；HTTPS 入口安全不代表 RocketMQ 查询连接安全。参阅[查询 MCP](../ecosystem/mcp.md)。

### MCP Control

在 `rocketmq-ai/rocketmq-mcp-control` 目录使用独立构建，把以下片段合入真实配置：

```toml
[server.tls]
cert_path = "/etc/rocketmq/tls/tls.crt"
key_path = "/etc/rocketmq/tls/tls.key"

[oauth]
issuer = "https://identity.example.test"
audience = "rocketmq-mcp-control"
jwks_url = "https://identity.example.test/.well-known/jwks.json"
jwks_ca_path = "/etc/rocketmq/issuer/ca.crt"
```

在各个已有 `[[clusters]]` 条目中设置 `use_tls = true`，通过上面的 Admin properties 文件提供 remoting 信任与客户端身份。Control 使用 `ROCKETMQ_MCP_CONTROL_CONFIG`，没有 `--config` 参数，也不会把 TLS 路径重定位到 TOML 所在目录。默认 URL/JWKS 策略要求公开规范 HTTPS；私网部署需按文档分别决定 `server.allow_internal_base_url`、必要时的 `server.allow_wildcard_bind`、`oauth.allow_private_network`。这些选项不会关闭证书验证或 OAuth 授权。编译 feature、策略、审计、凭据和 claims 都就绪前，应保持变更关闭。参阅 [MCP Control](../ecosystem/mcp-control.md)。

两种 HTTPS 监听器都使用传输层 TLS runtime；挂载的 `/etc/rocketmq/tls.properties` 会影响有效 TLS 策略，应明确管理。OAuth 签名/JWKS 密钥与 HTTPS TLS 证书用途不同，分别轮换。

## AI SRE 连接与 Secret {#sre}

独立管理各 SRE 信任边界：

| 路径 | 配置 / 材料 | 使用注意事项 |
| --- | --- | --- |
| Connector → MCP | `ROCKETMQ_SRE_MCP_URL`、`ROCKETMQ_SRE_MCP_CA_PATH`；OAuth client credentials | 使用 MCP 端点的 CA、匹配的 HTTPS URL，以及对应 scope、租户/集群和只读能力协商。OAuth token endpoint 是另一条信任连接。 |
| Connector → Control Plane | `ROCKETMQ_SRE_CONTROL_PLANE_URL`、`ROCKETMQ_SRE_CONTROL_PLANE_CA_PATH`、`ROCKETMQ_SRE_CONTROL_PLANE_CLIENT_IDENTITY_PATH` | HTTP/2 HTTPS；客户端身份文件包含证书链**和私钥**，还需要独立 Bearer token。 |
| Connector / Agent → Kubernetes | Connector 的 `ROCKETMQ_SRE_KUBERNETES_CA_PATH`；Agent 的 Kubernetes 设置 | 挂载 API 的公开 CA 和该工作负载所需的最小 service-account token/RBAC，不使用消息 CA 代替 Kubernetes CA。 |
| Executor / Agent 入站 | 专用工作负载 mTLS 网关、独立 Bearer 身份 | 隔离 Axum 监听器；网关用已验证身份覆盖 `x-forwarded-client-cert`。Agent 要求 `spiffe://rocketmq-sre/executor`，Executor 要求 `spiffe://rocketmq-sre/control-plane`。 |
| Connector / Agent → RocketMQ Admin | `ROCKETMQ_SRE_ADMIN_USE_TLS` / `ROCKETMQ_SRE_AGENT_BROKER_ADMIN_USE_TLS` | 与 HTTPS 通道独立；使用 Admin 信任/身份 properties，并确认端点支持。开发 manifest 在此明确使用明文。 |
| Model Gateway → HTTP 模型提供者 / gRPC Provider SPI | `HttpTlsConfig` / `GrpcSpiClientTlsConfig::mutual_tls` | 注入根 PEM 与可选 HTTP 客户端身份；SPI 需要 CA、客户端证书/私钥、服务端域名及预期适配器身份。这些是库 API，不是自动全局环境设置。 |
| SRE UI/API、PostgreSQL、OTLP | 部署网关 / 数据库 / Collector 设置 | 现有 Compose/Kind fixture 包含开发 HTTP 路径，生产边界需要另外配置和验证。 |

Kind Connector 通道 manifest 使用以下精确 Secret 名称，并按命名空间分离。先通过工作负载 PKI 获取**独立的**服务端/客户端 CA 与身份文件，前面的消息客户端示例不会生成这些材料。需要时先创建命名空间：

```bash
umask 077
cat private/sre/connector-client-cert.pem private/sre/connector-client-key.pem > private/sre/connector-client-identity.pem
```

以上 Bash 命令把已签发客户端证书链与私钥合并为身份文件，再创建 Secret：

```bash
kubectl -n rocketmq-sre create secret generic rocketmq-sre-control-plane-channel-server --from-file=control-plane-server-cert.pem=private/sre/control-plane-server-cert.pem --from-file=control-plane-server-key.pem=private/sre/control-plane-server-key.pem --from-file=connector-client-ca-cert.pem=private/sre/connector-client-ca-cert.pem
kubectl -n rocketmq-system create secret generic rocketmq-sre-control-plane-channel-client --from-file=control-plane-server-ca-cert.pem=private/sre/control-plane-server-ca-cert.pem --from-file=connector-client-identity.pem=private/sre/connector-client-identity.pem
```

现有 Kind 栈中，Control Plane sidecar 在 `/etc/rocketmq/sre-channel` 读取服务端 Secret；Connector 在 `/var/run/secrets/rocketmq-sre-channel` 读取客户端 Secret。HTTPS 通道为 `https://sre-control-plane.rocketmq-sre.svc.cluster.local:8444`，证书需包含该 DNS SAN。专用 Connector Axum 后端监听 loopback 8093，NetworkPolicy 防止绕过代理。

现有 Nginx fixture 固定 `CN=rocketmq-sre-connector` 和 `CN=RocketMQ SRE Connector Development CA`。生产环境需要同时对齐签发 subject/issuer、Nginx allowlist、Connector 配置和 Control Plane 绑定，不复用开发 CA。Connector 只挂载 MCP 公开 CA，不挂载 MCP 服务端私钥。现有 SRE 脚本还创建 OAuth/Bearer/ACL/bootstrap Secret，这两个通道 Secret 不替代它们。参阅 [AI SRE 部署](../ecosystem/ai-sre-deployment.md)与[通道契约](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-ai/rocketmq-sre/docs/control-plane-connector-mtls.md)。

## Dashboard、数据库与遥测 {#dashboards}

Web Dashboard 独立配置三条路径：

1. **浏览器 HTTPS：**由 HTTPS 反向代理/Ingress 提供前端，并把 `/api` 转发到私有后端。保持 `DASHBOARD_WEB_SESSION_COOKIE_SECURE=true` 和精确允许 origin；`DASHBOARD_WEB_USE_TLS` 不会为 Axum 后端启用 HTTPS。
2. **RocketMQ remoting：**设置 `DASHBOARD_WEB_USE_TLS=true`，配置进程的 Admin 信任/身份 properties，并提供独立 RocketMQ ACL 凭据。GPUI/Tauri 使用各自连接 TLS 设置和本机系统信任或传输层 properties 文件；TLS 状态标记本身不能证明握手成功。
3. **SQL：**选择存储后端，用 `DASHBOARD_WEB_DATABASE_URL_FILE` 读取挂载 URL。MySQL 使用 `ssl-mode=verify_identity` 与 `ssl-ca`，PostgreSQL 使用 `sslmode=verify-full` 与 `sslrootcert`。URL 主机名必须匹配数据库证书；File/SQLite 没有数据库 TLS 连接。

获取 HTTPS 证书、数据库 CA 和私有 URL 文件后，自定义 Dashboard 工作负载可这样创建 Secret：

```bash
kubectl -n rocketmq create secret tls rocketmq-dashboard-https --cert=private/dashboard.crt --key=private/dashboard.key
kubectl -n rocketmq create secret generic rocketmq-dashboard-db --from-file=database-url=private/dashboard-database-url --from-file=ca.crt=private/database-ca.crt
```

实际 Ingress/网关配置应引用 `rocketmq-dashboard-https`。数据库 Secret 挂载到后端，例如 `/run/secrets/dashboard-db`，设置 `DASHBOARD_WEB_DATABASE_URL_FILE=/run/secrets/dashboard-db/database-url`；私有 URL 通过驱动的 CA 参数引用 `/run/secrets/dashboard-db/ca.crt`。不能同时设置 URL 与 URL_FILE，也不能把秘密放入前端 `VITE_` 变量。参阅 [Web Dashboard 部署](../ecosystem/dashboard-web.md)与 [SQL TLS 示例](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-dashboard/rocketmq-dashboard-web/docs/storage-deployment.md)。

Prometheus、健康检查、运行时诊断和 OTLP 都有独立监听器/导出器，暴露的 HTTP 端点应由已配置网关或私有工作负载边界保护。共享可观测配置提供 endpoint/protocol/headers，没有通用自定义 CA/客户端私钥 TLS 块；不要编造 `observability.otlp.tls` 字段，也不能假定各构建均支持所有 `https://` 导出器。可通过已验证 TLS 边界后的本地 Collector 转发到远端，为 Collector 独立配置并验证 CA/身份。参阅[可观测配置](../configuration/observability.md)。

## 验证、轮换与排障 {#verification}

先验证证书握手，再验证实际协议与权限。Proxy gRPC mTLS 示例：

```bash
openssl s_client -connect proxy.mq.example.test:8081 -servername proxy.mq.example.test -CAfile private/ca.crt -cert private/client.crt -key private/client.key -verify_hostname proxy.mq.example.test -verify_return_error -alpn h2 </dev/null
```

检查退出状态、证书验证结果和协商出的 `h2`。remoting 握手改为对应 NameServer/Broker 端点，不要求 gRPC ALPN；IP 端点使用 `-verify_ip`。TLS 握手成功不能证明 Broker 注册、路由发现、gRPC API、消息投递或授权成功。执行真实的允许操作，并按策略验证明文、不受信任 CA、错误 SAN、缺少/不受信任客户端证书会被拒绝。OAuth 端点还要验证缺失或错误 scope 的 token 被拒绝。

| 组件 | 轮换行为 |
| --- | --- |
| 使用传输层的 NameServer/Broker 与 MCP HTTPS | 轮询服务端证书/私钥/CA 及配置的 TLS properties，失败时保留旧接收器。基础 enable/mode 变更需重配或重启进程。新连接使用新接收器，现有连接不重新握手。 |
| Proxy gRPC | 按 `reloadIntervalMs` 轮询文件元数据；拒绝候选时保留旧代次，不会持续重试未变化的错误候选，应修正/更新文件。 |
| Remoting 客户端 | 新连接根据当前材料构建 TLS；现有连接保留既有握手。按正常所有权流程重连或重启，没有服务端式客户端轮询任务。 |
| SRE 代理、提供者客户端、SQL 连接池、HTTPS 网关 | 使用各组件的重载/重启流程；已有 HTTP 客户端、数据库池连接或环境注入凭据可能需要重新创建。 |

组件支持信任重叠时，先把新 CA 加入接收方信任 bundle，再分发新身份，确认卷投射与成功重载/重启，建立新连接并验证允许与拒绝操作，最后移除旧 CA/身份。保持证书、私钥、CA 更新一致，监控被拒绝的重载候选。证明新身份可用前，不撤销唯一有效身份。

| 症状 | 检查项 |
| --- | --- |
| 配置 TLS 后启动失败 | 正确 feature、PEM/私钥匹配、权限、服务端模式、客户端 CA、properties 覆盖。 |
| 未知签发者 / 证书名称错误 | 实际 CA bundle、中间证书链、路由通告地址、DNS/IP SAN。 |
| TLS Proxy 正常但 Broker 查询失败 | 协议与出站连接独立；先核对组件表，不直接推断加密或兼容性。 |
| Secret 配置后 Pod 仍 Pending / ContainerCreating | 命名空间、Secret 名称、必需键名与 volume items，无需打印私钥材料。 |
| Secret 更新后仍使用旧证书 | 投射延迟、`subPath`、错误候选、现有连接或需重启的组件。 |
| SRE 通道注册失败 | 独立服务端/客户端 CA、合并身份、DN allowlist、Bearer token、DNS SAN、HTTP/2、后端隔离。 |
| Dashboard 页面正常但登录/SQL 失败 | HTTPS Secure Cookie/origin 与独立数据库信任/URL 设置。 |

本文记录源码接线与部署示例，不声明已完成真实集群全链路 TLS 或证书轮换演练。实现索引：[传输层 TLS](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-transport/src/tls.rs)、[传输配置](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-transport/src/config/tls_config.rs)、[Proxy TLS 重载](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-proxy/src/grpc/tls_acceptor.rs)、[Core chart](https://github.com/mxsm/rocketmq-rust/tree/main/distribution/helm/rocketmq-rust-core)、[MCP 配置](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-ai/rocketmq-mcp/conf/mcp.example.toml)、[Control 配置](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-ai/rocketmq-mcp-control/conf/mcp-control.example.toml)、[OpenSSL 验证选项](https://docs.openssl.org/3.5/man1/openssl-s_client/)。
