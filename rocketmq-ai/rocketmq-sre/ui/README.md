# RocketMQ-Rust AI SRE UI

独立的桌面端 AI SRE 工作台。生产构建默认使用 OIDC；开发会话和 mock
数据只在 Vite development 模式或显式开发配置下可用。UI 不调用 RocketMQ
Dashboard mutation API。

## API 类型生成

Phase 01 的只读 OpenAPI 输入固定在
`../openapi/rocketmq-sre-phase01.openapi.json`。Control Plane 的
`/v1/openapi.json` 与 UI 生成器读取这一份来源；修改 API 合同后运行：

```bash
npm run generate:api
npm run check:api
```

生成产物为 `src/api/generated.d.ts`。`src/api/types.ts` 通过生成的
`components["schemas"]` 桥接 Evidence、Message Journey、SSE、巡检报告和
工作流请求类型。OpenAPI 文档固定
`x-rocketmq-cluster-mutation-supported=false`，不声明 Apply、Delete、Reset
或其他 RocketMQ mutation 路由。

## 认证配置

生产构建默认使用 OIDC，并在缺少 `VITE_SRE_OIDC_AUTHORITY` 或
`VITE_SRE_OIDC_CLIENT_ID` 时 fail closed。通用 UI 镜像将这些公开的
OIDC 客户端参数作为 Docker build argument 接收，因为 Vite 会在构建时
写入浏览器 bundle。

Compose 与 Kind 开发 profile 会显式构建
`VITE_SRE_AUTH_MODE=development` 的 UI，并注入固定的 Phase 00
开发租户、集群和一次性 bearer fixture。该值不是生产密钥，且只有
Control Plane 以 `ROCKETMQ_SRE_DEV_AUTH=true` 启动时才有效，禁止用于
生产镜像。Phase 00 的 OAuth fixture 只支持 Connector 到 MCP 的
client credentials，不是浏览器 OIDC/PKCE Provider。

## 验证

```bash
npm ci
npm run check:api
npm run lint
npm run test -- --run
npm run test:e2e:security
npm run build
```

`test:e2e:security` 使用真实 Chromium 和开发态 mock API 验证 Conversation
的 provisional、`preview_reset`、安全终态、Evidence 引用和只读执行资格。
完整脱敏报告由上层 `scripts/conversation-security-qualification.ps1` 生成，
不写入 UI 项目目录。

桌面端验收视口为 `1280×720`、`1440×900` 和 `1920×1080`。当前阶段不做
移动端专项适配，只保证窄屏不会破坏基础导航和内容访问。
