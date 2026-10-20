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
`VITE_SRE_OIDC_CLIENT_ID` 时 fail closed。通用 UI 镜像在容器启动时将
公开的 OIDC 环境变量写入 `/runtime-config.js`，页面先加载配置再初始化认证。
同一个镜像可以在不同部署环境中使用，发布 Action 无需配置 OIDC GitHub Variables。

容器运行时支持以下公开配置：

| 环境变量 | 用途 |
| --- | --- |
| `VITE_SRE_OIDC_AUTHORITY` | 必填，认证服务的 Issuer URL |
| `VITE_SRE_OIDC_CLIENT_ID` | 必填，已注册的浏览器应用 Client ID |
| `VITE_SRE_OIDC_REDIRECT_URI` | 可选，默认 `<当前页面 origin>/auth/callback` |
| `VITE_SRE_OIDC_POST_LOGOUT_REDIRECT_URI` | 可选，默认当前页面 origin |
| `VITE_SRE_OIDC_SCOPE` | 可选，默认 `openid profile rocketmq:read rocketmq:diagnose rocketmq:model-governance` |

运行时配置优先于原有的 OIDC build arguments；构建参数仍兼容现有的自定义镜像。
重新创建容器即可应用新的环境变量，配置响应禁止缓存。
运行时配置仅包含上述字段，不接受认证模式、开发身份或 token。

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
