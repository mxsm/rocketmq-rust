# RocketMQ-Rust AI SRE UI

独立的桌面端 AI SRE 工作台。生产构建默认使用 OIDC；开发会话和 mock
数据只在 Vite development 模式或显式开发配置下可用。UI 不调用 RocketMQ
Dashboard mutation API。

## 工作区与调查

主导航分为值班台、调查、变更、可靠性、知识、设置六区。当前区的二级导航
保留原有页面入口；既有深链接继续可用。`/investigations` 是调查列表，
`/investigations/:investigationId` 展示服务端保存的诊断修订、证据引用、
缺失证据、时间线及关联会话、事件和计划入口。首页优先显示待判断、待补证
队列与交接入口；审批仍通过变更中心的既有计划流程进入。

顶栏共享授权集群、资源和 UTC 绝对时间窗。URL 使用原有 `cluster_id`、
`resource_kind`、`resource_key`，并增加 `window_start`、`window_end`（带时区
的 ISO 8601 时间，开始必须早于结束）。内部导航、浏览器前进后退和刷新保留
完整范围。无范围的旧对象链接按服务端返回的授权对象对齐；显式范围与对象
冲突时，页面要求对齐后继续，不会把旧对象显示为新范围的数据。

默认时间窗在工作区打开时固定为此前 24 小时。调查列表、值班队列按更新时间
筛选，工作台按修订创建时间和时间线发生时间筛选。其他页面保留各自注明的
采样窗口；会话查询仍使用已有的相对 `window_seconds`，绝对窗不改变采集合同。
新查询可能产生窗外修订，工作台显示数量，并提供显式扩展时间窗操作。
证据保留原始观测时间，窗外证据有提示。规则支持评分不是校准概率。

当前调查工作台支持人工只读调查，不提供持久 run、暂停或预算控制。补证和
追问从关联只读会话发起；升级事件调用既有工作流 API，执行仍只从事件的
受监督计划、审批与围栏路径进入。未新增目标写入能力。

范围或身份、租户、授权变化会释放旧页、请求与订阅，旧结果不能覆盖新范围；
同身份刷新和 token 续期保留当前表单。工作流订阅由同身份消费者共享，按
租户、集群和对象过滤，断流使用串行轮询。浅色/深色主题、语言和显示时区
保存在操作员偏好中；新增导航和调查界面提供中英文文案。

开发演示可访问 `/?demo=1`。历史演示记录位于 2026-07-27，需选择对应绝对
时间窗；新会话查询生成当前时间的演示修订与可读取证据。演示租户与 DEV
身份保持一致，生产租户校验不变。

## API 类型生成

当前生成器的 OpenAPI 输入为
`../openapi/rocketmq-sre-phase05.openapi.json`。Control Plane 的
`/v1/openapi.json` 与 UI 生成器读取这一份来源；修改 API 合同后运行：

```bash
npm run generate:api
npm run check:api
```

生成产物为 `src/api/generated.d.ts`。`src/api/types.ts` 通过生成的
`components["schemas"]` 桥接 Evidence、Message Journey、SSE、巡检报告和
工作流请求类型。诊断 API 保持只读；审批与执行页面仅使用既有版本化的
Control Plane 受监督接口，不提供任意目标请求、配置补丁或 shell。

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

本次工作区改动的聚焦回归可运行：

```bash
npm run test -- --run src/context src/hooks src/data/SreDataContext.test.tsx src/data/SreDataContext.demo.test.tsx src/data/mockApi.test.ts src/features/investigations src/features/scope src/pages/InvestigationWorkspacePage.test.tsx src/pages/SupervisedExecutionPages.test.tsx src/pages/WorkflowPages.test.tsx src/router.test.tsx
```

覆盖范围投影、裸旧链接、浏览器历史、对象冲突、身份撤销、token 续期、
迟到响应、同范围刷新、共享订阅与串行轮询，以及证据定位、脱敏和取消。

桌面端验收视口为 `1280×720`、`1440×900` 和 `1920×1080`。当前阶段不做
移动端专项适配，只保证窄屏不会破坏基础导航和内容访问。
