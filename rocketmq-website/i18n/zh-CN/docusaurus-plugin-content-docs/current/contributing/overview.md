---
title: "参与 RocketMQ-Rust"
---

# 参与 RocketMQ-Rust

从具体用户问题及其归属组件开始。有价值的贡献可以是可复现报告、修正后的双语示例、聚焦回归测试或行为变更。[开发指南](./development-guide.md)说明环境搭建和本地反馈流程，本文说明如何选择并呈现工作。

## 选择入口

| 目标 | 首先阅读 | 有用的交付内容 |
| --- | --- | --- |
| 报告故障 | [故障排查](../operations/troubleshooting.md)和对应组件指南 | 精确操作、预期/实际结果、版本/feature 和最小复现 |
| 改善教程或翻译 | [文档指南](./documentation.md)及两种语言文件 | 正确命令、前置条件、预期输出和完整对应译文 |
| 修复核心行为 | [架构](../architecture/overview.md)和[模块地图](../architecture/module-map.md) | 归属层内的聚焦修改及回归覆盖 |
| 改善客户端集成 | [客户端配置](../configuration/client-config.md)和 [API 迁移](../migration/rust-api.md) | 包含生命周期和完成语义的公共 API 示例 |
| 开发应用产品 | [生态](../ecosystem/overview.md)和该应用的本地指南 | 在独立工程内改进产品行为、界面或服务集成 |
| 改善性能 | [容量与性能](../operations/capacity-performance.md) | 明确工作负载、观察到的瓶颈和可比较测量 |

可执行变更使用 [GitHub Issues](https://github.com/mxsm/rocketmq-rust/issues)，使用或设计问题使用 [Discussions](https://github.com/mxsm/rocketmq-rust/discussions)。实现前检查相关工作，避免重复修改。较宽泛的 issue 应先说明准备改善的具体行为，不把小修复扩展为无关重构。

## 准备正确的工程

克隆自己的 fork 或使用已授权检出，然后遵循[开发环境搭建](./development-guide.md)。编辑前查看 `git status --short`，保护已有修改。通过最近的 `AGENTS.md` 和当前 manifest 选择构建根目录。

根 Cargo 工作区、独立示例、Dashboard 应用、MCP、SRE 和网站的命令不同。目录名不一定是 Cargo 包名，例如 `rocketmq-client` 应以 `rocketmq-client-rust` 选择。[发行范围](../overview/release-scope.md)解释工作区成员和产品发行成员的区别。

除非变更有意处理对应契约，否则保留页面 ID、公共 API 和序列化字段。依赖和配置修改应限于问题所需，不格式化无关文件，也不替换他人未提交工作。

## 提供可复现的问题报告

使用对应类型的仓库 issue 表单，包含最少但有效的信息：

1. 组件、源码或制品版本、相关 feature/后端/模式及平台。
2. 前置条件和精确操作，可能时提供最小示例。
3. 预期行为与实际观察，包括稳定错误码和请求标识。
4. 影响及已测试的临时处理方法。
5. 若提出修复，说明归属文件和观察变更效果的方法。

从公开报告中移除凭据、Bearer 令牌、ACL/TLS 材料、消息体及无关个人或生产细节。脱敏输入仍应保留形状和相关类型，使示例可用。不要仅凭通用客户端超时推断故障根因。

Issue 模板区分缺陷、功能、增强、重构、测试和文档。当前字段和标题前缀位于 [.github/ISSUE_TEMPLATE](https://github.com/mxsm/rocketmq-rust/tree/main/.github/ISSUE_TEMPLATE)，使用真实表单，不额外发明必填字段。

## 实现并检查受影响行为

保持变更足够聚焦，使审阅者能关联原因、实现与结果。遵循[编码规范](./coding-standards.md)，复用现有抽象，将共享行为放在归属层。

Rust 行为变更选择对应包/目标和聚焦回归测试。编译受影响代码的测试构建也可提供编译依据。共享 API 或 feature 变化时，增加直接受影响消费者的检查。并非每次贡献都自动要求根目录全目标或全部 feature 检查。

网站内容应同步修改英文和中文，保留可执行示例及技术限制，并在 `rocketmq-website` 运行 `npm run build`。文档工作不需要源码指纹、清洁工作区仪式、评分或新增审批流程，正常校对和现有预览/构建工具即可。

记录实际运行内容及其说明的问题。外部集群、GUI 平台或可选工具不可用时，说明未测试场景，不宣称通过，也不修改无关依赖来绕过问题。

## 提交便于审阅的 PR

关联 issue，并使用当前 [PR 模板](https://github.com/mxsm/rocketmq-rust/blob/main/.github/PULL_REQUEST_TEMPLATE.md)。仓库约定使用关联 issue 的英文标题，例如：

```text
[ISSUE #1234]📝Clarify consumer offset completion
```

将示例编号替换为实际 issue。先描述具体问题和最终行为，再总结相关验证及限制。语义变更解释兼容性或迁移影响；简单文档修正只需简短描述和相关构建结果。

根据评审反馈改进同一范围的结果，最终实现范围变化时同步更新描述。不要提交生成构建、日志、覆盖率、临时截图和本地测试数据。仅在截图属于有意交付的文档资源且准确反映产品时保留。

提交 PR 不意味着发布或部署。维护者按照对应项目流程处理集成和发行决策。社区分发身份见[发行范围](../overview/release-scope.md)。

来源：[根工程约定](https://github.com/mxsm/rocketmq-rust/blob/main/AGENTS.md)、[网站指南](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-website/AGENTS.md)、[Issue 模板](https://github.com/mxsm/rocketmq-rust/tree/main/.github/ISSUE_TEMPLATE)、[PR 约定](https://github.com/mxsm/rocketmq-rust/blob/main/.agents/skills/rocketmq-rust-pr-submitter/SKILL.md)。

## 测试与发行工作

- [测试策略与工程入口](./testing.md)
- [发行工程](./release-engineering.md)
