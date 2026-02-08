# RocketMQ-Rust 文档

[English](README.md) | 中文

---

## 📚 概述

本目录包含 **RocketMQ-Rust** 项目的完整文档，包括：

- **设计文档**：架构设计、技术规范和实现细节
- **改进提案**：功能增强提案、优化策略和特性请求
- **开发者指南**：贡献指南、开发工作流程和最佳实践
- **用户文档**：入门指南、教程和使用示例

## 📁 目录结构

```
rocketmq-doc/
├── en/              # 英文文档
│   ├── 01-*.md     # 入门与基础
│   ├── 02-*.md     # 核心概念与组件
│   ├── 03-*.md     # 高级主题
│   ├── 07-*.md     # 设计文档
│   └── 19-*.md     # 贡献指南
├── zh-cn/           # 中文文档
│   └── (同结构)    # 与 en/ 相同结构
├── README.md        # 英文说明
└── README-zh_cn.md  # 本文件
```

## 📖 文档分类

#### 入门指南 (01-*)
- RocketMQ-Rust 是什么
- 快速开始指南
- 本地运行 RocketMQ-Rust
- 使用 Docker 运行 RocketMQ-Rust
- 在 Kubernetes 上运行 RocketMQ-Rust

#### 核心概念 (02-*)
- 架构概览
- 组件与模块
- 事务消息

#### 高级主题 (03-*)
- 项目路线图
- 性能优化
- 部署策略

#### 设计文档 (07-*)
- Broker 命名规范
- 协议设计
- 存储设计
- 网络架构

#### 贡献指南 (19-*)
- 贡献指南
- 代码审查流程
- 文档规范

## 🤝 贡献文档

我们欢迎对文档的改进贡献！请：

1. 遵循命名规范：`[分类]-[主题].md`（例如：`01-quick-start.md`）
2. 将英文文档放在 `en/` 目录，中文文档放在 `zh-cn/` 目录
3. 使用清晰的标题、代码示例和图表
4. 保持文档与代码变更同步更新
5. 提交 PR 附带您的文档变更

## 📝 编写指南

- 使用 **Markdown** 格式
- 适当位置添加代码示例
- 为复杂概念添加图表（使用 Mermaid 或图片）
- 保持语言清晰简洁
- 添加相关文档的链接

## 📚 快速链接

- [RocketMQ-Rust 是什么？](zh-cn/01-what-is-rocketmq-rust.md)
- [架构概览](zh-cn/01-architecture.md)
- [组件说明](zh-cn/02-components.md)
- [Broker 命名规范](zh-cn/07-01-broker-naming.md)
- [事务消息](zh-cn/01-transaction-producer.md)

---

## 📄 许可证

本文档是 RocketMQ-Rust 项目的一部分，采用双许可证：
- Apache License 2.0
- MIT License
