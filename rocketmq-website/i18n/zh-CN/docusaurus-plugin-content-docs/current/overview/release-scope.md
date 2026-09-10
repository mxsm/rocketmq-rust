---
title: "发行范围与分发身份"
---

# 发行范围与分发身份

RocketMQ-Rust 包含的产品多于一次核心发行。使用根 Cargo manifest 确认工作区成员、核心范围清单确认发行分类、所选制品的发布信息确认实际发布内容。这些来源各有用途，不能相互替代。

## 源码工作区、核心发行与独立产品

| 集合 | 当前源码定义 | 属于该集合意味着什么 |
| --- | --- | --- |
| 根 Cargo 工作区 | `Cargo.toml`，28 个成员 | 可从仓库根目录通过 Cargo 选择这些包。 |
| 核心发行包集合 | `scripts/core-release-scope.json`，27 个包 | 已纳入核心发行流程分类，不代表已在 registry 发布。 |
| 核心服务 | NameServer、Broker、Controller、Proxy | 核心范围命名的服务产品，仍有不同构建 feature、配置和启动命令。 |
| Dashboard common | 根工作区成员，明确排除在核心发行外 | 可随根工作区构建共享 Dashboard 模型/服务，但桌面和 Web 产品不因此属于核心发行。 |
| 独立 Cargo 产品 | Examples、GPUI、Tauri 后端、Web 后端、MCP、MCP Control、SRE 及专用测试 fixture/fuzzing | 使用各自 manifest 和指南，根构建不覆盖它们。 |
| Node 项目 | 网站、Dashboard 前端、SRE UI 和 TypeScript SDK | 各自拥有 package manifest 和命令。 |

范围文件列出了 Dashboard、MCP、SRE 的仓库排除项。MCP Control 同样是独立项目，未列入核心包清单；没有列入不意味着继承核心发行状态。[模块地图](../architecture/module-map.md)和[生态概览](../ecosystem/overview.md)说明这些归属。

## 按定义理解包分类

| 分类 | 在发行模型中的含义 | 当前示例 |
| --- | --- | --- |
| `registry-publish` | 纳入 registry 包规划 | 客户端、模型、协议、传输、运行时、安全、存储、服务库和 Admin Core |
| `binary-only` | 在该分类中作为二进制产品分发，而非 registry 库 | `rocketmq-admin-cli`、`rocketmq-admin-tui`、`rocketmq-store-inspect` |
| `internal-only` | 模式允许的内部包分类 | 当前核心条目未使用 |
| `non-publish` | 模式允许的不发布包分类 | 当前核心条目未使用 |

当前核心清单包含 24 个 `registry-publish` 和三个 `binary-only` 条目。服务包可以同时包含库和二进制，分类不会移除可执行文件。反过来，Cargo.toml 中的包版本不能证明下载归档或 registry 版本已经存在。

## 分发身份

仓库中的 `distribution/release-identity.json` 声明：

| 字段 | 声明值 |
| --- | --- |
| 分发名称 | RocketMQ Rust Community Distribution |
| 身份类型 | `unofficial-community` |
| 是否为 Apache 官方发行 | `false` |
| 源码项目 | `mxsm/rocketmq-rust` |
| Registry 所有者 / 包前缀 | crates.io 上的 `mxsm` / `rocketmq-` |
| OCI 命名空间 | `ghcr.io/mxsm/rocketmq-rust` |
| Helm chart 名称 | `rocketmq-rust` |
| 许可证标识 | `Apache-2.0` |

这些是声明的发行目标和身份元数据，不代表所有镜像、chart 或包当前均可获取。“项目官方文档”指为本项目维护的文档，不能解释为 Apache Software Foundation 官方发行身份。身份文件明确将其定义为非官方社区分发。

制品使用方式见[容器](../deployment/containers.md)和 [Kubernetes](../deployment/kubernetes.md)。保持镜像 tag、chart values、服务二进制与相应文档配套，不用未经核实的“latest”制品替代指定发行版。

## 版本号与 1.0.0 开发版 文档

根工作区当前声明版本 `1.0.0`，独立应用可以声明自身版本，例如 `0.1.0`。这些是源码 manifest 数值，不能据此推断发布日期、支持周期或跨产品同步发布。

Docusaurus 当前文档标记为 **1.0.0 开发版**，提供英文和 `zh-CN` 内容。1.0.0 开发版 描述当前源码系列，可能包含晚于已发布制品的行为。中文页是相同页面 ID 的完整对应正文，不是另一产品版本。

定位事故或兼容性问题时，记录实际二进制/包版本、构建 feature、部署模式及相关配置，再核对匹配的源码/API 文档。[能力矩阵](./capability-matrix.md)和[兼容性参考](../reference/protocol-compatibility.md)解释了为何仅有版本字符串不足以判断行为。

## 选择正确的构建与交付单元

1. 找到归属包/应用及其 manifest。
2. 明确交付物是库、服务二进制、桌面安装包、前端资源、chart 还是容器。
3. 使用该产品的构建入口及支持的 feature/平台组合。
4. 将产物与匹配配置、操作流程和已知限制关联。

例如，构建 Tauri React 前端不会生成桌面安装包，构建根工作区不会构建 Web Dashboard 后端。发布核心消息包不会发布 MCP，也不会为其配置 HTTP 认证。

本文是源码范围参考，不是新的发布公告，也不宣称发行工作流已经运行。

来源：[工作区 manifest](https://github.com/mxsm/rocketmq-rust/blob/main/Cargo.toml)、[核心发行范围](https://github.com/mxsm/rocketmq-rust/blob/main/scripts/core-release-scope.json)、[分发身份](https://github.com/mxsm/rocketmq-rust/blob/main/distribution/release-identity.json)、[网站版本配置](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-website/docusaurus.config.ts)。
