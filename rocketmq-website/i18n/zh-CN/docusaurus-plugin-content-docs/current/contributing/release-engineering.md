---
title: "发行工程"
---

仓库定义了社区分发、核心包范围及部署资产。本页解释这些部分如何协作，以及文档应如何描述它们；不宣布已发布版本，也不执行发布。产品身份和排除范围参见[发行范围](../overview/release-scope.md)。

## 遵循声明的范围

根工作区包含 28 个成员。`scripts/core-release-scope.json` 分类了 27 个核心包：24 个注册表发布包和三个仅二进制包。Dashboard common 成员不属于这份核心包清单。注册表分类与服务可执行文件数量是不同概念。Dashboard、MCP 和 SRE 是独立产品，应使用各自的源码和部署文档。

发行身份为 `RocketMQ Rust Community Distribution`，身份类别是 `unofficial-community`，且 `official_apache_release: false`。发行说明和下载内容应一致使用该身份。采用 Apache 2.0 许可证，不代表产物是 Apache 项目的官方发行版。

## 使用 Cargo 打包注册表 crate

仓库不再单独维护候选准备、压缩包或 crate 暂存工具；注册表包直接使用 Cargo 准备。如需检查注册表发布包能否打包，在仓库根目录运行：

```bash
cargo package --workspace --locked --no-verify --exclude rocketmq-admin-cli --exclude rocketmq-admin-tui --exclude rocketmq-store-inspect --exclude rocketmq-dashboard-common
```

被排除的是三个仅二进制包和 Dashboard common 成员。打包结果检查的是各个包的边界，不是成功执行 `cargo publish`。仅供测试使用的同级 dev-dependency 只声明 path，因此 Cargo 会从发布的 manifest 中去掉它们，发布顺序只取决于普通依赖和构建依赖。

## 同步维护发行与网站文档

1. **选择文档对象。** 确认是源码搭建、注册表包还是独立产品，明确版本和 feature 假设。
2. **描述实际变化。** 覆盖行为、配置默认值、公共 API、协议/存储兼容性及迁移影响。对应边界复用 [Java 迁移](../migration/java-to-rust.md)、[Rust API 迁移](../migration/rust-api.md)和[升级回滚](../operations/upgrade-rollback.md)。
3. **区分能力与观察。** 已注册命令、声明目标或矩阵场景属于实现事实。运行或恢复成功需要实际观察及其范围。
4. **更新两种语言。** 保持页面 ID、命令、配置键、单位和图示语义一致。1.0.0 开发版 表示当前开发文档，不应静默改称已发行版本。
5. **产物存在后更新安装链接。** 指向实际产物与说明，再检查相关网站路由和示例。避免推测下载 URL 或笼统声明生产就绪。

文档工作不需要指纹流程、固定工作副本、新审批阶段或额外 CI 门禁。

## 源码参考

- [核心包范围](https://github.com/mxsm/rocketmq-rust/blob/main/scripts/core-release-scope.json)和[发行身份](https://github.com/mxsm/rocketmq-rust/blob/main/distribution/release-identity.json)。
