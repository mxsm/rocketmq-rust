# rocketmq-rust

[Apache Rocketmq](https://github.com/apache/rocketmq)非官方的Rust实现

|                   **Stargazers Over Time**                   |                  **Contributors Over Time**                  |
| :----------------------------------------------------------: | :----------------------------------------------------------: |
| [![Stargazers over time](https://api.star-history.com/svg?repos=mxsm/rocketmq-rust&type=Date)](https://api.star-history.com/svg?repos=mxsm/rocketmq-rust&type=Date) | [![GitHub Contributor Over Time](https://contributor-overtime-api.git-contributor.com/contributors-svg?chart=contributorOverTime&repo=mxsm/rocketmq-rust)](https://git-contributor.com?chart=contributorOverTime&repo=mxsm/rocketmq-rust) |

## 模块

现有的Rocketmq主要有如下几个功能模块：

- **Name Server**
- **Broker**
- **Store(存储-本地)**
- **Controller(高可用)**
- **Client(SDK)**
- **Proxy(代理)**
- **Tiered store(分级存储模块)**

每个模块的具体作用可以参照[Rocketmq的官方说明文档](https://github.com/apache/rocketmq/tree/develop/docs) 。Rust的实现会根据下面的顺序逐一来进行实现。

## Name Server

### Broker管理

- [x] **Broker注册(请求码：103)-暂时只支持基本的Broker注册，对于Controller模式待支持**
- [x] **获取集群信息(请求码：106)**
- [ ] **HeartBeat消息处理**

TODO

后续会进行其他的模块实现，首先会对NameServer进行Rust实现，目标是能够达到和Java版本一样的功能。

## 贡献

![Alt](https://repobeats.axiom.co/api/embed/6ca125de92b36e1f78c6681d0a1296b8958adea1.svg "Repobeats analytics image")

<a href="https://github.com/mxsm/rocketmq-rust/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=mxsm/rocketmq-rust&anon=1" />
</a>