# rocketmq-rust

Rocketmq非官方的Rust实现

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

- [ ] **Broker注册(请求码：103)**
- [ ] **获取集群信息(请求码：106)**
- [ ] **HeartBeat消息处理**

TODO

后续会进行其他的模块实现，首先会对NameServer进行Rust实现，目标是能够达到和Java版本一样的功能。