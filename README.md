# rocketmq-rust

Unofficial Rust implementation of RocketMQ.

## Modules

The existing RocketMQ has the following functional modules:

- **Name Server**
- **Broker**
- **Store (Local Storage)**
- **Controller (High Availability)**
- **Client (SDK)**
- **Proxy**
- **Tiered Store (Tiered Storage Module)**

The specific functions of each module can be referred to in the [official RocketMQ documentation](https://github.com/apache/rocketmq/tree/develop/docs). The Rust implementation will be carried out sequentially in the following order.

## Name Server

### Broker Management

- [ ] **[WIP] Broker registration**
- [ ] **Heartbeat message processing**

TODO

Other module implementations will be done subsequently, starting with the Rust implementation of the Name Server. The goal is to achieve functionality similar to the Java version.