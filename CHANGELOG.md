# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **feat(common):** Add `filter_type` module to filter.rs in rocketmq-common crate ([#5454](https://github.com/mxsm/rocketmq-rust/issues/5454))

### Changed

- **refactor(client):** Refactor `default_mq_producer::start` in `default_mq_producer.rs` removing repeated `as_mut().unwrap()`([#5576](https://github.com/mxsm/rocketmq-rust/issues/5576))
- **refactor(tui):** Reformat `rocketmq-tui` using `taplo`([#5242](https://github.com/mxsm/rocketmq-rust/issues/5242))
- **refactor(error):** Reformat `Cargo.toml` using `taplo` with entry alignment ([#5232](https://github.com/mxsm/rocketmq-rust/issues/5232))
- **refactor(client):** Change `MQProducer::send_to_queue` return type to `RocketMQResult<Option<SendResult>>` in `mq_producer.rs`, `default_mq_producer.rs` and `transaction_mq_producer.rs` ([#5169](https://github.com/mxsm/rocketmq-rust/issues/5169))
- **refactor(client):** Replace `lazy_static!` with `std::sync::LazyLock` in `trace_view.rs` ([#5092](https://github.com/mxsm/rocketmq-rust/issues/5092))
- **refactor(store):** Replace `lazy_static!` with `std::sync::LazyLock` in `delivery.rs` and remove `lazy_static` dependency from `rocketmq-store` ([#5091](https://github.com/mxsm/rocketmq-rust/issues/5091))
- **refactor(common):** Replace `lazy_static!` with `std::sync::LazyLock` in `name_server_address_utils.rs` ([#5068](https://github.com/mxsm/rocketmq-rust/issues/5068))
- **refactor(common):** Replace `lazy_static!` with `std::sync::LazyLock` in `broker_config.rs` ([#5056](https://github.com/mxsm/rocketmq-rust/issues/5056))
- **refactor(remoting):** Replace `lazy_static!` with `std::sync::LazyLock` in `remoting_command.rs` and remove `lazy_static` dependency from `rocketmq-remoting` ([#5060](https://github.com/mxsm/rocketmq-rust/issues/5060))
- **perf(ArcMut):** Add the `#[inline]` attribute to the `mut_from_ref`, `downgrade`, and `get_inner` methods for `ArcMut`, improving performance ([#2876](https://github.com/mxsm/rocketmq-rust/pull/2876))
- **chore(controller):** Update default controller listen address to use port 60109 ([#5527](https://github.com/mxsm/rocketmq-rust/issues/5527))
