# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- **refactor(common):** Replace `lazy_static!` with `std::sync::LazyLock` in `name_server_address_utils.rs` ([#5068](https://github.com/mxsm/rocketmq-rust/issues/5068))
- **refactor(common):** Replace `lazy_static!` with `std::sync::LazyLock` in `broker_config.rs` ([#5056](https://github.com/mxsm/rocketmq-rust/issues/5056))
- **refactor(remoting):** Replace `lazy_static!` with `std::sync::LazyLock` in `remoting_command.rs` and remove `lazy_static` dependency from `rocketmq-remoting` ([#5060](https://github.com/mxsm/rocketmq-rust/issues/5060))
- **perf(ArcMut):** Add the `#[inline]` attribute to the `mut_from_ref`, `downgrade`, and `get_inner` methods for `ArcMut`, improving performance ([#2876](https://github.com/mxsm/rocketmq-rust/pull/2876))
