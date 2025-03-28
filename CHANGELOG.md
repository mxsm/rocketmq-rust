# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- **perf(ArcMut):** Add the `#[inline]` attribute to the `mut_from_ref`, `downgrade`, and `get_inner` methods for `ArcMut`, improving performance ([#2876](https://github.com/mxsm/rocketmq-rust/pull/2876))