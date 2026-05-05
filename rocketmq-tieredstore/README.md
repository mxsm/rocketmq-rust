# rocketmq-tieredstore

Tiered storage crate for `rocketmq-rust`.

This crate is being migrated from Apache RocketMQ Java tieredstore semantics in staged steps. The first stage provides the independent crate boundary, core traits, metadata model, provider abstraction, and basic memory/POSIX segment IO.
