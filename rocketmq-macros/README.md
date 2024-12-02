# RocketMQ-Rust Macros

Using macros to automate repetitive tasks.

## Overview

This crate is primarily for the macros used in RocketMQ-Rust. The currently defined macros are as follows:

- **RequestHeaderCodec([Derive Macros](https://doc.rust-lang.org/reference/procedural-macros.html#derive-macros))**
- **RemotingSerializable([Derive Macros](https://doc.rust-lang.org/reference/procedural-macros.html#derive-macros))** - Currently not in use, implemented via alternative methods.

## RequestHeaderCodec Macro

The `RequestHeaderCodec` macro implements serialization and deserialization for RocketMQ's `RequestHeader`. It automatically implements the **`CommandCustomHeader trait`** and the **`FromMap trait`**. This macro includes an attribute: **`required`**, which indicates that a field of the struct must be present during deserialization; otherwise, it will result in an error.

> Note: The `required` attribute corresponds to the `@CFNotNull` annotation in the Java version and is implemented in this way in Rust.

Example usage is shown below:

```rust
#[derive(Debug, Clone, Serialize, Deserialize, Default, RequestHeaderCodec)]
#[serde(rename_all = "camelCase")]
pub struct SendMessageRequestHeader {
    #[required]  // this field is required
    pub producer_group: CheetahString,
    pub topic: CheetahString,
    pub default_topic: CheetahString,
    pub default_topic_queue_nums: i32,
    pub queue_id: Option<i32>,
    pub sys_flag: i32,
    pub born_timestamp: i64,
    pub flag: i32,
    pub properties: Option<CheetahString>,
    pub reconsume_times: Option<i32>,
    pub unit_mode: Option<bool>,
    pub batch: Option<bool>,
    pub max_reconsume_times: Option<i32>,
    #[serde(flatten)]
    pub topic_request_header: Option<TopicRequestHeader>,
}
```

In this example, the `SendMessageRequestHeader` struct is annotated with the `RequestHeaderCodec` derive macro, along with other standard Rust attributes like `Debug`, `Clone`, `Serialize`, `Deserialize`, and `Default`. The `#[required]` attribute is used on the `producer_group` field to denote that it is mandatory for successful deserialization.

