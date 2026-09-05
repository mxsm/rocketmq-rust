// Copyright 2026 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Typed runtime primitives shared by request-header codec implementations.
//!
//! This module deliberately exposes only protocol-reviewed wire value types and
//! protocol-owned sinks. Both extension traits are sealed so adding a Rust type
//! or sink cannot silently expand RocketMQ wire semantics in another crate.

mod codec;
mod error;
mod field_source;
mod json_field_source;
mod json_writer;
mod schema;
mod sink;
mod value;

mod private {
    pub trait Sealed {}
    pub trait FieldSourceSealed {}
}

pub(crate) use crate::ProtocolContractViolation;
pub use codec::HeaderCodec;
#[doc(hidden)]
pub use error::into_rocketmq_error;
pub(crate) use field_source::BinaryHeaderFields;
pub use field_source::HeaderFieldSource;
pub(crate) use json_field_source::JsonHeaderFields;
pub(crate) use json_writer::write_json_string;
pub use schema::AliasConflictPolicy;
pub use schema::DynamicCollisionPolicy;
pub use schema::FlattenPresenceSpec;
pub use schema::HeaderFieldSpec;
pub use schema::HeaderFlattenSpec;
pub use schema::HeaderPresence;
pub use schema::ResolvedHeaderKey;
pub use sink::BinarySink;
pub use sink::EncodeSink;
pub use sink::JsonSink;
pub use sink::MapSink;
pub use value::validate_unsigned_java_range;
pub use value::HeaderFieldContext;
pub use value::HeaderRange;
pub use value::HeaderValue;
pub use value::HeaderValueKind;
