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

use super::HeaderRange;
use super::HeaderValueKind;

/// Decode behavior when canonical and legacy alias keys coexist.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AliasConflictPolicy {
    /// Reject different canonical and alias values.
    Error,
    /// Use the canonical value when an audited compatibility rule permits it.
    PreferCanonical,
}

/// Merge behavior between typed fields and dynamic extension fields.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DynamicCollisionPolicy {
    /// Accept an identical value but reject a different value.
    ErrorOnDifferentValue,
}

/// Missing-field semantics recorded in generated header schemas.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HeaderPresence {
    /// The key must be present.
    Required,
    /// Absence decodes as `None`.
    Optional,
    /// Absence uses `Default::default`.
    Default,
    /// Absence calls the named, reviewed default provider.
    DefaultWith(&'static str),
}

/// Presence semantics for a flattened nested header.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FlattenPresenceSpec {
    /// Always construct and validate the nested header.
    Always,
    /// Construct the nested header when any of its canonical keys or aliases exists.
    Any,
}

/// Static schema metadata for one non-flattened header field.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HeaderFieldSpec {
    /// Rust source field name.
    pub rust_field: &'static str,
    /// Canonical wire key used by encoders.
    pub key: &'static str,
    /// Ordered decode-only aliases.
    pub aliases: &'static [&'static str],
    /// Canonical/alias conflict behavior.
    pub alias_conflict: AliasConflictPolicy,
    /// Protocol value category.
    pub kind: HeaderValueKind,
    /// Missing-field behavior.
    pub presence: HeaderPresence,
    /// Stable literal or dynamic default semantic identifier.
    pub default_semantic: Option<&'static str>,
    /// Java field type used by compatibility tooling.
    pub java_type: Option<&'static str>,
    /// Signed Java range imposed on an unsigned Rust value.
    pub java_range: Option<HeaderRange>,
    /// Stable field order within the declaring header.
    pub binary_order: u16,
    /// Stable type identifier of the declaring header.
    pub declared_in: &'static str,
}

/// Static schema metadata for one flattened nested header.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HeaderFlattenSpec {
    /// Rust source field name.
    pub rust_field: &'static str,
    /// Stable typed-codec identifier of the nested header.
    pub nested_type_id: &'static str,
    /// Nested construction policy.
    pub presence: FlattenPresenceSpec,
    /// Stable group order within the declaring header.
    pub binary_order: u16,
    /// Stable type identifier of the declaring header.
    pub declared_in: &'static str,
}

/// Canonical resolution result for a canonical key or decode alias.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResolvedHeaderKey {
    /// Stable header name used in diagnostics.
    pub header: &'static str,
    /// Stable type identifier of the field owner, including nested owners.
    pub owner_type_id: &'static str,
    /// Canonical wire key.
    pub canonical: &'static str,
    /// Resolution precedence: canonical is zero and alias `n` is `n + 1`.
    pub precedence: u16,
    /// Canonical/alias conflict behavior.
    pub alias_conflict: AliasConflictPolicy,
    /// Typed/dynamic collision behavior.
    pub dynamic_collision: DynamicCollisionPolicy,
}
