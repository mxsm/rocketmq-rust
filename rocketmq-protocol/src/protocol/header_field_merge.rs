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

use std::collections::HashMap;

use crate::protocol::command_custom_header::CommandCustomHeader;
use crate::protocol::command_custom_header::HeaderMap;
use crate::protocol::header_codec::AliasConflictPolicy;
use crate::protocol::header_codec::DynamicCollisionPolicy;
use crate::protocol::header_codec::HeaderCodecError;

type FieldIdentity = (&'static str, &'static str);

/// Returns whether a dynamic field names a canonical typed key or decode alias.
#[inline]
pub(crate) fn has_custom_ext_collision(header: &dyn CommandCustomHeader, dynamic: Option<&HeaderMap>) -> bool {
    dynamic.is_some_and(|fields| fields.keys().any(|key| header.contains_wire_key(key.as_str())))
}

/// Merges typed and dynamic extension fields under the authoritative V3 policy.
///
/// The typed map is also the result map. Temporary selection metadata is only
/// allocated after a typed/dynamic semantic overlap is found.
pub(crate) fn merge_header_and_dynamic(
    header: &dyn CommandCustomHeader,
    dynamic: Option<&HeaderMap>,
) -> Result<HeaderMap, HeaderCodecError> {
    let mut merged = HeaderMap::with_capacity(dynamic.map_or(0, HeaderMap::len));
    header.try_encode_into_map(&mut merged)?;

    let Some(dynamic) = dynamic else {
        return Ok(merged);
    };

    let mut selected_dynamic: Option<HashMap<FieldIdentity, u16>> = None;
    for (raw_key, value) in dynamic {
        let Some(resolved) = header.resolve_wire_key(raw_key.as_str()) else {
            // Legacy codecs without a resolver retain their historical exact-key
            // typed-wins behavior. Unknown dynamic fields remain untouched.
            if !merged.contains_key(raw_key) {
                merged.insert(raw_key.clone(), value.clone());
            }
            continue;
        };

        let identity = (resolved.owner_type_id, resolved.canonical);
        if let Some(selected_precedence) = selected_dynamic
            .as_mut()
            .and_then(|selected| selected.get_mut(&identity))
        {
            let Some(selected_value) = merged.get(resolved.canonical) else {
                merged.insert(resolved.canonical.into(), value.clone());
                *selected_precedence = resolved.precedence;
                continue;
            };
            if selected_value == value {
                *selected_precedence = (*selected_precedence).min(resolved.precedence);
                continue;
            }

            match resolved.alias_conflict {
                AliasConflictPolicy::Error => {
                    return Err(HeaderCodecError::Conflict {
                        header: resolved.header,
                        key: resolved.canonical,
                    });
                }
                AliasConflictPolicy::PreferCanonical if resolved.precedence < *selected_precedence => {
                    merged.insert(resolved.canonical.into(), value.clone());
                    *selected_precedence = resolved.precedence;
                }
                AliasConflictPolicy::PreferCanonical => {}
            }
            continue;
        }

        if let Some(typed_value) = merged.get(resolved.canonical) {
            if typed_value != value {
                match resolved.dynamic_collision {
                    DynamicCollisionPolicy::ErrorOnDifferentValue => {
                        return Err(HeaderCodecError::DynamicFieldConflict {
                            header: resolved.header,
                            key: resolved.canonical,
                        });
                    }
                }
            }
            continue;
        }

        merged.insert(resolved.canonical.into(), value.clone());
        selected_dynamic
            .get_or_insert_with(HashMap::new)
            .insert(identity, resolved.precedence);
    }

    Ok(merged)
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;
    use rocketmq_macros::RequestHeaderCodecV3;

    use super::*;
    use crate::rpc::rpc_request_header::RpcRequestHeader;

    #[derive(RequestHeaderCodecV3)]
    #[header(type_id = "rocketmq_protocol::tests::StrictAliasHeader")]
    struct StrictAliasHeader {
        #[header(key = "canonical", alias = "legacy")]
        value: Option<CheetahString>,
    }

    #[test]
    fn identical_typed_alias_is_deduplicated_to_the_canonical_key() {
        let header = RpcRequestHeader::new(Some("tenant".into()), None, None, None);
        let dynamic = HeaderMap::from([("namespace".into(), "tenant".into()), ("trace".into(), "x".into())]);

        let merged = merge_header_and_dynamic(&header, Some(&dynamic)).unwrap();

        assert_eq!(merged.get("ns").map(CheetahString::as_str), Some("tenant"));
        assert!(!merged.contains_key("namespace"));
        assert_eq!(merged.get("trace").map(CheetahString::as_str), Some("x"));
    }

    #[test]
    fn different_typed_alias_is_rejected_without_exposing_values() {
        let header = RpcRequestHeader::new(Some("secret-alpha".into()), None, None, None);
        let dynamic = HeaderMap::from([("namespace".into(), "secret-beta".into())]);

        let error = merge_header_and_dynamic(&header, Some(&dynamic)).unwrap_err();

        assert!(matches!(
            error,
            HeaderCodecError::DynamicFieldConflict {
                header: "RpcRequestHeader",
                key: "ns"
            }
        ));
        assert!(!error.to_string().contains("secret-alpha"));
        assert!(!error.to_string().contains("secret-beta"));
    }

    #[test]
    fn absent_typed_aliases_use_frozen_precedence_in_any_insertion_order() {
        let header = RpcRequestHeader::default();
        for entries in [
            [("namespace", "legacy"), ("ns", "canonical")],
            [("ns", "canonical"), ("namespace", "legacy")],
        ] {
            let dynamic = entries
                .into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect();

            let merged = merge_header_and_dynamic(&header, Some(&dynamic)).unwrap();

            assert_eq!(merged.get("ns").map(CheetahString::as_str), Some("canonical"));
            assert!(!merged.contains_key("namespace"));
        }
    }

    #[test]
    fn strict_absent_alias_conflict_is_rejected_in_any_insertion_order() {
        let header = StrictAliasHeader { value: None };
        for entries in [
            [("legacy", "old"), ("canonical", "new")],
            [("canonical", "new"), ("legacy", "old")],
        ] {
            let dynamic = entries
                .into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect();

            let error = merge_header_and_dynamic(&header, Some(&dynamic)).unwrap_err();

            assert!(matches!(
                error,
                HeaderCodecError::Conflict {
                    header: "StrictAliasHeader",
                    key: "canonical"
                }
            ));
        }
    }

    #[test]
    fn collision_probe_uses_canonical_keys_and_aliases_only() {
        let header = RpcRequestHeader::default();
        assert!(has_custom_ext_collision(
            &header,
            Some(&HeaderMap::from([("namespace".into(), "tenant".into())]))
        ));
        assert!(!has_custom_ext_collision(
            &header,
            Some(&HeaderMap::from([("trace".into(), "value".into())]))
        ));
    }
}
