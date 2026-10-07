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

use proc_macro2::{Span, TokenStream};
use quote::{format_ident, quote};

use super::model::{
    AliasConflict, FieldModel, FlattenPresence, HeaderModel, HeaderRange, MissingPolicy, ValueKind, WireName,
};

// Keep small flat decoders at the production call site without duplicating
// larger or recursively flattened state machines in every compatibility shim.
const MAX_ALWAYS_INLINE_SOURCE_FIELDS: usize = 6;

pub(super) fn context_declarations(model: &HeaderModel) -> TokenStream {
    let protocol_path = &model.protocol_path;
    let value_trait = quote!(#protocol_path::protocol::header_codec::HeaderValue);
    let context_type = quote!(#protocol_path::protocol::header_codec::HeaderFieldContext);
    let key_type = quote!(#protocol_path::protocol::header_codec::HeaderFieldKey);
    let declarations = model.fields.iter().filter(|field| !field.flattened).map(|field| {
        let context = context_ident(field);
        let prepared_key = key_ident(field);
        let base_type = field.option_inner.as_ref().unwrap_or(&field.ty);
        let key = literal(&field.key.value, field.key.span);
        let binary_key = binary_key_literal(&field.key.value);
        let json_key = json_key_literal(&field.key.value);
        let range = range_tokens(field, protocol_path);
        quote! {
            const #context: #context_type = #context_type::new(
                <Self as #protocol_path::protocol::header_codec::HeaderCodec>::TYPE_ID,
                #key,
                <#base_type as #value_trait>::KIND,
                #range,
            );
            const #prepared_key: #key_type = #key_type::new(#key, #binary_key, #json_key);
        }
    });
    quote!(#(#declarations)*)
}

pub(super) fn manual_fast_helpers(model: &HeaderModel, codec_trait: &TokenStream) -> TokenStream {
    let protocol_path = &model.protocol_path;
    let error_type = quote!(#protocol_path::ProtocolContractViolation);
    let sink_trait = quote!(#protocol_path::protocol::header_codec::EncodeSink);
    let encode = local_encode_body(model, codec_trait, protocol_path);

    quote! {
        #[inline]
        fn __request_header_codec_encode_local<__RequestHeaderSink: #sink_trait>(
            &self,
            sink: &mut __RequestHeaderSink,
        ) -> Result<(), #error_type> {
            #encode
        }
    }
}

pub(super) fn codec_items(model: &HeaderModel, codec_trait: &TokenStream) -> TokenStream {
    let protocol_path = &model.protocol_path;
    let error_type = quote!(#protocol_path::ProtocolContractViolation);
    let sink_trait = quote!(#protocol_path::protocol::header_codec::EncodeSink);
    let map_type = quote!(#protocol_path::HeaderMap);
    let validate = validation_body(model, &error_type);
    let encode = encode_body(model, codec_trait, protocol_path);
    let decode_source_inline = if model.fast
        && !model.fields.iter().any(|field| field.flattened)
        && model.fields.len() <= MAX_ALWAYS_INLINE_SOURCE_FIELDS
    {
        quote!(#[inline(always)])
    } else {
        quote!(#[inline(never)])
    };
    let decode = decode_items(model, codec_trait, &error_type, protocol_path, &decode_source_inline);
    let len_hint = len_hint_body(model, codec_trait, protocol_path);

    quote! {
        #[inline]
        fn validate_for_wire(&self) -> Result<(), #error_type> {
            #validate
        }

        #[inline]
        fn encode_into<__RequestHeaderSink: #sink_trait>(
            &self,
            sink: &mut __RequestHeaderSink,
        ) -> Result<(), #error_type> {
            #encode
        }

        #[inline]
        fn decode_from_map(map: &#map_type) -> Result<Self, #error_type> {
            <Self as #codec_trait>::decode_from_source(map)
        }

        #decode

        #[inline]
        fn contains_any_field(map: &#map_type) -> bool {
            <Self as #codec_trait>::contains_any_field_source(map)
        }

        #[inline]
        fn encoded_len_hint(&self) -> usize {
            #len_hint
        }
    }
}

fn validation_body(model: &HeaderModel, error_type: &TokenStream) -> TokenStream {
    let type_id = &model.type_id;
    let required_strings = model
        .fields
        .iter()
        .filter(|field| {
            !field.flattened
                && field.option_inner.is_none()
                && field.kind == ValueKind::String
                && matches!(field.missing, Some(MissingPolicy::Required))
        })
        .map(|field| {
            let ident = &field.ident;
            let rule = literal(&format!("required_non_empty:{}", field.key.value), field.span);
            quote! {
                if self.#ident.is_empty() {
                    return Err(#error_type::Validation { header: #type_id, rule: #rule });
                }
            }
        });
    let custom = model.validate_path.as_ref().map(|path| quote!(#path(self)?;));
    quote! {
        #(#required_strings)*
        #custom
        Ok(())
    }
}

fn encode_body(model: &HeaderModel, codec_trait: &TokenStream, protocol_path: &syn::Path) -> TokenStream {
    encode_selected_body(model.fields.iter(), codec_trait, protocol_path)
}

fn local_encode_body(model: &HeaderModel, codec_trait: &TokenStream, protocol_path: &syn::Path) -> TokenStream {
    encode_selected_body(
        model.fields.iter().filter(|field| !field.flattened),
        codec_trait,
        protocol_path,
    )
}

fn encode_selected_body<'a>(
    fields: impl Iterator<Item = &'a FieldModel>,
    codec_trait: &TokenStream,
    protocol_path: &syn::Path,
) -> TokenStream {
    let mut fields: Vec<_> = fields.collect();
    fields.sort_by_key(|field| field.stable_order());
    let writes = fields.into_iter().map(|field| {
        let ident = &field.ident;
        if field.flattened {
            return if field.option_inner.is_some() {
                quote! {
                    if let Some(value) = &self.#ident {
                        #codec_trait::encode_into(value, sink)?;
                    }
                }
            } else {
                quote!(#codec_trait::encode_into(&self.#ident, sink)?;)
            };
        }

        let prepared_key = key_ident(field);
        let context = context_ident(field);
        let optional_range_check = range_check(field, protocol_path, quote!(value));
        if field.option_inner.is_some() {
            quote! {
                if let Some(value) = &self.#ident {
                    #optional_range_check
                    sink.write_field(Self::#prepared_key, value, Self::#context)?;
                }
            }
        } else {
            let range_check = range_check(field, protocol_path, quote!(&self.#ident));
            quote! {
                #range_check
                sink.write_field(Self::#prepared_key, &self.#ident, Self::#context)?;
            }
        }
    });
    quote! {
        <Self as #codec_trait>::validate_for_wire(self)?;
        #(#writes)*
        Ok(())
    }
}

fn range_check(field: &FieldModel, protocol_path: &syn::Path, value: TokenStream) -> TokenStream {
    if field.range.is_none() {
        return quote! {};
    }
    let context = context_ident(field);
    quote! {
        #protocol_path::protocol::header_codec::validate_unsigned_java_range(
            (*#value) as u64,
            Self::#context,
        )?;
    }
}

/// Where the generated decoder reads one field from while it builds the header.
enum FieldInput {
    /// Expression of type `Option<&str>` holding the selected wire value.
    Scalar(TokenStream),
    /// Expressions that decode a flattened header and report whether one of its keys is present.
    Flatten { decode: TokenStream, contains: TokenStream },
}

fn decode_items(
    model: &HeaderModel,
    codec_trait: &TokenStream,
    error_type: &TokenStream,
    protocol_path: &syn::Path,
    decode_source_inline: &TokenStream,
) -> TokenStream {
    // The slot array is sized by associated constants, which stable Rust only
    // accepts for concrete types. Generic headers keep one scan per layer.
    if model.generics.params.is_empty() {
        slot_decode_items(model, codec_trait, error_type, protocol_path, decode_source_inline)
    } else {
        scan_decode_items(model, codec_trait, error_type, protocol_path, decode_source_inline)
    }
}

/// Generates a decoder that fills one slot per wire name during a single scan
/// and lets flattened children read their own slot range from the same array.
fn slot_decode_items(
    model: &HeaderModel,
    codec_trait: &TokenStream,
    error_type: &TokenStream,
    protocol_path: &syn::Path,
    decode_source_inline: &TokenStream,
) -> TokenStream {
    let ident = &model.ident;
    let source_type = quote!(#protocol_path::protocol::header_codec::HeaderFieldSource);
    let slot_count = quote!(<#ident as #codec_trait>::SLOT_COUNT);

    // Local wire names take the leading slots in declaration order. Each
    // flattened child then owns the next `SLOT_COUNT` slots of its type.
    let mut first_slots = Vec::with_capacity(model.fields.len());
    let mut local_slots = 0usize;
    for field in &model.fields {
        first_slots.push(local_slots);
        if !field.flattened {
            local_slots += 1 + field.aliases.len();
        }
    }

    let mut arms = Vec::new();
    let mut normalize_locals = Vec::new();
    let mut forwards = Vec::new();
    let mut construct_fields = Vec::with_capacity(model.fields.len());
    let mut child_types: Vec<&syn::Type> = Vec::new();
    for (field, first_slot) in model.fields.iter().zip(first_slots) {
        let input = if field.flattened {
            let base_type = field.option_inner.as_ref().unwrap_or(&field.ty);
            let start = quote!(#local_slots #(+ <#child_types as #codec_trait>::SLOT_COUNT)*);
            let range = quote!(#start..#start + <#base_type as #codec_trait>::SLOT_COUNT);
            // Every layer sees every field, exactly as with one scan per layer.
            forwards.push(quote! {
                <#base_type as #codec_trait>::collect_slot(&mut slots[#range], key, value);
            });
            child_types.push(base_type);
            FieldInput::Flatten {
                decode: quote!(<#base_type as #codec_trait>::decode_from_slots(&slots[#range], source)),
                contains: quote! {
                    if <#base_type as #codec_trait>::SUPPORTS_SLOT_DECODE {
                        slots[#range].iter().any(::core::option::Option::is_some)
                    } else {
                        <#base_type as #codec_trait>::contains_any_field_source(source)
                    }
                },
            }
        } else {
            let mut candidates = Vec::with_capacity(1 + field.aliases.len());
            for (name, precedence) in field_candidates(field) {
                let name = literal(&name.value, name.span);
                let slot = first_slot + usize::from(precedence);
                arms.push(quote!(#name => slots[#slot] = Some(value),));
                candidates.push(quote!(slots[#slot]));
            }
            if field.aliases.is_empty() {
                FieldInput::Scalar(quote!(slots[#first_slot]))
            } else {
                let local = raw_ident(field);
                normalize_locals.push(candidate_normalization(
                    field,
                    &local,
                    &candidates,
                    error_type,
                    codec_trait,
                ));
                FieldInput::Scalar(quote!(#local))
            }
        };
        construct_fields.push(construct_field(field, &input, codec_trait, error_type, protocol_path));
    }

    let constants = quote! {
        const SUPPORTS_SLOT_DECODE: bool = true;
        const SLOT_COUNT: usize = #local_slots #(+ <#child_types as #codec_trait>::SLOT_COUNT)*;
    };

    if model.fields.is_empty() {
        return quote! {
            #constants

            #decode_source_inline
            fn decode_from_source(_source: &dyn #source_type) -> Result<Self, #error_type> {
                let header = Self {};
                <Self as #codec_trait>::validate_for_wire(&header)?;
                Ok(header)
            }

            #[inline]
            fn decode_from_slots(
                _slots: &[Option<&str>],
                _source: &dyn #source_type,
            ) -> Result<Self, #error_type> {
                let header = Self {};
                <Self as #codec_trait>::validate_for_wire(&header)?;
                Ok(header)
            }
        };
    }

    quote! {
        #constants

        #[inline]
        fn collect_slot<'__slot>(slots: &mut [Option<&'__slot str>], key: &str, value: &'__slot str) {
            let Ok(slots) = <&mut [Option<&'__slot str>; #slot_count]>::try_from(slots) else {
                return;
            };
            match key {
                #(#arms)*
                _ => {}
            }
            #(#forwards)*
        }

        #[inline]
        fn decode_from_slots(slots: &[Option<&str>], source: &dyn #source_type) -> Result<Self, #error_type> {
            // A caller that sized the array for another schema gets a fresh scan instead.
            let Ok(slots) = <&[Option<&str>; #slot_count]>::try_from(slots) else {
                return <Self as #codec_trait>::decode_from_source(source);
            };
            #(#normalize_locals)*
            let header = Self {
                #(#construct_fields)*
            };
            <Self as #codec_trait>::validate_for_wire(&header)?;
            Ok(header)
        }

        #decode_source_inline
        fn decode_from_source(source: &dyn #source_type) -> Result<Self, #error_type> {
            let mut slots = [None; #slot_count];
            source.visit_fields_while(&mut |key, value| {
                <Self as #codec_trait>::collect_slot(&mut slots, key, value);
                true
            });
            <Self as #codec_trait>::decode_from_slots(&slots, source)
        }
    }
}

/// Generates a decoder that scans the source once for this header layer and
/// lets every flattened child scan it again.
fn scan_decode_items(
    model: &HeaderModel,
    codec_trait: &TokenStream,
    error_type: &TokenStream,
    protocol_path: &syn::Path,
    decode_source_inline: &TokenStream,
) -> TokenStream {
    let source_type = quote!(#protocol_path::protocol::header_codec::HeaderFieldSource);
    let scalar_fields: Vec<_> = model.fields.iter().filter(|field| !field.flattened).collect();
    let candidate_declarations = scalar_fields.iter().flat_map(|field| {
        field_candidates(field).map(|(_name, precedence)| {
            let local = raw_candidate_ident(field, precedence);
            quote!(let mut #local = None;)
        })
    });
    let arms = scalar_fields.iter().flat_map(|field| {
        field_candidates(field).map(|(name, precedence)| {
            let name = literal(&name.value, name.span);
            let local = raw_candidate_ident(field, precedence);
            quote!(#name => { #local = Some(value); })
        })
    });
    let normalize_locals = scalar_fields.iter().map(|field| {
        let candidates: Vec<_> = field_candidates(field)
            .map(|(_name, precedence)| {
                let candidate = raw_candidate_ident(field, precedence);
                quote!(#candidate)
            })
            .collect();
        candidate_normalization(field, &raw_ident(field), &candidates, error_type, codec_trait)
    });
    let construct_fields = model.fields.iter().map(|field| {
        let input = if field.flattened {
            let base_type = field.option_inner.as_ref().unwrap_or(&field.ty);
            FieldInput::Flatten {
                decode: quote!(<#base_type as #codec_trait>::decode_from_source(source)),
                contains: quote!(<#base_type as #codec_trait>::contains_any_field_source(source)),
            }
        } else {
            let local = raw_ident(field);
            FieldInput::Scalar(quote!(#local))
        };
        construct_field(field, &input, codec_trait, error_type, protocol_path)
    });

    quote! {
        #decode_source_inline
        fn decode_from_source(source: &dyn #source_type) -> Result<Self, #error_type> {
            #(#candidate_declarations)*
            source.visit_fields_while(&mut |key, value| {
                match key {
                    #(#arms)*
                    _ => {}
                }
                true
            });
            #(#normalize_locals)*
            let header = Self {
                #(#construct_fields)*
            };
            <Self as #codec_trait>::validate_for_wire(&header)?;
            Ok(header)
        }
    }
}

/// Selects one raw value among a field's canonical key and aliases.
fn candidate_normalization(
    field: &FieldModel,
    local: &syn::Ident,
    candidates: &[TokenStream],
    error_type: &TokenStream,
    codec_trait: &TokenStream,
) -> TokenStream {
    // A single wire name has nothing to conflict with.
    if let [candidate] = candidates {
        return quote!(let #local = #candidate;);
    }
    match field.alias_conflict {
        AliasConflict::PreferCanonical => quote! {
            let #local = None #(.or(#candidates))*;
        },
        AliasConflict::Error => {
            let key = literal(&field.key.value, field.key.span);
            let selections = candidates.iter().map(|candidate| {
                quote! {
                    if let Some(value) = #candidate {
                        match selected {
                            None => selected = Some(value),
                            Some(current) if current == value => {}
                            Some(_) => {
                                return Err(#error_type::Conflict {
                                    header: <Self as #codec_trait>::TYPE_ID,
                                    key: #key,
                                });
                            }
                        }
                    }
                }
            });
            quote! {
                let #local = {
                    let mut selected = None;
                    #(#selections)*
                    selected
                };
            }
        }
    }
}

fn field_candidates(field: &FieldModel) -> impl Iterator<Item = (&WireName, u16)> {
    std::iter::once((&field.key, 0_u16)).chain(
        field
            .aliases
            .iter()
            .enumerate()
            .map(|(index, alias)| (alias, (index + 1) as u16)),
    )
}

fn construct_field(
    field: &FieldModel,
    input: &FieldInput,
    codec_trait: &TokenStream,
    error_type: &TokenStream,
    protocol_path: &syn::Path,
) -> TokenStream {
    let ident = &field.ident;
    let local = match input {
        FieldInput::Flatten { decode, contains } => {
            return if field.option_inner.is_some() {
                match field.flatten_presence.unwrap_or(FlattenPresence::Always) {
                    FlattenPresence::Always => quote!(#ident: Some(#decode?),),
                    FlattenPresence::Any => quote! {
                        #ident: if #contains {
                            Some(#decode?)
                        } else {
                            None
                        },
                    },
                }
            } else {
                quote!(#ident: #decode?,)
            };
        }
        FieldInput::Scalar(raw) => raw,
    };

    let base_type = field.option_inner.as_ref().unwrap_or(&field.ty);
    let context = context_ident(field);
    let value_trait = quote!(#protocol_path::protocol::header_codec::HeaderValue);
    let Some(missing) = field.missing.as_ref() else {
        debug_assert!(
            field.missing.is_some(),
            "scalar field must have a validated missing policy"
        );
        return TokenStream::new();
    };
    match missing {
        MissingPolicy::Optional => quote! {
            #ident: #local
                .map(|raw| <#base_type as #value_trait>::decode(raw, Self::#context))
                .transpose()?,
        },
        MissingPolicy::Required => {
            let key = literal(&field.key.value, field.key.span);
            quote! {
                #ident: <#base_type as #value_trait>::decode(
                    #local.ok_or(#error_type::Missing {
                        header: <Self as #codec_trait>::TYPE_ID,
                        key: #key,
                    })?,
                    Self::#context,
                )?,
            }
        }
        MissingPolicy::Default => {
            if field.option_inner.is_some() {
                quote! {
                    #ident: match #local {
                        Some(raw) => Some(<#base_type as #value_trait>::decode(raw, Self::#context)?),
                        None => Some(<#base_type as ::core::default::Default>::default()),
                    },
                }
            } else {
                quote! {
                    #ident: match #local {
                        Some(raw) => <#base_type as #value_trait>::decode(raw, Self::#context)?,
                        None => <#base_type as ::core::default::Default>::default(),
                    },
                }
            }
        }
        MissingPolicy::DefaultWith(path) => {
            if field.option_inner.is_some() {
                quote! {
                    #ident: match #local {
                        Some(raw) => Some(<#base_type as #value_trait>::decode(raw, Self::#context)?),
                        None => #path(),
                    },
                }
            } else {
                quote! {
                    #ident: match #local {
                        Some(raw) => <#base_type as #value_trait>::decode(raw, Self::#context)?,
                        None => #path(),
                    },
                }
            }
        }
    }
}

fn len_hint_body(model: &HeaderModel, codec_trait: &TokenStream, protocol_path: &syn::Path) -> TokenStream {
    let value_trait = quote!(#protocol_path::protocol::header_codec::HeaderValue);
    let adds = model.fields.iter().map(|field| {
        let ident = &field.ident;
        if field.flattened {
            return if field.option_inner.is_some() {
                quote! {
                    if let Some(value) = &self.#ident {
                        len = len.saturating_add(#codec_trait::encoded_len_hint(value));
                    }
                }
            } else {
                quote!(len = len.saturating_add(#codec_trait::encoded_len_hint(&self.#ident));)
            };
        }
        let overhead = 6_usize.saturating_add(field.key.value.len());
        if field.option_inner.is_some() {
            quote! {
                if let Some(value) = &self.#ident {
                    len = len.saturating_add(#overhead).saturating_add(#value_trait::encoded_len(value));
                }
            }
        } else {
            quote! {
                len = len.saturating_add(#overhead).saturating_add(#value_trait::encoded_len(&self.#ident));
            }
        }
    });
    quote! {
        let mut len = 0usize;
        #(#adds)*
        len
    }
}

fn range_tokens(field: &FieldModel, protocol_path: &syn::Path) -> TokenStream {
    match field.range {
        None => quote!(None),
        Some(HeaderRange::I32) => quote!(Some(#protocol_path::protocol::header_codec::HeaderRange::I32)),
        Some(HeaderRange::I64) => quote!(Some(#protocol_path::protocol::header_codec::HeaderRange::I64)),
    }
}

fn context_ident(field: &FieldModel) -> syn::Ident {
    format_ident!(
        "__REQUEST_HEADER_CODEC_CONTEXT_{}",
        field.ident.to_string().to_ascii_uppercase(),
        span = field.span
    )
}

fn key_ident(field: &FieldModel) -> syn::Ident {
    format_ident!(
        "__REQUEST_HEADER_CODEC_KEY_{}",
        field.ident.to_string().to_ascii_uppercase(),
        span = field.span
    )
}

/// The key as a ROCKETMQ pair writes it: big-endian `u16` length, then the bytes.
fn binary_key_literal(key: &str) -> proc_macro2::Literal {
    // Validation already rejected keys longer than the wire length field.
    let length = u16::try_from(key.len()).unwrap_or(u16::MAX);
    let mut encoded = Vec::with_capacity(key.len() + 2);
    encoded.extend_from_slice(&length.to_be_bytes());
    encoded.extend_from_slice(key.as_bytes());
    proc_macro2::Literal::byte_string(&encoded)
}

/// The key as a JSON member name followed by `:`, or an empty literal when the
/// key needs escaping and the sink must therefore escape it at runtime.
fn json_key_literal(key: &str) -> proc_macro2::Literal {
    let plain = key.bytes().all(|byte| byte >= 0x20 && byte != b'"' && byte != b'\\');
    if !plain {
        return proc_macro2::Literal::byte_string(b"");
    }
    let mut encoded = Vec::with_capacity(key.len() + 3);
    encoded.push(b'"');
    encoded.extend_from_slice(key.as_bytes());
    encoded.extend_from_slice(b"\":");
    proc_macro2::Literal::byte_string(&encoded)
}

fn raw_ident(field: &FieldModel) -> syn::Ident {
    format_ident!("__request_header_codec_raw_{}", field.ident, span = Span::call_site())
}

fn raw_candidate_ident(field: &FieldModel, precedence: u16) -> syn::Ident {
    format_ident!(
        "__request_header_codec_raw_{}_{}",
        field.ident,
        precedence,
        span = Span::call_site()
    )
}

fn literal(value: &str, span: Span) -> syn::LitStr {
    syn::LitStr::new(value, span)
}

#[cfg(test)]
mod tests {
    use syn::parse_quote;

    use super::*;

    fn byte_string(literal: proc_macro2::Literal) -> Vec<u8> {
        syn::parse_str::<syn::LitByteStr>(&literal.to_string())
            .expect("byte string literal")
            .value()
    }

    /// Generated tokens without the whitespace `TokenStream` prints between them.
    fn compact(tokens: TokenStream) -> String {
        tokens.to_string().split_whitespace().collect()
    }

    fn model(input: syn::DeriveInput) -> HeaderModel {
        HeaderModel::parse(input).expect("model")
    }

    fn codec_trait() -> TokenStream {
        quote!(protocol_api::protocol::header_codec::HeaderCodec)
    }

    #[test]
    fn prepared_key_literals_match_the_wire_encodings() {
        assert_eq!(byte_string(binary_key_literal("queueId")), b"\x00\x07queueId");
        let long_key = "k".repeat(300);
        assert_eq!(
            byte_string(binary_key_literal(&long_key)),
            [&[1_u8, 44][..], long_key.as_bytes()].concat()
        );
        assert_eq!(byte_string(json_key_literal("queueId")), b"\"queueId\":");
        assert_eq!(byte_string(json_key_literal("主题")), "\"主题\":".as_bytes());
        for needs_escaping in ["a\"b", "a\\b", "a\nb"] {
            assert!(byte_string(json_key_literal(needs_escaping)).is_empty());
        }
    }

    #[test]
    fn concrete_headers_decode_flattened_children_from_shared_slots() {
        let model = model(parse_quote! {
            #[header(type_id = "fixtures::Parent", crate = "protocol_api")]
            struct Parent {
                #[header(required)]
                id: i32,
                #[header(key = "name", alias = "legacyName", alias_conflict = "prefer_canonical")]
                name: Option<String>,
                #[header(flatten, presence = "any")]
                nested: Option<Nested>,
            }
        });
        let tokens = compact(codec_items(&model, &codec_trait()));

        assert_eq!(tokens.matches("visit_fields_while").count(), 1);
        assert!(tokens.contains("constSUPPORTS_SLOT_DECODE:bool=true;"));
        assert!(tokens.contains(
            "constSLOT_COUNT:usize=3usize+<Nestedasprotocol_api::protocol::header_codec::HeaderCodec>::SLOT_COUNT;"
        ));
        assert!(tokens.contains("\"legacyName\"=>slots[2usize]=Some(value),"));
        assert!(tokens.contains("::collect_slot(&mutslots[3usize..3usize+"));
        assert!(tokens.contains("::decode_from_slots(&slots[3usize..3usize+"));
        assert!(!tokens.contains("Nestedasprotocol_api::protocol::header_codec::HeaderCodec>::decode_from_source"));
    }

    #[test]
    fn generic_headers_keep_one_scan_per_layer() {
        let model = model(parse_quote! {
            #[header(type_id = "fixtures::Generic", crate = "protocol_api")]
            struct Generic<T> {
                #[header(required)]
                value: T,
                #[header(flatten)]
                nested: Nested<T>,
            }
        });
        let tokens = compact(codec_items(&model, &codec_trait()));

        assert!(!tokens.contains("SLOT_COUNT"));
        assert!(!tokens.contains("collect_slot"));
        assert!(tokens.contains("::decode_from_source(source)?"));
    }

    #[test]
    fn encoders_write_fields_through_prepared_keys() {
        let model = model(parse_quote! {
            #[header(type_id = "fixtures::Encoded", crate = "protocol_api")]
            struct Encoded {
                #[header(required)]
                queue_id: i32,
                remark: Option<String>,
            }
        });
        let declarations = compact(context_declarations(&model));
        let items = compact(codec_items(&model, &codec_trait()));

        assert!(declarations.contains(
            "const__REQUEST_HEADER_CODEC_KEY_QUEUE_ID:protocol_api::protocol::header_codec::HeaderFieldKey="
        ));
        assert!(items.contains(
            "sink.write_field(Self::__REQUEST_HEADER_CODEC_KEY_QUEUE_ID,&self.queue_id,Self::\
             __REQUEST_HEADER_CODEC_CONTEXT_QUEUE_ID)?;"
        ));
        assert!(items
            .contains("sink.write_field(Self::__REQUEST_HEADER_CODEC_KEY_REMARK,value,Self::__REQUEST_HEADER_CODEC_CONTEXT_REMARK)?;"));
    }
}
