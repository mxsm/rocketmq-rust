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
    let declarations = model.fields.iter().filter(|field| !field.flattened).map(|field| {
        let context = context_ident(field);
        let base_type = field.option_inner.as_ref().unwrap_or(&field.ty);
        let key = literal(&field.key.value, field.key.span);
        let range = range_tokens(field, protocol_path);
        quote! {
            const #context: #context_type = #context_type::new(
                <Self as #protocol_path::protocol::header_codec::HeaderCodec>::TYPE_ID,
                #key,
                <#base_type as #value_trait>::KIND,
                #range,
            );
        }
    });
    quote!(#(#declarations)*)
}

pub(super) fn manual_fast_helpers(model: &HeaderModel, codec_trait: &TokenStream) -> TokenStream {
    let protocol_path = &model.protocol_path;
    let error_type = quote!(#protocol_path::protocol::header_codec::HeaderCodecError);
    let sink_trait = quote!(#protocol_path::protocol::header_codec::EncodeSink);
    let encode = local_encode_body(model, codec_trait, protocol_path);

    quote! {
        #[inline]
        fn __request_header_codec_v3_encode_local<__RequestHeaderSink: #sink_trait>(
            &self,
            sink: &mut __RequestHeaderSink,
        ) -> Result<(), #error_type> {
            #encode
        }
    }
}

pub(super) fn codec_items(model: &HeaderModel, codec_trait: &TokenStream) -> TokenStream {
    let protocol_path = &model.protocol_path;
    let error_type = quote!(#protocol_path::protocol::header_codec::HeaderCodecError);
    let sink_trait = quote!(#protocol_path::protocol::header_codec::EncodeSink);
    let map_type = quote!(#protocol_path::HeaderMap);
    let source_type = quote!(#protocol_path::protocol::header_codec::HeaderFieldSource);
    let validate = validation_body(model, &error_type);
    let encode = encode_body(model, codec_trait, protocol_path);
    let decode_source = decode_source_body(model, codec_trait, &error_type, protocol_path);
    let decode_source_inline = if model.fast
        && !model.fields.iter().any(|field| field.flattened)
        && model.fields.len() <= MAX_ALWAYS_INLINE_SOURCE_FIELDS
    {
        quote!(#[inline(always)])
    } else {
        quote!(#[inline])
    };
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

        #decode_source_inline
        fn decode_from_source(source: &dyn #source_type) -> Result<Self, #error_type> {
            #decode_source
        }

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

        let key = literal(&field.key.value, field.key.span);
        let context = context_ident(field);
        let optional_range_check = range_check(field, protocol_path, quote!(value));
        if field.option_inner.is_some() {
            quote! {
                if let Some(value) = &self.#ident {
                    #optional_range_check
                    sink.write(#key, value, Self::#context)?;
                }
            }
        } else {
            let range_check = range_check(field, protocol_path, quote!(&self.#ident));
            quote! {
                #range_check
                sink.write(#key, &self.#ident, Self::#context)?;
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

fn decode_source_body(
    model: &HeaderModel,
    codec_trait: &TokenStream,
    error_type: &TokenStream,
    protocol_path: &syn::Path,
) -> TokenStream {
    let scalar_fields: Vec<_> = model.fields.iter().filter(|field| !field.flattened).collect();
    let candidate_declarations = scalar_fields.iter().flat_map(|field| {
        field_candidates(field).map(|(_name, precedence)| {
            let local = raw_candidate_ident(field, precedence);
            quote!(let mut #local = None;)
        })
    });
    let arms = scalar_fields.iter().flat_map(|field| source_candidate_arms(field));
    let normalize_locals = scalar_fields
        .iter()
        .map(|field| source_candidate_normalization(field, error_type, codec_trait));
    let construct_fields = model
        .fields
        .iter()
        .map(|field| construct_field(field, codec_trait, error_type, protocol_path));

    quote! {
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

fn source_candidate_arms(field: &FieldModel) -> Vec<TokenStream> {
    field_candidates(field)
        .map(|(name, precedence)| {
            let name = literal(&name.value, name.span);
            let local = raw_candidate_ident(field, precedence);
            quote!(#name => { #local = Some(value); })
        })
        .collect()
}

fn source_candidate_normalization(
    field: &FieldModel,
    error_type: &TokenStream,
    codec_trait: &TokenStream,
) -> TokenStream {
    let local = raw_ident(field);
    let candidates: Vec<_> = field_candidates(field)
        .map(|(_name, precedence)| raw_candidate_ident(field, precedence))
        .collect();
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
    codec_trait: &TokenStream,
    error_type: &TokenStream,
    protocol_path: &syn::Path,
) -> TokenStream {
    let ident = &field.ident;
    if field.flattened {
        let base_type = field.option_inner.as_ref().unwrap_or(&field.ty);
        let decode = quote!(<#base_type as #codec_trait>::decode_from_source(source));
        let contains = quote!(<#base_type as #codec_trait>::contains_any_field_source(source));
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

    let local = raw_ident(field);
    let base_type = field.option_inner.as_ref().unwrap_or(&field.ty);
    let context = context_ident(field);
    let value_trait = quote!(#protocol_path::protocol::header_codec::HeaderValue);
    match field.missing.as_ref().expect("validated scalar missing policy") {
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
        MissingPolicy::Default => quote! {
            #ident: match #local {
                Some(raw) => <#base_type as #value_trait>::decode(raw, Self::#context)?,
                None => <#base_type as ::core::default::Default>::default(),
            },
        },
        MissingPolicy::DefaultWith(path) => quote! {
            #ident: match #local {
                Some(raw) => <#base_type as #value_trait>::decode(raw, Self::#context)?,
                None => #path(),
            },
        },
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
        "__REQUEST_HEADER_CODEC_V3_CONTEXT_{}",
        field.ident.to_string().to_ascii_uppercase(),
        span = field.span
    )
}

fn raw_ident(field: &FieldModel) -> syn::Ident {
    format_ident!(
        "__request_header_codec_v3_raw_{}",
        field.ident,
        span = Span::call_site()
    )
}

fn raw_candidate_ident(field: &FieldModel, precedence: u16) -> syn::Ident {
    format_ident!(
        "__request_header_codec_v3_raw_{}_{}",
        field.ident,
        precedence,
        span = Span::call_site()
    )
}

fn literal(value: &str, span: Span) -> syn::LitStr {
    syn::LitStr::new(value, span)
}
