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
    AliasConflict, FieldModel, FlattenPresence, HeaderModel, HeaderRange, LookupPlan, MissingPolicy, ValueKind,
};

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
    let len_hint = local_len_hint_body(model, protocol_path);

    quote! {
        #[inline]
        fn __request_header_codec_v3_encode_local<__RequestHeaderSink: #sink_trait>(
            &self,
            sink: &mut __RequestHeaderSink,
        ) -> Result<(), #error_type> {
            #encode
        }

        #[inline]
        fn __request_header_codec_v3_local_encoded_len_hint(&self) -> usize {
            #len_hint
        }
    }
}

pub(super) fn codec_items(model: &HeaderModel, codec_trait: &TokenStream) -> TokenStream {
    let protocol_path = &model.protocol_path;
    let error_type = quote!(#protocol_path::protocol::header_codec::HeaderCodecError);
    let sink_trait = quote!(#protocol_path::protocol::header_codec::EncodeSink);
    let map_type = quote!(#protocol_path::HeaderMap);
    let validate = validation_body(model, &error_type);
    let encode = encode_body(model, codec_trait, protocol_path);
    let decode = decode_body(model, codec_trait, &error_type, protocol_path);
    let contains = contains_body(model, codec_trait);
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
            #decode
        }

        #[inline]
        fn contains_any_field(map: &#map_type) -> bool {
            #contains
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

fn local_len_hint_body(model: &HeaderModel, protocol_path: &syn::Path) -> TokenStream {
    let value_trait = quote!(#protocol_path::protocol::header_codec::HeaderValue);
    let adds = model.fields.iter().filter(|field| !field.flattened).map(|field| {
        let ident = &field.ident;
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

fn decode_body(
    model: &HeaderModel,
    codec_trait: &TokenStream,
    error_type: &TokenStream,
    protocol_path: &syn::Path,
) -> TokenStream {
    let scalar_count = model.fields.iter().filter(|field| !field.flattened).count();
    let has_flatten = model.fields.iter().any(|field| field.flattened);
    let scan = match model.lookup {
        LookupPlan::Scan => true,
        LookupPlan::Get => false,
        LookupPlan::Auto => !has_flatten && scalar_count >= 4,
    };

    let scalar_fields: Vec<_> = model.fields.iter().filter(|field| !field.flattened).collect();
    let local_declarations = scalar_fields.iter().map(|field| {
        let local = raw_ident(field);
        quote!(let mut #local = None;)
    });
    let lookup = if scan {
        let arms = scalar_fields
            .iter()
            .flat_map(|field| candidate_arms(field, error_type, codec_trait));
        quote! {
            for (key, value) in map {
                match key.as_str() {
                    #(#arms)*
                    _ => {}
                }
            }
        }
    } else {
        let lookups = scalar_fields
            .iter()
            .flat_map(|field| candidate_lookups(field, error_type, codec_trait));
        quote!(#(#lookups)*)
    };
    let normalize_locals = scalar_fields.iter().map(|field| {
        let local = raw_ident(field);
        quote!(let #local = #local.map(|(value, _precedence)| value);)
    });
    let construct_fields = model
        .fields
        .iter()
        .map(|field| construct_field(field, codec_trait, error_type, protocol_path));

    quote! {
        #(#local_declarations)*
        #lookup
        #(#normalize_locals)*
        let header = Self {
            #(#construct_fields)*
        };
        <Self as #codec_trait>::validate_for_wire(&header)?;
        Ok(header)
    }
}

fn candidate_arms(field: &FieldModel, error_type: &TokenStream, codec_trait: &TokenStream) -> Vec<TokenStream> {
    std::iter::once((&field.key, 0_u16))
        .chain(
            field
                .aliases
                .iter()
                .enumerate()
                .map(|(index, alias)| (alias, (index + 1) as u16)),
        )
        .map(|(name, precedence)| {
            let name = literal(&name.value, name.span);
            let update = candidate_update(field, error_type, codec_trait, precedence);
            quote!(#name => { #update })
        })
        .collect()
}

fn candidate_lookups(field: &FieldModel, error_type: &TokenStream, codec_trait: &TokenStream) -> Vec<TokenStream> {
    std::iter::once((&field.key, 0_u16))
        .chain(
            field
                .aliases
                .iter()
                .enumerate()
                .map(|(index, alias)| (alias, (index + 1) as u16)),
        )
        .map(|(name, precedence)| {
            let name = literal(&name.value, name.span);
            let update = candidate_update(field, error_type, codec_trait, precedence);
            quote! {
                if let Some(value) = map.get(#name) {
                    #update
                }
            }
        })
        .collect()
}

fn candidate_update(
    field: &FieldModel,
    error_type: &TokenStream,
    codec_trait: &TokenStream,
    precedence: u16,
) -> TokenStream {
    let local = raw_ident(field);
    let key = literal(&field.key.value, field.key.span);
    let conflict_header = quote!(<Self as #codec_trait>::TYPE_ID);
    match field.alias_conflict {
        AliasConflict::Error => quote! {
            match #local {
                None => #local = Some((value, #precedence)),
                Some((selected, selected_precedence)) => {
                    if selected.as_str() != value.as_str() {
                        return Err(#error_type::Conflict {
                            header: #conflict_header,
                            key: #key,
                        });
                    }
                    if #precedence < selected_precedence {
                        #local = Some((value, #precedence));
                    }
                }
            }
        },
        AliasConflict::PreferCanonical => quote! {
            match #local {
                None => #local = Some((value, #precedence)),
                Some((_selected, selected_precedence)) if #precedence < selected_precedence => {
                    #local = Some((value, #precedence));
                }
                Some(_) => {}
            }
        },
    }
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
        return if field.option_inner.is_some() {
            match field.flatten_presence.unwrap_or(FlattenPresence::Always) {
                FlattenPresence::Always => quote!(#ident: Some(<#base_type as #codec_trait>::decode_from_map(map)?),),
                FlattenPresence::Any => quote! {
                    #ident: if <#base_type as #codec_trait>::contains_any_field(map) {
                        Some(<#base_type as #codec_trait>::decode_from_map(map)?)
                    } else {
                        None
                    },
                },
            }
        } else {
            quote!(#ident: <#base_type as #codec_trait>::decode_from_map(map)?,)
        };
    }

    let local = raw_ident(field);
    let base_type = field.option_inner.as_ref().unwrap_or(&field.ty);
    let context = context_ident(field);
    let value_trait = quote!(#protocol_path::protocol::header_codec::HeaderValue);
    match field.missing.as_ref().expect("validated scalar missing policy") {
        MissingPolicy::Optional => quote! {
            #ident: #local
                .map(|raw| <#base_type as #value_trait>::decode(raw.as_str(), Self::#context))
                .transpose()?,
        },
        MissingPolicy::Required => {
            let key = literal(&field.key.value, field.key.span);
            quote! {
                #ident: <#base_type as #value_trait>::decode(
                    #local.ok_or(#error_type::Missing {
                        header: <Self as #codec_trait>::TYPE_ID,
                        key: #key,
                    })?.as_str(),
                    Self::#context,
                )?,
            }
        }
        MissingPolicy::Default => quote! {
            #ident: match #local {
                Some(raw) => <#base_type as #value_trait>::decode(raw.as_str(), Self::#context)?,
                None => <#base_type as ::core::default::Default>::default(),
            },
        },
        MissingPolicy::DefaultWith(path) => quote! {
            #ident: match #local {
                Some(raw) => <#base_type as #value_trait>::decode(raw.as_str(), Self::#context)?,
                None => #path(),
            },
        },
    }
}

fn contains_body(model: &HeaderModel, codec_trait: &TokenStream) -> TokenStream {
    let checks = model.fields.iter().map(|field| {
        if field.flattened {
            let base_type = field.option_inner.as_ref().unwrap_or(&field.ty);
            quote!(<#base_type as #codec_trait>::contains_any_field(map))
        } else {
            let names = std::iter::once(&field.key)
                .chain(field.aliases.iter())
                .map(|name| literal(&name.value, name.span));
            quote!(false #(|| map.contains_key(#names))*)
        }
    });
    quote!(false #(|| #checks)*)
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

fn literal(value: &str, span: Span) -> syn::LitStr {
    syn::LitStr::new(value, span)
}
