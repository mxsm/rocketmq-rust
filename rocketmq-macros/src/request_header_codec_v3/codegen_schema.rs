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

use proc_macro2::TokenStream;
use quote::quote;

use super::model::{AliasConflict, FieldModel, FlattenPresence, HeaderModel, HeaderRange, MissingPolicy};

pub(super) fn codec_items(model: &HeaderModel, codec_trait: &TokenStream) -> TokenStream {
    let protocol_path = &model.protocol_path;
    let ident = &model.ident;
    let type_id = &model.type_id;
    let java_class = model
        .java_class
        .as_ref()
        .map_or_else(|| quote!(None), |value| quote!(Some(#value)));
    let fast = model.fast;
    let scalar_count = model.fields.iter().filter(|field| !field.flattened).count();
    let nested_counts = model.fields.iter().filter(|field| field.flattened).map(|field| {
        let base_type = field.option_inner.as_ref().unwrap_or(&field.ty);
        quote!(<#base_type as #codec_trait>::FIELD_COUNT_HINT)
    });

    let mut scalar_fields: Vec<_> = model.fields.iter().filter(|field| !field.flattened).collect();
    scalar_fields.sort_by_key(|field| field.stable_order());
    let field_specs = scalar_fields.iter().map(|field| field_spec(field, model));

    let mut flatten_fields: Vec<_> = model.fields.iter().filter(|field| field.flattened).collect();
    flatten_fields.sort_by_key(|field| field.stable_order());
    let flatten_specs = flatten_fields
        .iter()
        .map(|field| flatten_spec(field, model, codec_trait));

    let visit_fields = flatten_fields.iter().map(|field| {
        let base_type = field.option_inner.as_ref().unwrap_or(&field.ty);
        quote!(<#base_type as #codec_trait>::visit_field_specs(visitor);)
    });
    let visit_flattens = flatten_fields.iter().enumerate().map(|(index, field)| {
        let base_type = field.option_inner.as_ref().unwrap_or(&field.ty);
        quote! {
            visitor(&Self::LOCAL_FLATTEN_SPECS[#index]);
            <#base_type as #codec_trait>::visit_flatten_specs(visitor);
        }
    });
    let resolver_arms = scalar_fields
        .iter()
        .flat_map(|field| resolver_arms(field, model, protocol_path, codec_trait));
    let nested_resolvers = flatten_fields.iter().map(|field| {
        let base_type = field.option_inner.as_ref().unwrap_or(&field.ty);
        quote! {
            if let Some(resolved) = <#base_type as #codec_trait>::resolve_wire_key(key) {
                return Some(resolved);
            }
        }
    });

    quote! {
        const TYPE_ID: &'static str = #type_id;
        const HEADER_NAME: &'static str = stringify!(#ident);
        const JAVA_CLASS: Option<&'static str> = #java_class;
        const FIELD_COUNT_HINT: usize = #scalar_count #(.saturating_add(#nested_counts))*;
        const LOCAL_FIELD_SPECS: &'static [#protocol_path::protocol::header_codec::HeaderFieldSpec] = &[
            #(#field_specs),*
        ];
        const LOCAL_FLATTEN_SPECS: &'static [#protocol_path::protocol::header_codec::HeaderFlattenSpec] = &[
            #(#flatten_specs),*
        ];
        const FAST_ENABLED: bool = #fast;

        fn visit_field_specs(
            visitor: &mut dyn FnMut(&#protocol_path::protocol::header_codec::HeaderFieldSpec),
        ) {
            for field in Self::LOCAL_FIELD_SPECS {
                visitor(field);
            }
            #(#visit_fields)*
        }

        fn visit_flatten_specs(
            visitor: &mut dyn FnMut(&#protocol_path::protocol::header_codec::HeaderFlattenSpec),
        ) {
            #(#visit_flattens)*
        }

        #[inline]
        fn resolve_wire_key(
            key: &str,
        ) -> Option<#protocol_path::protocol::header_codec::ResolvedHeaderKey> {
            match key {
                #(#resolver_arms)*
                _ => {}
            }
            #(#nested_resolvers)*
            None
        }
    }
}

fn field_spec(field: &FieldModel, model: &HeaderModel) -> TokenStream {
    let protocol_path = &model.protocol_path;
    let rust_field = syn::LitStr::new(&field.ident.to_string(), field.span);
    let key = syn::LitStr::new(&field.key.value, field.key.span);
    let aliases = field
        .aliases
        .iter()
        .map(|alias| syn::LitStr::new(&alias.value, alias.span));
    let conflict = conflict_tokens(field, protocol_path);
    let base_type = field.option_inner.as_ref().unwrap_or(&field.ty);
    let presence = presence_tokens(field, protocol_path);
    let default_semantic = field.default_semantic.as_ref().map_or_else(
        || quote!(None),
        |value| {
            let value = syn::LitStr::new(&value.value, value.span);
            quote!(Some(#value))
        },
    );
    let java_type = field.java_type.as_ref().map_or_else(
        || quote!(None),
        |value| {
            let value = syn::LitStr::new(&value.value, value.span);
            quote!(Some(#value))
        },
    );
    let java_range = range_tokens(field, protocol_path);
    let order = field.stable_order();
    let type_id = &model.type_id;

    quote! {
        #protocol_path::protocol::header_codec::HeaderFieldSpec {
            rust_field: #rust_field,
            key: #key,
            aliases: &[#(#aliases),*],
            alias_conflict: #conflict,
            kind: <#base_type as #protocol_path::protocol::header_codec::HeaderValue>::KIND,
            presence: #presence,
            default_semantic: #default_semantic,
            java_type: #java_type,
            java_range: #java_range,
            binary_order: #order,
            declared_in: #type_id,
        }
    }
}

fn flatten_spec(field: &FieldModel, model: &HeaderModel, codec_trait: &TokenStream) -> TokenStream {
    let protocol_path = &model.protocol_path;
    let rust_field = syn::LitStr::new(&field.ident.to_string(), field.span);
    let base_type = field.option_inner.as_ref().unwrap_or(&field.ty);
    let presence = match field.flatten_presence.unwrap_or(FlattenPresence::Always) {
        FlattenPresence::Always => quote!(#protocol_path::protocol::header_codec::FlattenPresenceSpec::Always),
        FlattenPresence::Any => quote!(#protocol_path::protocol::header_codec::FlattenPresenceSpec::Any),
    };
    let order = field.stable_order();
    let type_id = &model.type_id;
    quote! {
        #protocol_path::protocol::header_codec::HeaderFlattenSpec {
            rust_field: #rust_field,
            nested_type_id: <#base_type as #codec_trait>::TYPE_ID,
            presence: #presence,
            binary_order: #order,
            declared_in: #type_id,
        }
    }
}

fn resolver_arms(
    field: &FieldModel,
    model: &HeaderModel,
    protocol_path: &syn::Path,
    codec_trait: &TokenStream,
) -> Vec<TokenStream> {
    let canonical = syn::LitStr::new(&field.key.value, field.key.span);
    let conflict = conflict_tokens(field, protocol_path);
    std::iter::once((&field.key, 0_u16))
        .chain(field.aliases.iter().enumerate().map(|(index, alias)| (alias, (index + 1) as u16)))
        .map(|(name, precedence)| {
            let name = syn::LitStr::new(&name.value, name.span);
            let type_id = &model.type_id;
            quote! {
                #name => return Some(#protocol_path::protocol::header_codec::ResolvedHeaderKey {
                    header: <Self as #codec_trait>::HEADER_NAME,
                    owner_type_id: #type_id,
                    canonical: #canonical,
                    precedence: #precedence,
                    alias_conflict: #conflict,
                    dynamic_collision: #protocol_path::protocol::header_codec::DynamicCollisionPolicy::ErrorOnDifferentValue,
                }),
            }
        })
        .collect()
}

fn conflict_tokens(field: &FieldModel, protocol_path: &syn::Path) -> TokenStream {
    match field.alias_conflict {
        AliasConflict::Error => quote!(#protocol_path::protocol::header_codec::AliasConflictPolicy::Error),
        AliasConflict::PreferCanonical => {
            quote!(#protocol_path::protocol::header_codec::AliasConflictPolicy::PreferCanonical)
        }
    }
}

fn presence_tokens(field: &FieldModel, protocol_path: &syn::Path) -> TokenStream {
    match field.missing.as_ref().expect("validated scalar missing policy") {
        MissingPolicy::Optional => quote!(#protocol_path::protocol::header_codec::HeaderPresence::Optional),
        MissingPolicy::Required => quote!(#protocol_path::protocol::header_codec::HeaderPresence::Required),
        MissingPolicy::Default => quote!(#protocol_path::protocol::header_codec::HeaderPresence::Default),
        MissingPolicy::DefaultWith(path) => {
            let path = syn::LitStr::new(&quote!(#path).to_string(), field.span);
            quote!(#protocol_path::protocol::header_codec::HeaderPresence::DefaultWith(#path))
        }
    }
}

fn range_tokens(field: &FieldModel, protocol_path: &syn::Path) -> TokenStream {
    match field.range {
        None => quote!(None),
        Some(HeaderRange::I32) => quote!(Some(#protocol_path::protocol::header_codec::HeaderRange::I32)),
        Some(HeaderRange::I64) => quote!(Some(#protocol_path::protocol::header_codec::HeaderRange::I64)),
    }
}
