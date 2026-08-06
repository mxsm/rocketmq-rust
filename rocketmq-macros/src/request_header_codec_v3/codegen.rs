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
use syn::parse_quote;
use syn::Generics;

use super::codegen_map;
use super::codegen_schema;
use super::codegen_shim;
use super::model::{HeaderModel, LegacyShim, MissingPolicy};

pub(super) fn generate(model: &HeaderModel) -> TokenStream {
    let ident = &model.ident;
    let protocol_path = &model.protocol_path;
    let codec_trait = quote!(#protocol_path::protocol::header_codec::HeaderCodec);
    let mut generics = codec_generics(model, &codec_trait);
    let (_, original_ty_generics, _) = model.generics.split_for_impl();
    generics
        .make_where_clause()
        .predicates
        .push(parse_quote!(#ident #original_ty_generics: 'static));
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let context_declarations = codegen_map::context_declarations(model);
    let manual_fast_helpers = if model.fast && matches!(model.legacy_shim, LegacyShim::Manual) {
        codegen_map::manual_fast_helpers(model, &codec_trait)
    } else {
        TokenStream::new()
    };
    let map_items = codegen_map::codec_items(model, &codec_trait);
    let schema_items = codegen_schema::codec_items(model, &codec_trait);
    let shims = match model.legacy_shim {
        LegacyShim::Generated => codegen_shim::generate(model, &generics, &codec_trait),
        LegacyShim::Manual => TokenStream::new(),
    };

    quote! {
        impl #impl_generics #ident #ty_generics #where_clause {
            #context_declarations
            #manual_fast_helpers
        }

        impl #impl_generics #codec_trait for #ident #ty_generics #where_clause {
            #schema_items
            #map_items
        }

        #shims
    }
}

fn codec_generics(model: &HeaderModel, codec_trait: &TokenStream) -> Generics {
    let protocol_path = &model.protocol_path;
    let value_trait = quote!(#protocol_path::protocol::header_codec::HeaderValue);
    let mut generics = model.generics.clone();
    let where_clause = generics.make_where_clause();

    for field in &model.fields {
        let base_type = field.option_inner.as_ref().unwrap_or(&field.ty);
        if field.flattened {
            where_clause.predicates.push(parse_quote!(#base_type: #codec_trait));
        } else {
            where_clause.predicates.push(parse_quote!(#base_type: #value_trait));
            if matches!(field.missing, Some(MissingPolicy::Default)) {
                where_clause
                    .predicates
                    .push(parse_quote!(#base_type: ::core::default::Default));
            }
        }
    }
    generics
}
