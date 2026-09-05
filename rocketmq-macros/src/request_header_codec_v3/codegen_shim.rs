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
use syn::Generics;

use super::canonical::{command_custom_header_trait, from_map_trait};
use super::model::HeaderModel;

pub(super) fn generate(model: &HeaderModel, generics: &Generics, codec_trait: &TokenStream) -> TokenStream {
    let ident = &model.ident;
    let protocol_path = &model.protocol_path;
    let command_trait = command_custom_header_trait(protocol_path);
    let from_map_trait = from_map_trait(protocol_path);
    let map_type = quote!(#protocol_path::HeaderMap);
    let field_source = quote!(#protocol_path::protocol::header_codec::HeaderFieldSource);
    let codec_error = quote!(#protocol_path::ProtocolContractViolation);
    let into_rocketmq_error = quote!(#protocol_path::protocol::header_codec::into_rocketmq_error);
    let rocketmq_error = quote!(#protocol_path::__request_header_codec::RocketMQError);
    let map_sink = quote!(#protocol_path::protocol::header_codec::MapSink);
    let fast_methods = if model.fast {
        let binary_sink = quote!(#protocol_path::protocol::header_codec::BinarySink);
        let json_sink = quote!(#protocol_path::protocol::header_codec::JsonSink);
        let encode_capability = quote!(#protocol_path::protocol::command_custom_header::HeaderEncodeCapability);
        let bytes_mut = quote!(#protocol_path::__request_header_codec::BytesMut);
        quote! {
            fn encode_capability(&self) -> #encode_capability {
                #encode_capability::DirectBinary
            }

            fn encode_direct_binary(&self, out: &mut #bytes_mut) -> Result<(), #codec_error> {
                let checkpoint = out.len();
                let result = {
                    let mut sink = #binary_sink::new(out);
                    <Self as #codec_trait>::encode_into(self, &mut sink)
                };
                if result.is_err() {
                    out.truncate(checkpoint);
                }
                result
            }

            fn supports_direct_json_fields(&self) -> bool {
                true
            }

            fn encode_direct_json_fields(&self, out: &mut #bytes_mut) -> Result<(), #codec_error> {
                let checkpoint = out.len();
                let result = {
                    let mut sink = #json_sink::new(out);
                    let result = <Self as #codec_trait>::encode_into(self, &mut sink);
                    if result.is_ok() {
                        sink.finish();
                    }
                    result
                };
                if result.is_err() {
                    out.truncate(checkpoint);
                }
                result
            }
        }
    } else {
        TokenStream::new()
    };
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    quote! {
        impl #impl_generics #command_trait for #ident #ty_generics #where_clause {
            fn check_fields(&self) -> Result<(), #rocketmq_error> {
                <Self as #codec_trait>::validate_for_wire(self)
                    .map_err(#into_rocketmq_error)
            }

            fn to_map(&self) -> Option<#map_type> {
                let mut map = #map_type::with_capacity(<Self as #codec_trait>::FIELD_COUNT_HINT);
                <Self as #command_trait>::try_encode_into_map(self, &mut map).ok()?;
                Some(map)
            }

            fn encode_into_map(&self, out: &mut #map_type) {
                let _ = <Self as #command_trait>::try_encode_into_map(self, out);
            }

            fn try_encode_into_map(&self, out: &mut #map_type) -> Result<(), #codec_error> {
                out.reserve(<Self as #codec_trait>::FIELD_COUNT_HINT);
                let mut sink = #map_sink::new(out);
                <Self as #codec_trait>::encode_into(self, &mut sink)
            }

            fn resolve_wire_key(
                &self,
                key: &str,
            ) -> Option<#protocol_path::protocol::header_codec::ResolvedHeaderKey> {
                <Self as #codec_trait>::resolve_wire_key(key)
            }

            fn canonical_wire_key(&self, key: &str) -> Option<&'static str> {
                <Self as #codec_trait>::canonical_wire_key(key)
            }

            fn contains_wire_key(&self, key: &str) -> bool {
                <Self as #codec_trait>::contains_wire_key(key)
            }

            fn encoded_len_hint(&self) -> usize {
                <Self as #codec_trait>::encoded_len_hint(self)
            }

            #fast_methods
        }

        impl #impl_generics #from_map_trait for #ident #ty_generics #where_clause {
            type Error = #rocketmq_error;
            type Target = Self;
            const SUPPORTS_HEADER_FIELD_SOURCE: bool = true;

            fn from(map: &#map_type) -> Result<Self::Target, Self::Error> {
                <Self as #codec_trait>::decode_from_map(map)
                    .map_err(#into_rocketmq_error)
            }

            fn from_field_source(source: &dyn #field_source) -> Result<Self::Target, Self::Error> {
                <Self as #codec_trait>::decode_from_source(source)
                    .map_err(#into_rocketmq_error)
            }
        }
    }
}
