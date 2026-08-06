// Copyright 2023 The RocketMQ Rust Authors
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

mod attr;
mod codegen;
mod model;
mod validate;

use syn::DeriveInput;

pub(super) fn request_header_codec_inner_v2(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    expand(input).unwrap_or_else(syn::Error::into_compile_error).into()
}

fn expand(input: proc_macro::TokenStream) -> syn::Result<proc_macro2::TokenStream> {
    let input = syn::parse::<DeriveInput>(input)?;
    let model = model::HeaderModel::parse(input)?;
    validate::validate(&model)?;
    Ok(codegen::generate(model))
}

#[cfg(test)]
mod tests {
    use quote::quote;

    use super::{codegen, model, validate};

    #[test]
    fn expansion_outline_preserves_generics_resolved_paths_and_borrowed_values() {
        let input = syn::parse_quote! {
            #[request_header_codec_v2(crate = "protocol_api")]
            struct Header<T>
            where
                T: Default + ToString + std::str::FromStr + 'static,
            {
                #[serde(rename = "v", alias = "value")]
                field: T,
            }
        };
        let model = model::HeaderModel::parse(input).expect("model");
        validate::validate(&model).expect("validation");
        let compact = codegen::generate(model).to_string().replace(' ', "");

        for expected in [
            quote!(impl<T> protocol_api::protocol::command_custom_header::CommandCustomHeader for Header<T>)
                .to_string(),
            quote!(fn encode_into_map(&self, out: &mut protocol_api::HeaderMap)).to_string(),
            quote!(let __request_header_codec_v2_field: Option<&protocol_api::__request_header_codec::CheetahString>)
                .to_string(),
            quote!(map.get(Self::FIELD).or_else(|| map.get("value"))).to_string(),
        ] {
            assert!(
                compact.contains(&expected.replace(' ', "")),
                "missing expansion outline: {expected}"
            );
        }
        assert!(compact.contains("whereT:Default+ToString+std::str::FromStr+'static"));
    }
}
