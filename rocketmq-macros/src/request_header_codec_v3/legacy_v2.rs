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

mod adapter;
pub(super) mod codegen;
pub(super) mod validate;

use syn::DeriveInput;

pub(crate) fn expand(input: DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let model = adapter::adapt(input)?;
    super::validate::validate(&model)?;
    Ok(super::codegen::generate(&model))
}

#[cfg(test)]
mod tests {
    use quote::quote;

    use super::expand;

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
        let compact = expand(input).expect("model").to_string().replace(' ', "");

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
