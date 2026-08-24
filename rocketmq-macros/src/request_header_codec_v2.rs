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

pub(crate) mod attr;

use syn::DeriveInput;

pub(super) fn request_header_codec_inner_v2(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    expand(input).unwrap_or_else(syn::Error::into_compile_error).into()
}

fn expand(input: proc_macro::TokenStream) -> syn::Result<proc_macro2::TokenStream> {
    let input = syn::parse::<DeriveInput>(input)?;
    crate::request_header_codec_v3::legacy_v2::expand(input)
}
