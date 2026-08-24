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

use proc_macro2::TokenStream;
use syn::DeriveInput;

pub(crate) fn expand(input: DeriveInput) -> TokenStream {
    let Some(model) = adapter::adapt(input) else {
        return TokenStream::new();
    };
    match super::validate::validate(&model) {
        Ok(()) => super::codegen::generate(&model),
        Err(error) => error.into_compile_error(),
    }
}
