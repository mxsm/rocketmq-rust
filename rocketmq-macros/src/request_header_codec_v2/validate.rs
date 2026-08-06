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

use std::collections::HashMap;

use super::model::{combine_error, HeaderModel};

pub(super) fn validate(model: &HeaderModel) -> syn::Result<()> {
    let mut owners: HashMap<&str, &syn::Ident> = HashMap::new();
    let mut errors = None;

    for field in model.fields.iter().filter(|field| !field.flattened) {
        for key in std::iter::once(field.wire_key.as_str()).chain(field.aliases.iter().map(String::as_str)) {
            if let Some(owner) = owners.insert(key, &field.ident) {
                combine_error(
                    &mut errors,
                    syn::Error::new(
                        field.span,
                        format!("wire key `{key}` is already used by field `{owner}`"),
                    ),
                );
            }
        }
    }

    match errors {
        Some(error) => Err(error),
        None => Ok(()),
    }
}
