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

mod attr;
mod canonical;
mod codegen;
mod codegen_map;
mod codegen_schema;
mod codegen_shim;
pub(crate) mod legacy_v1;
pub(crate) mod legacy_v2;
mod model;
mod validate;

use syn::DeriveInput;

pub(super) fn request_header_codec_inner_v3(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    expand(input).unwrap_or_else(syn::Error::into_compile_error).into()
}

fn expand(input: proc_macro::TokenStream) -> syn::Result<proc_macro2::TokenStream> {
    let input = syn::parse::<DeriveInput>(input)?;
    let input_span = input.ident.span();
    let (model, mut errors) = model::HeaderModel::parse_partial(input);
    let Some(model) = model else {
        return Err(errors.unwrap_or_else(|| {
            syn::Error::new(input_span, "RequestHeaderCodecV3 could not construct a header model")
        }));
    };
    if let Err(error) = validate::validate(&model) {
        combine_error(&mut errors, error);
    }
    if let Some(error) = errors {
        return Err(error);
    }

    let implementation = codegen::generate(&model);
    let diagnostics = migration_diagnostics(&model);
    Ok(quote::quote!(#implementation #diagnostics))
}

fn migration_diagnostics(model: &model::HeaderModel) -> proc_macro2::TokenStream {
    use quote::{quote, quote_spanned};

    let warnings = model.fields.iter().filter(|field| field.legacy_required).map(|field| {
        let span = field.span;
        quote_spanned! {span=>
            const _: () = {
                #[deprecated(note = "legacy #[required] is accepted temporarily; use #[header(required)]")]
                const __REQUEST_HEADER_CODEC_V3_LEGACY_REQUIRED: () = ();
                let _ = __REQUEST_HEADER_CODEC_V3_LEGACY_REQUIRED;
            };
        }
    });
    quote!(#(#warnings)*)
}

pub(super) fn combine_error(errors: &mut Option<syn::Error>, error: syn::Error) {
    if let Some(errors) = errors {
        errors.combine(error);
    } else {
        *errors = Some(error);
    }
}

#[cfg(test)]
mod tests {
    use quote::{quote, ToTokens};

    use super::migration_diagnostics;
    use super::model::{AliasConflict, FlattenPresence, HeaderModel, LegacyShim, LookupPlan, MissingPolicy, ValueKind};
    use super::validate;

    #[test]
    fn accepts_complete_container_and_field_models() {
        let input = syn::parse_quote! {
            #[header(
                type_id = "fixtures::GenericHeader",
                java_class = "org.apache.rocketmq.fixtures.GenericHeader",
                validate = "Self::validate_header",
                lookup = "get",
                legacy_shim = "manual",
                crate = "protocol_api",
                fast
            )]
            struct GenericHeader<T> {
                #[header(key = "value", alias = "legacyValue", alias_conflict = "error", required, binary_order = 1)]
                value: T,
                #[header(key = "offset", required, java_type = "long", range = "i64", binary_order = 2)]
                offset: u64,
                #[header(flatten, presence = "any", binary_order = 3)]
                nested: Option<Nested<T>>,
            }
        };

        let model = HeaderModel::parse(input).expect("model");
        validate::validate(&model).expect("validation");

        assert_eq!(model.lookup, LookupPlan::Get);
        assert_eq!(model.legacy_shim, LegacyShim::Manual);
        assert!(model.fast);
        assert_eq!(
            model.protocol_path.to_token_stream().to_string(),
            quote!(protocol_api).to_string()
        );
        assert_eq!(model.fields[0].kind, ValueKind::Generic);
        assert!(matches!(model.fields[0].missing, Some(MissingPolicy::Required)));
        assert_eq!(model.fields[0].alias_conflict, AliasConflict::Error);
        assert_eq!(model.fields[2].flatten_presence, Some(FlattenPresence::Any));
    }

    #[test]
    fn serde_metadata_does_not_change_v3_wire_metadata() {
        let input = syn::parse_quote! {
            #[header(type_id = "fixtures::IndependentMetadata", crate = "protocol_api")]
            #[serde(rename_all = "kebab-case")]
            struct IndependentMetadata {
                #[serde(rename = "serde-name", alias = "serde-alias", default)]
                #[header(required)]
                request_id: String,
            }
        };

        let model = HeaderModel::parse(input).expect("model");
        validate::validate(&model).expect("validation");

        assert_eq!(model.fields[0].key.value, "requestId");
        assert!(model.fields[0].aliases.is_empty());
    }

    #[test]
    fn combines_errors_from_multiple_fields() {
        let input = syn::parse_quote! {
            #[header(type_id = "fixtures::Combined", crate = "protocol_api")]
            struct Combined {
                #[header(required)]
                first: Option<String>,
                second: bool,
            }
        };

        let error = match HeaderModel::parse(input) {
            Ok(_) => panic!("invalid fields must fail"),
            Err(error) => error,
        };
        let rendered = error.into_compile_error().to_string();

        assert!(rendered.contains("required cannot be used on Option<T>"));
        assert!(rendered.contains("non-Option fields require required, default, or default_with"));
    }

    #[test]
    fn legacy_required_emits_a_locatable_migration_diagnostic() {
        let input = syn::parse_quote! {
            #[header(type_id = "fixtures::LegacyRequired", crate = "protocol_api")]
            struct LegacyRequired {
                #[required]
                value: String,
            }
        };

        let model = HeaderModel::parse(input).expect("model");
        validate::validate(&model).expect("validation");
        let diagnostics = migration_diagnostics(&model).to_string();

        assert!(diagnostics.contains("deprecated"));
        assert!(diagnostics.contains("legacy #[required]"));
    }
}
