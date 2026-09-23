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
use quote::quote;
use syn::{GenericArgument, Path, PathArguments, Type};

pub(super) fn option_inner(ty: &Type) -> Option<&Type> {
    let Type::Path(type_path) = ty else {
        return None;
    };
    let segment = type_path.path.segments.last()?;
    if segment.ident != "Option" {
        return None;
    }
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return None;
    };
    if arguments.args.len() != 1 {
        return None;
    }
    match arguments.args.first()? {
        GenericArgument::Type(inner) => Some(inner),
        _ => None,
    }
}

pub(super) fn resolve_protocol_path(span: Span, attribute_help: &str) -> syn::Result<Path> {
    use proc_macro_crate::{crate_name, FoundCrate};

    match crate_name("rocketmq-protocol") {
        Ok(FoundCrate::Itself) => Ok(syn::parse_quote!(crate)),
        Ok(FoundCrate::Name(name)) => {
            let ident = syn::Ident::new(&name.replace('-', "_"), span);
            Ok(syn::parse_quote!(::#ident))
        }
        Err(error) => Err(syn::Error::new(
            span,
            format!("unable to resolve rocketmq-protocol ({error}); use {attribute_help}"),
        )),
    }
}

pub(super) fn command_custom_header_trait(protocol_path: &Path) -> TokenStream {
    quote!(#protocol_path::protocol::command_custom_header::CommandCustomHeader)
}

pub(super) fn from_map_trait(protocol_path: &Path) -> TokenStream {
    quote!(#protocol_path::protocol::command_custom_header::FromMap)
}
