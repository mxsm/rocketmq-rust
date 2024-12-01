/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use proc_macro::TokenStream;
use quote::ToTokens;
use syn::Field;
use syn::PathArguments;
use syn::Type;
use syn::TypePath;

use crate::remoting_serializable::remoting_serializable_inner;
use crate::request_header_custom::request_header_codec_inner;

mod remoting_serializable;
mod request_header_custom;

#[proc_macro_derive(RequestHeaderCodec, attributes(required))]
pub fn request_header_codec(input: TokenStream) -> TokenStream {
    request_header_codec_inner(input)
}

#[proc_macro_derive(RemotingSerializable)]
pub fn remoting_serializable(input: TokenStream) -> TokenStream {
    remoting_serializable_inner(input)
}

fn get_type_name(ty: &Type) -> String {
    ty.to_token_stream().to_string()
}

fn snake_to_camel_case(input: &str) -> String {
    let mut camel_case = String::new();
    let mut capitalize_next = false;

    for c in input.chars() {
        if c == '_' {
            capitalize_next = true;
        } else if capitalize_next {
            camel_case.push(c.to_ascii_uppercase());
            capitalize_next = false;
        } else {
            camel_case.push(c);
        }
    }

    camel_case
}

fn is_option_type(ty: &Type) -> Option<&Type> {
    match ty {
        Type::Path(path) => {
            if let Some(segment) = path.path.segments.last() {
                if segment.ident == "Option" {
                    if let PathArguments::AngleBracketed(args) = &segment.arguments {
                        if let Some(syn::GenericArgument::Type(inner_ty)) = args.args.first() {
                            return Some(inner_ty);
                        }
                    }
                }
            }
            None
        }
        _ => None,
    }
}

fn is_struct_type(ty: &Type) -> bool {
    // Check if the type is a path (i.e., a named type)
    if let Type::Path(TypePath { path, .. }) = ty {
        // Check if the type is Option<T>
        if let Some(segment) = path.segments.first() {
            // If it's Option, check its generic argument
            if segment.ident == "Option" {
                if let syn::PathArguments::AngleBracketed(ref args) = &segment.arguments {
                    // Extract the first argument (T) inside Option
                    if let Some(syn::GenericArgument::Type(inner_ty)) = args.args.first() {
                        // Recursively check the inner type, if it's not basic or String, return
                        // true
                        return !is_basic_or_string_type(inner_ty);
                    }
                }
            }
        }

        // Check the last segment of the path to determine if it's a basic type or String,
        // CheetahString
        if let Some(_segment) = path.segments.last() {
            // If it's a basic type or String/CheetahString, return false
            if is_basic_or_string_type(ty) {
                return false;
            }

            // If it's neither basic nor String/CheetahString, it's likely a struct
            return true;
        }
    }

    // If none of the conditions match, it's not a struct
    false
}

fn is_basic_or_string_type(ty: &Type) -> bool {
    if let Type::Path(TypePath { path, .. }) = ty {
        if let Some(segment) = path.segments.last() {
            let segment_ident = &segment.ident;
            // check if the type is a basic type or a string or a CheetahString
            return segment_ident == "String"
                || segment_ident == "CheetahString"
                || segment_ident == "i8"
                || segment_ident == "i16"
                || segment_ident == "i32"
                || segment_ident == "i64"
                || segment_ident == "u8"
                || segment_ident == "u16"
                || segment_ident == "u32"
                || segment_ident == "u64"
                || segment_ident == "f32"
                || segment_ident == "f64"
                || segment_ident == "bool";
        }
    }
    false
}

fn has_serde_flatten_attribute(field: &Field) -> bool {
    for attr in &field.attrs {
        if let Some(ident) = attr.path().get_ident() {
            let mut has_serde_flatten_attribute = false;
            if ident == "serde" {
                let _ = attr.parse_nested_meta(|meta| {
                    has_serde_flatten_attribute = meta
                        .path
                        .segments
                        .iter()
                        .any(|segment| segment.ident == "flatten");
                    Ok(())
                });
            }
            return has_serde_flatten_attribute;
        }
    }
    false
}
