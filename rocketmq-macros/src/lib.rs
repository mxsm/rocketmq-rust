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

use proc_macro::TokenStream;
use quote::ToTokens;
use syn::PathArguments;
use syn::Type;
use syn::TypePath;

use crate::remoting_serializable::remoting_serializable_inner;
use crate::request_header_custom::request_header_codec_inner;
use crate::request_header_custom::request_header_codec_inner_v2;

mod remoting_serializable;
mod request_header_custom;

#[proc_macro_derive(RequestHeaderCodec, attributes(required))]
pub fn request_header_codec(input: TokenStream) -> TokenStream {
    request_header_codec_inner(input)
}

#[proc_macro_derive(RequestHeaderCodecV2, attributes(required))]
pub fn request_header_codec_v2(input: TokenStream) -> TokenStream {
    request_header_codec_inner_v2(input)
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

/// Determines if a given type is an `Option<T>` and returns the inner type `T`.
///
/// This function checks the provided `syn::Type` to see if it represents an `Option<T>`.
/// If the type is `Option<T>`, it returns a reference to the inner type `T`.
/// If the type is not `Option<T>`, it returns `None`.
///
/// # Arguments
///
/// * `ty` - A reference to the `syn::Type` to check.
///
/// # Returns
///
/// * `Some(&syn::Type)` if the type is `Option<T>`, containing a reference to the inner type `T`.
/// * `None` if the type is not `Option<T>`.
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

/// Determines if a given type is a struct, excluding basic types, `String`, `CheetahString`, and
/// `Option<T>` where `T` is a basic type or string.
///
/// This function checks the provided `syn::Type` to see if it represents a struct. It specifically
/// excludes:
/// - Basic data types (e.g., `i8`, `i16`, `i32`, `i64`, `u8`, `u16`, `u32`, `u64`, `f32`, `f64`,
///   `bool`)
/// - Standard Rust `String`
/// - Custom `CheetahString` type
/// - `Option<T>` where `T` is a basic type or string
///
/// # Arguments
///
/// * `ty` - A reference to the `syn::Type` to check.
///
/// # Returns
///
/// * `true` if the type is a struct, otherwise `false`.
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

/// Determines if a given type is a basic data type, a standard `String`, or a custom
/// `CheetahString`.
///
/// This function checks the provided `Type` to see if it matches one of the following:
/// - A basic numeric type (e.g., `i8`, `i16`, `i32`, `i64`, `u8`, `u16`, `u32`, `u64`, `f32`,
///   `f64`)
/// - A boolean type (`bool`)
/// - A standard Rust `String`
/// - A custom `CheetahString` type
///
/// # Arguments
///
/// * `ty` - A reference to the `syn::Type` to check.
///
/// # Returns
///
/// * `true` if the type is a basic data type, `String`, or `CheetahString`, otherwise `false`.
fn is_basic_or_string_type(ty: &syn::Type) -> bool {
    // Check if the type is a path (e.g., a named type)
    if let syn::Type::Path(syn::TypePath { path, .. }) = ty {
        // Get the last segment of the path, which should be the type name
        if let Some(segment) = path.segments.last() {
            // Extract the identifier of the last segment
            let segment_ident = &segment.ident;

            // Check if the identifier matches any of the basic types, `String`, or `CheetahString`
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

    // If the type is not a path or does not match any of the expected types, return false
    false
}

/// Checks if a given field has the `serde(flatten)` attribute.
///
/// This function iterates over all attributes of the provided field and checks
/// for the presence of the `serde` attribute. If the `serde` attribute is found,
/// it then looks inside to see if the `flatten` argument is present.
///
/// # Arguments
///
/// * `field` - A reference to the `syn::Field` to check for the `serde(flatten)` attribute.
///
/// # Returns
///
/// * `true` if the `serde(flatten)` attribute is found, otherwise `false`.
fn has_serde_flatten_attribute(field: &syn::Field) -> bool {
    // Iterate over all attributes of the field
    for attr in &field.attrs {
        // Check if the attribute is named "serde"
        if let Some(ident) = attr.path().get_ident() {
            if ident == "serde" {
                // Initialize a flag to determine if `flatten` is found
                let mut has_serde_flatten_attribute = false;

                // Parse the nested metadata of the `serde` attribute
                let _ = attr.parse_nested_meta(|meta| {
                    // Check if any segment within the `serde` path is named "flatten"
                    has_serde_flatten_attribute = meta.path.segments.iter().any(|segment| segment.ident == "flatten");
                    Ok(())
                });

                // Return the result immediately if `flatten` is found
                return has_serde_flatten_attribute;
            }
        }
    }

    // Return `false` if no `serde(flatten)` attribute was found
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snake_to_camel_case_converts_snake_case_to_camel_case() {
        assert_eq!(snake_to_camel_case("hello_world"), "helloWorld");
    }

    #[test]
    fn snake_to_camel_case_handles_empty_string() {
        assert_eq!(snake_to_camel_case(""), "");
    }

    #[test]
    fn snake_to_camel_case_handles_single_word() {
        assert_eq!(snake_to_camel_case("hello"), "hello");
    }

    #[test]
    fn snake_to_camel_case_handles_multiple_underscores() {
        assert_eq!(snake_to_camel_case("hello__world"), "helloWorld");
    }

    #[test]
    fn snake_to_camel_case_handles_trailing_underscore() {
        assert_eq!(snake_to_camel_case("hello_world_"), "helloWorld");
    }

    #[test]
    fn snake_to_camel_case_handles_leading_underscore() {
        assert_eq!(snake_to_camel_case("_hello_world"), "HelloWorld");
    }

    #[test]
    fn snake_to_camel_case_handles_consecutive_underscores() {
        assert_eq!(snake_to_camel_case("hello___world"), "helloWorld");
    }

    #[test]
    fn snake_to_camel_case_handles_all_underscores() {
        assert_eq!(snake_to_camel_case("___"), "");
    }
}
