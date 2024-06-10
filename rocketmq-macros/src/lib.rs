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
use syn::PathArguments;
use syn::Type;

use crate::remoting_serializable::remoting_serializable_inner;
use crate::request_header_custom::request_header_codec_inner;

mod remoting_serializable;
mod request_header_custom;

#[proc_macro_derive(RequestHeaderCodec)]
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
