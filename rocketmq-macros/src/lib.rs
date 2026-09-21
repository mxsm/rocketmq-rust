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

use crate::remoting_serializable::remoting_serializable_inner;
use crate::request_header_codec_v3::request_header_codec_inner_v3;

mod remoting_serializable;
mod request_header_codec_v3;

/// Recommended request-header derive. Generates typed map codecs, schema
/// metadata, and compatibility adapters for an explicit wire model.
#[proc_macro_derive(RequestHeaderCodecV3, attributes(header, required))]
pub fn request_header_codec_v3(input: TokenStream) -> TokenStream {
    request_header_codec_inner_v3(input)
}

#[proc_macro_derive(RemotingSerializable)]
pub fn remoting_serializable(input: TokenStream) -> TokenStream {
    remoting_serializable_inner(input)
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
