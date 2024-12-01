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
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::parse_macro_input;
use syn::Data;
use syn::DeriveInput;
use syn::Fields;
use syn::Ident;

use crate::get_type_name;
use crate::is_option_type;
use crate::snake_to_camel_case;

pub(super) fn request_header_codec_inner(
    input: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let struct_name = &input.ident;
    //For Java code, all class attributes have names,
    //so all struct attributes of RequestHeader that are compatible with Java have names.
    let fields = match input.data {
        Data::Struct(value) => match value.fields {
            Fields::Named(f) => f.named,
            _ => return quote! {}.into(),
        },
        _ => return quote! {}.into(),
    };

    //build CommandCustomHeader impl
    let (static_fields, (to_maps, from_map)): (Vec<_>, (Vec<_>, Vec<_>)) = fields
        .iter()
        .map(|field| {
            let field_name = field.ident.as_ref().unwrap();
            let mut required = false;

            for attr in &field.attrs {
                if let Some(ident) = attr.path().get_ident() {
                    if ident == "required" {
                        required = true;
                    }
                }
            }

            //Determining whether it is an Option type or a direct data type
            //This will lead to different ways of processing in the future.
            let has_option = is_option_type(&field.ty);
            let camel_case_name = snake_to_camel_case(&format!("{}", field_name));
            let static_name = Ident::new(
                &format!("{}", field_name).to_ascii_uppercase(),
                field_name.span(),
            );
            let type_name = if let Some(value) = &has_option {
                get_type_name(*value)
            } else {
                get_type_name(&field.ty)
            };
            (
                quote! {
                      const #static_name: &'static str = #camel_case_name;
                  },
                (
                    if let Some(_) = has_option {
                        if type_name == "CheetahString" {
                            quote! {
                                  if let Some(ref value) = self.#field_name {
                                     map.insert (
                                          cheetah_string::CheetahString::from_static_str(Self::#static_name),
                                          value.clone()
                                     );
                                   }
                              }
                        } else if type_name == "String" {
                            quote! {
                                  if let Some(ref value) = self.#field_name {
                                     map.insert (
                                          cheetah_string::CheetahString::from_static_str(Self::#static_name),
                                          cheetah_string::CheetahString::from_string(value.clone())
                                     );
                                   }
                              }
                        } else {
                            quote! {
                                  if let Some(ref value) = self.#field_name {
                                     map.insert (
                                         cheetah_string::CheetahString::from_static_str(Self::#static_name),
                                         cheetah_string::CheetahString::from_string(value.to_string())
                                     );
                                   }
                              }
                        }
                    } else {
                        if type_name == "CheetahString" {
                            quote! {
                                     map.insert (
                                          cheetah_string::CheetahString::from_static_str(Self::#static_name),
                                          self.#field_name.clone()
                                     );
                              }
                        } else if type_name == "String" {
                            quote! {
                                     map.insert (
                                          cheetah_string::CheetahString::from_static_str(Self::#static_name),
                                          cheetah_string::CheetahString::from_string(self.#field_name.clone())
                                     );
                              }
                        } else {
                            quote! {
                                  map.insert (
                                     cheetah_string::CheetahString::from_static_str(Self::#static_name),
                                     cheetah_string::CheetahString::from_string(self.#field_name.to_string())
                                 );
                              }
                        }
                    },
                    // build FromMap impl
                    if let Some(value) = has_option {
                        if type_name == "CheetahString" || type_name == "String" {
                            if required {
                                quote! {
                                  #field_name: Some(
                                         map.get(&cheetah_string::CheetahString::from_static_str(Self::#static_name))
                                         .cloned()
                                         .ok_or(Self::Error::RemotingCommandError(
                                            format!("Missing {} field", Self::#static_name),
                                         ))?
                                     ),
                                }
                            } else {
                                quote! {
                                  #field_name: map.get(&cheetah_string::CheetahString::from_static_str(Self::#static_name)).cloned(),
                                 }
                            }
                        } else if required {
                            quote! {
                                  #field_name: Some(
                                         map.get(&cheetah_string::CheetahString::from_static_str(Self::#static_name)).ok_or(Self::Error::RemotingCommandError(
                                            format!("Missing {} field", Self::#static_name),
                                        ))?
                                        .parse::<#value>()
                                        .map_err(|_| Self::Error::RemotingCommandError(format!("Parse {} field error", Self::#static_name)))?
                                     ),
                              }
                        } else {
                            quote! {
                                  #field_name:map.get(&cheetah_string::CheetahString::from_static_str(Self::#static_name)).and_then(|s| s.parse::<#value>().ok()),
                                 }
                        }
                    } else {
                        let types = &field.ty;
                        if type_name == "CheetahString" || type_name == "String" {
                            if required {
                                quote! {
                                  #field_name: map.get(&cheetah_string::CheetahString::from_static_str(Self::#static_name))
                                         .cloned()
                                         .ok_or(Self::Error::RemotingCommandError(
                                            format!("Missing {} field", Self::#static_name),
                                         ))?,
                              }
                            } else {
                                quote! {
                                  #field_name: map.get(&cheetah_string::CheetahString::from_static_str(Self::#static_name)).cloned().unwrap_or_default(),
                                }
                            }
                        } else if required {
                                quote! {
                                    #field_name:map.get(&cheetah_string::CheetahString::from_static_str(Self::#static_name)).ok_or(Self::Error::RemotingCommandError(
                                        format!("Missing {} field", Self::#static_name),
                                    ))?
                                    .parse::<#types>()
                                    .map_err(|_| Self::Error::RemotingCommandError(format!("Parse {} field error", Self::#static_name)))?,
                                  }
                            } else {
                                quote! {
                                    #field_name:map.get(&cheetah_string::CheetahString::from_static_str(Self::#static_name)).and_then(|s| s.parse::<#types>().ok()).unwrap_or_default(),
                                  }
                            }

                    }
                )
            )
        })
        .unzip();
    let expanded: TokenStream2 = quote! {
        impl #struct_name {
            #(#static_fields)*
        }

        impl crate::protocol::command_custom_header::CommandCustomHeader for #struct_name {
            fn to_map(&self) -> Option<std::collections::HashMap<cheetah_string::CheetahString, cheetah_string::CheetahString>> {
                let mut map = std::collections::HashMap::new();
                #(#to_maps)*
                Some(map)
            }
        }

        impl crate::protocol::command_custom_header::FromMap for #struct_name {

            type Error = crate::remoting_error::RemotingError;

            type Target = Self;

            fn from(map: &std::collections::HashMap<cheetah_string::CheetahString, cheetah_string::CheetahString>) -> Result<Self::Target, Self::Error> {
                Ok(#struct_name {
                    #(#from_map)*
                })
            }
        }
    };

    TokenStream::from(expanded)
}
