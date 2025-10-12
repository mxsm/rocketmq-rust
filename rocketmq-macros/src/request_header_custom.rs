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
use proc_macro2::Span;
use proc_macro2::TokenStream as TokenStream2;
use quote::format_ident;
use quote::quote;
use syn::parse_macro_input;
use syn::Data;
use syn::DeriveInput;
use syn::Fields;
use syn::Ident;

use crate::get_type_name;
use crate::has_serde_flatten_attribute;
use crate::is_option_type;
use crate::is_struct_type;
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

    let (static_fields, (to_maps, from_map)): (Vec<_>, (Vec<_>, Vec<_>)) = fields
        .iter()
        .map(|field| {
            let field_name = field.ident.as_ref().unwrap();
            let mut required = false;
            let is_struct_type = is_struct_type(&field.ty);
            let has_serde_flatten_attribute = has_serde_flatten_attribute(field);
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
            let camel_case_name = snake_to_camel_case(&format!("{field_name}"));
            let static_name = Ident::new(
                &format!("{field_name}").to_ascii_uppercase(),
                field_name.span(),
            );
            let type_name = if let Some(value) = &has_option {
                get_type_name(value)
            } else {
                get_type_name(&field.ty)
            };
            (
                //build static field
                quote! {
                       const #static_name: &'static str = #camel_case_name;
                 },
                (
                    //build CommandCustomHeader impl
                    if has_option.is_some() {
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
                        } else if is_struct_type && has_serde_flatten_attribute {
                            quote! {
                                   if let Some(ref value) = self.#field_name {
                                         if let Some(value) = value.to_map() {
                                             map.extend(value);
                                         }
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
                    } else if type_name == "CheetahString" {
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
                    } else if is_struct_type && has_serde_flatten_attribute {
                        //If it is a struct type and has the serde(flatten) attribute,
                        // it will be processed as a struct.
                        quote! {
                             if let Some(value) = self.#field_name.to_map() {
                                 map.extend(value);
                             }
                         }
                    } else {
                        quote! {
                             map.insert (
                                 cheetah_string::CheetahString::from_static_str(Self::#static_name),
                                 cheetah_string::CheetahString::from_string(self.#field_name.to_string())
                             );
                         }
                    },

                    // build FromMap impl
                    if let Some(value) = has_option {
                        if type_name == "CheetahString" || type_name == "String" {
                            if required {
                                if type_name == "CheetahString" {
                                    quote! {
                                       #field_name: Some(
                                              map.get(&cheetah_string::CheetahString::from_static_str(Self::#static_name))
                                              .cloned()
                                              .ok_or(rocketmq_error::RocketmqError::DeserializeHeaderError(
                                                 format!("Missing {} field", Self::#static_name),
                                              ))?
                                          ),
                                     }
                                } else {
                                    quote! {
                                       Some(
                                              map.get(&cheetah_string::CheetahString::from_static_str(Self::#static_name))
                                              .cloned()
                                              .ok_or(rocketmq_error::RocketmqError::DeserializeHeaderError(
                                                 format!("Missing {} field", Self::#static_name),
                                              ))?.to_string()
                                          )
                                     }
                                }
                            } else {
                                quote! {
                                   #field_name: map.get(&cheetah_string::CheetahString::from_static_str(Self::#static_name)).cloned(),
                                 }
                            }
                        } else if is_struct_type && has_serde_flatten_attribute {
                            let type_ = has_option.unwrap();
                            quote! {
                                 #field_name: Some(<#type_ as crate::protocol::command_custom_header::FromMap>::from(map)?),
                             }
                        } else if required {
                            quote! {
                                   #field_name: Some(
                                          map.get(&cheetah_string::CheetahString::from_static_str(Self::#static_name)).ok_or(rocketmq_error::RocketmqError::DeserializeHeaderError(
                                             format!("Missing {} field", Self::#static_name),
                                         ))?
                                         .parse::<#value>()
                                         .map_err(|_| rocketmq_error::RocketmqError::DeserializeHeaderError(format!("Parse {} field error", Self::#static_name)))?
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
                                if type_name == "CheetahString" {
                                    quote! {
                                       #field_name: map.get(&cheetah_string::CheetahString::from_static_str(Self::#static_name))
                                              .cloned()
                                              .ok_or(rocketmq_error::RocketmqError::DeserializeHeaderError(
                                                 format!("Missing {} field", Self::#static_name),
                                              ))?,
                                     }
                                } else {
                                    quote! {
                                       #field_name: map.get(&cheetah_string::CheetahString::from_static_str(Self::#static_name))
                                              .cloned()
                                              .ok_or(rocketmq_error::RocketmqError::DeserializeHeaderError(
                                                 format!("Missing {} field", Self::#static_name),
                                              ))?.to_string(),
                                     }
                                }
                            } else {
                                quote! {
                                   #field_name: map.get(&cheetah_string::CheetahString::from_static_str(Self::#static_name)).cloned().unwrap_or_default(),
                                 }
                            }
                        } else if is_struct_type && has_serde_flatten_attribute {
                            let type_ = &field.ty;
                            quote! {
                                 #field_name: <#type_ as crate::protocol::command_custom_header::FromMap>::from(map)?,
                             }
                        } else if required {
                            quote! {
                                     #field_name:map.get(&cheetah_string::CheetahString::from_static_str(Self::#static_name)).ok_or(rocketmq_error::RocketmqError::DeserializeHeaderError(
                                         format!("Missing {} field", Self::#static_name),
                                     ))?
                                     .parse::<#types>()
                                     .map_err(|_| rocketmq_error::RocketmqError::DeserializeHeaderError(format!("Parse {} field error", Self::#static_name)))?,
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

            type Error = rocketmq_error::RocketmqError;

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

pub(super) fn request_header_codec_inner_v2(
    input: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let struct_name = &input.ident;

    let fields = match input.data {
        Data::Struct(value) => match value.fields {
            Fields::Named(f) => f.named,
            _ => return quote! {}.into(),
        },
        _ => return quote! {}.into(),
    };

    // Collect metadata for each field
    struct FieldMeta {
        ident: syn::Ident,
        ty: syn::Type,
        is_option: Option<syn::Type>, // inner type if Option<T>
        is_struct_type: bool,
        has_flatten: bool,
        required: bool,
        field_name_str: String, // camel case used as key string
    }

    let mut metas: Vec<FieldMeta> = Vec::new();
    for field in fields.iter() {
        let field_name = field.ident.as_ref().unwrap().clone();
        let ty = field.ty.clone();

        let mut required = false;
        let mut has_flatten = false;
        for attr in &field.attrs {
            if let Some(id) = attr.path().get_ident() {
                if id == "required" {
                    required = true;
                }
                if id == "serde" {
                    if let syn::Meta::List(meta_list) = &attr.meta {
                        let meta_tokens = meta_list.tokens.to_string();
                        if meta_tokens.contains("flatten") {
                            has_flatten = true;
                        }
                    }
                }
            }
        }

        let is_option_type = crate::is_option_type(&ty);
        let inner_opt = is_option_type.map(|t| (*t).clone());
        let is_struct_type_flag = crate::is_struct_type(&ty);

        let field_name_str = crate::snake_to_camel_case(&format!("{}", field_name));

        metas.push(FieldMeta {
            ident: field_name,
            ty,
            is_option: inner_opt,
            is_struct_type: is_struct_type_flag,
            has_flatten,
            required,
            field_name_str,
        });
    }

    // Build static constants (FIELD_X) for the struct as before, using format_ident!
    let static_fields_tokens: Vec<_> = metas
        .iter()
        .map(|m| {
            let const_ident = format_ident!("{}", m.ident.to_string().to_ascii_uppercase());
            let key_lit = syn::LitStr::new(&m.field_name_str, Span::call_site());
            quote! {
                const #const_ident: &'static str = #key_lit;
            }
        })
        .collect();

    // Build to_map tokens: pre-allocate capacity and insert shallow clones
    let to_map_tokens: Vec<_> = metas.iter().map(|m| {
        let const_ident = format_ident!("{}", m.ident.to_string().to_ascii_uppercase());
        let field_ident = &m.ident;
        if m.is_option.is_some() {
            if let Some(inner_ty) = &m.is_option {
                let type_name = crate::get_type_name(inner_ty);
                if type_name == "CheetahString" {
                    quote! {
                        if let Some(ref value) = self.#field_ident {
                            map.insert(
                                cheetah_string::CheetahString::from_static_str(Self::#const_ident),
                                value.clone()
                            );
                        }
                    }
                } else if type_name == "String" {
                    quote! {
                        if let Some(ref value) = self.#field_ident {
                            map.insert(
                                cheetah_string::CheetahString::from_static_str(Self::#const_ident),
                                cheetah_string::CheetahString::from_string(value.clone())
                            );
                        }
                    }
                } else if m.is_struct_type && m.has_flatten {
                    quote! {
                        if let Some(ref value) = self.#field_ident {
                            if let Some(sub) = value.to_map() {
                                map.extend(sub);
                            }
                        }
                    }
                } else {
                    quote! {
                        if let Some(ref value) = self.#field_ident {
                            map.insert(
                                cheetah_string::CheetahString::from_static_str(Self::#const_ident),
                                cheetah_string::CheetahString::from_string(value.to_string())
                            );
                        }
                    }
                }
            } else {
                quote! {}
            }
        } else {
            let type_name = crate::get_type_name(&m.ty);
            if type_name == "CheetahString" {
                quote! {
                    map.insert(
                        cheetah_string::CheetahString::from_static_str(Self::#const_ident),
                        self.#field_ident.clone()
                    );
                }
            } else if type_name == "String" {
                quote! {
                    map.insert(
                        cheetah_string::CheetahString::from_static_str(Self::#const_ident),
                        cheetah_string::CheetahString::from_string(self.#field_ident.clone())
                    );
                }
            } else if m.is_struct_type && m.has_flatten {
                quote! {
                    if let Some(sub) = self.#field_ident.to_map() {
                        map.extend(sub);
                    }
                }
            } else {
                quote! {
                    map.insert(
                        cheetah_string::CheetahString::from_static_str(Self::#const_ident),
                        cheetah_string::CheetahString::from_string(self.#field_ident.to_string())
                    );
                }
            }
        }
    }).collect();

    // Build optimized FromMap: single pass over map entries
    // 1) declare local Option vars for each field; create explicit idents for locals
    let local_decls: Vec<_> = metas
        .iter()
        .map(|m| {
            let local_ident = format_ident!("__{}", m.ident);
            quote! {
                let mut #local_ident: Option<cheetah_string::CheetahString> = None;
            }
        })
        .collect();

    // Build match arms: when key equals static FIELD, assign local = Some(v.clone())
    let match_arms: Vec<_> = metas
        .iter()
        .map(|m| {
            // skip flatten struct capturing
            if m.is_struct_type && m.has_flatten {
                quote! {}
            } else {
                let key_lit = syn::LitStr::new(&m.field_name_str, Span::call_site());
                let local_ident = format_ident!("__{}", m.ident);
                quote! {
                    #key_lit => {
                        #local_ident = Some(v.clone());
                    }
                }
            }
        })
        .collect();

    // After iteration: construct struct fields by parsing or unwraping as required
    let construct_fields: Vec<_> = metas.iter().map(|m| {
        let ident = &m.ident;
        //let const_ident = format_ident!("{}", m.ident.to_string().to_ascii_uppercase());
        let field_label = &m.field_name_str;

        if m.is_struct_type && m.has_flatten {
            if let Some(inner_ty) = &m.is_option {
                let ty_tokens = inner_ty;
                quote! {
                    #ident: Some(<#ty_tokens as crate::protocol::command_custom_header::FromMap>::from(map)?),
                }
            } else {
                let ty_tokens = &m.ty;
                quote! {
                    #ident: <#ty_tokens as crate::protocol::command_custom_header::FromMap>::from(map)?,
                }
            }
        } else {
            let local_ident = format_ident!("__{}", m.ident);
            let type_name = if let Some(inner) = &m.is_option {
                crate::get_type_name(inner)
            } else {
                crate::get_type_name(&m.ty)
            };

            let missing_msg = format!("Missing {} field", field_label);

            if m.is_option.is_some() {
                if type_name == "CheetahString" {
                    quote! {
                        #ident: #local_ident,
                    }
                } else if type_name == "String" {
                    quote! {
                        #ident: #local_ident.map(|s| s.to_string()),
                    }
                } else {
                    // Option<primitive>
                    // parse_ty: for Option<T>, we used inner type string earlier; here we reconstruct syn::Type
                    let parse_ty: syn::Type = syn::parse_str(&crate::get_type_name(m.is_option.as_ref().unwrap())).unwrap_or_else(|_| syn::parse_str("String").unwrap());
                    quote! {
                        #ident: #local_ident.and_then(|s| s.as_str().parse::<#parse_ty>().ok()),
                    }
                }
            } else if type_name == "CheetahString" {
                    if m.required {
                        quote! {
                            #ident: #local_ident.ok_or(rocketmq_error::RocketmqError::DeserializeHeaderError(
                                format!(#missing_msg)
                            ))?,
                        }
                    } else {
                        quote! {
                            #ident: #local_ident.unwrap_or_default(),
                        }
                    }
                } else if type_name == "String" {
                    if m.required {
                        quote! {
                            #ident: #local_ident.ok_or(rocketmq_error::RocketmqError::DeserializeHeaderError(
                                format!(#missing_msg)
                            ))?.to_string(),
                        }
                    } else {
                        quote! {
                            #ident: #local_ident.map(|s| s.to_string()).unwrap_or_default(),
                        }
                    }
                } else {
                    let parse_ty = &m.ty;
                    if m.required {
                        quote! {
                            #ident: #local_ident.ok_or(rocketmq_error::RocketmqError::DeserializeHeaderError(
                                format!(#missing_msg)
                            ))?.as_str().parse::<#parse_ty>().map_err(|_| rocketmq_error::RocketmqError::DeserializeHeaderError(format!("Parse {} field error", #field_label)))?,
                        }
                    } else {
                        quote! {
                            #ident: #local_ident.and_then(|s| s.as_str().parse::<#parse_ty>().ok()).unwrap_or_default(),
                        }
                    }
                }

        }
    }).collect();

    let fields_count = metas.len();
    let expanded = quote! {
        impl #struct_name {
            #(#static_fields_tokens)*
        }

        impl  rocketmq_remoting::protocol::command_custom_header::CommandCustomHeader for #struct_name {
            fn to_map(&self) -> Option<std::collections::HashMap<cheetah_string::CheetahString, cheetah_string::CheetahString>> {
                let mut map = std::collections::HashMap::with_capacity(#fields_count);
                #(#to_map_tokens)*
                Some(map)
            }
        }

        impl  rocketmq_remoting::protocol::command_custom_header::FromMap for #struct_name {
            type Error = rocketmq_error::RocketmqError;
            type Target = Self;

            fn from(map: &std::collections::HashMap<cheetah_string::CheetahString, cheetah_string::CheetahString>) -> Result<Self::Target, Self::Error> {
                #(#local_decls)*

                for (k, v) in map.iter() {
                    match k.as_str() {
                        #(#match_arms)*
                        _ => {}
                    }
                }

                Ok(#struct_name {
                    #(#construct_fields)*
                })
            }
        }
    };

    TokenStream::from(expanded)
}
