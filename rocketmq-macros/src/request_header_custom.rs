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
use proc_macro2::Span;
use proc_macro2::TokenStream as TokenStream2;
use proc_macro2::TokenTree;
use quote::format_ident;
use quote::quote;
use syn::parse_macro_input;
use syn::Data;
use syn::DeriveInput;
use syn::Fields;
use syn::Ident;
use syn::Meta;

use crate::get_type_name;
use crate::has_serde_flatten_attribute;
use crate::is_option_type;
use crate::is_struct_type;
use crate::snake_to_camel_case;

pub(super) fn request_header_codec_inner(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
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

            type Error = rocketmq_error::RocketMQError;

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

/// Field metadata extracted during macro processing
struct FieldMetadata {
    ident: syn::Ident,
    ty: syn::Type,
    inner_type: Option<syn::Type>,
    is_required: bool,
    camel_key: String,
    type_category: TypeCategory,
}

/// Categorize field types for optimized code generation
#[derive(PartialEq, Eq)]
enum TypeCategory {
    CheetahString,
    String,
    Primitive,
    StructFlattened,
}

impl FieldMetadata {
    fn from_field(field: &syn::Field) -> Self {
        let ident = field.ident.as_ref().unwrap().clone();
        let ty = field.ty.clone();

        // Parse attributes
        let mut is_required = false;
        let mut is_flatten = false;

        for attr in &field.attrs {
            if let Some(id) = attr.path().get_ident() {
                if id == "required" {
                    is_required = true;
                }
                if id == "serde" {
                    if let Meta::List(meta_list) = &attr.meta {
                        for token in meta_list.tokens.clone().into_iter() {
                            if let TokenTree::Ident(ident) = token {
                                if ident.eq("flatten") {
                                    is_flatten = true;
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }

        // Type analysis
        let inner_type = is_option_type(&ty).map(|t| (*t).clone());
        let is_struct = is_struct_type(&ty);

        // Determine type category for optimized handling
        // This classification is used throughout code generation to avoid repeated type checking
        let base_type = inner_type.as_ref().unwrap_or(&ty);
        let type_name = get_type_name(base_type);

        let type_category = if is_struct && is_flatten {
            TypeCategory::StructFlattened
        } else if type_name == "CheetahString" {
            TypeCategory::CheetahString
        } else if type_name == "String" {
            TypeCategory::String
        } else {
            TypeCategory::Primitive
        };

        let camel_key = snake_to_camel_case(&ident.to_string());

        Self {
            ident,
            ty,
            inner_type,
            is_required,
            camel_key,
            type_category,
        }
    }

    /// Generate constant field name declaration
    fn gen_const_decl(&self) -> TokenStream2 {
        let const_ident = format_ident!("{}", self.ident.to_string().to_ascii_uppercase());
        let key_lit = syn::LitStr::new(&self.camel_key, Span::call_site());
        quote! {
            const #const_ident: &'static str = #key_lit;
        }
    }

    /// Generate serialization code for to_map()
    fn gen_to_map(&self) -> TokenStream2 {
        let field_ident = &self.ident;
        let const_ident = format_ident!("{}", self.ident.to_string().to_ascii_uppercase());

        // Handle struct flattening
        if self.type_category == TypeCategory::StructFlattened {
            return if self.inner_type.is_some() {
                quote! {
                    if let Some(ref value) = self.#field_ident {
                        if let Some(sub) = value.to_map() {
                            map.extend(sub);
                        }
                    }
                }
            } else {
                quote! {
                    if let Some(sub) = self.#field_ident.to_map() {
                        map.extend(sub);
                    }
                }
            };
        }

        // Optimized handling for different type categories
        match (&self.type_category, self.inner_type.is_some()) {
            (TypeCategory::CheetahString, true) => quote! {
                if let Some(ref value) = self.#field_ident {
                    map.insert(
                        cheetah_string::CheetahString::from_static_str(Self::#const_ident),
                        value.clone()
                    );
                }
            },
            (TypeCategory::CheetahString, false) => quote! {
                map.insert(
                    cheetah_string::CheetahString::from_static_str(Self::#const_ident),
                    self.#field_ident.clone()
                );
            },
            (TypeCategory::String, true) => quote! {
                if let Some(ref value) = self.#field_ident {
                    map.insert(
                        cheetah_string::CheetahString::from_static_str(Self::#const_ident),
                        cheetah_string::CheetahString::from_string(value.clone())
                    );
                }
            },
            (TypeCategory::String, false) => quote! {
                map.insert(
                    cheetah_string::CheetahString::from_static_str(Self::#const_ident),
                    cheetah_string::CheetahString::from_string(self.#field_ident.clone())
                );
            },
            (TypeCategory::Primitive, true) => quote! {
                if let Some(ref value) = self.#field_ident {
                    map.insert(
                        cheetah_string::CheetahString::from_static_str(Self::#const_ident),
                        cheetah_string::CheetahString::from_string(value.to_string())
                    );
                }
            },
            (TypeCategory::Primitive, false) => quote! {
                map.insert(
                    cheetah_string::CheetahString::from_static_str(Self::#const_ident),
                    cheetah_string::CheetahString::from_string(self.#field_ident.to_string())
                );
            },
            _ => quote! {},
        }
    }

    /// Generate local variable declaration for from_map()
    fn gen_local_decl(&self) -> Option<TokenStream2> {
        if self.type_category == TypeCategory::StructFlattened {
            return None;
        }

        let local_ident = format_ident!("__{}", self.ident);
        Some(quote! {
            let mut #local_ident: Option<cheetah_string::CheetahString> = None;
        })
    }

    /// Generate match arm for key extraction
    fn gen_match_arm(&self) -> Option<TokenStream2> {
        if self.type_category == TypeCategory::StructFlattened {
            return None;
        }

        let key_lit = syn::LitStr::new(&self.camel_key, Span::call_site());
        let local_ident = format_ident!("__{}", self.ident);

        Some(quote! {
            #key_lit => {
                #local_ident = Some(v.clone());
            }
        })
    }

    /// Generate struct field construction code
    fn gen_field_construct(&self) -> TokenStream2 {
        let field_ident = &self.ident;
        let missing_msg = format!("Missing {} field", self.camel_key);

        // Handle flattened structs via recursive FromMap call
        if self.type_category == TypeCategory::StructFlattened {
            return if let Some(inner_ty) = &self.inner_type {
                quote! {
                    #field_ident: Some(<#inner_ty as crate::protocol::command_custom_header::FromMap>::from(map)?),
                }
            } else {
                let ty = &self.ty;
                quote! {
                    #field_ident: <#ty as crate::protocol::command_custom_header::FromMap>::from(map)?,
                }
            };
        }

        let local_ident = format_ident!("__{}", self.ident);

        // Optimized field construction based on type and optionality
        match (&self.type_category, self.inner_type.is_some(), self.is_required) {
            // StructFlattened should never reach here (handled above)
            (TypeCategory::StructFlattened, _, _) => quote! {},
            // Option<CheetahString>
            (TypeCategory::CheetahString, true, _) => quote! {
                #field_ident: #local_ident,
            },
            // CheetahString (required)
            (TypeCategory::CheetahString, false, true) => quote! {
                #field_ident: #local_ident.ok_or_else(||
                    rocketmq_error::RocketmqError::DeserializeHeaderError(#missing_msg.to_string())
                )?,
            },
            // CheetahString (optional with default)
            (TypeCategory::CheetahString, false, false) => quote! {
                #field_ident: #local_ident.unwrap_or_default(),
            },
            // Option<String>
            (TypeCategory::String, true, _) => quote! {
                #field_ident: #local_ident.map(|s| s.to_string()),
            },
            // String (required)
            (TypeCategory::String, false, true) => quote! {
                #field_ident: #local_ident.ok_or_else(||
                    rocketmq_error::RocketmqError::DeserializeHeaderError(#missing_msg.to_string())
                )?.to_string(),
            },
            // String (optional with default)
            (TypeCategory::String, false, false) => quote! {
                #field_ident: #local_ident.map(|s| s.to_string()).unwrap_or_default(),
            },
            // Option<Primitive>
            (TypeCategory::Primitive, true, _) => {
                let inner_ty = self.inner_type.as_ref().unwrap();
                let parse_error = format!("Parse {} field error", self.camel_key);
                quote! {
                    #field_ident: match #local_ident {
                        Some(s) => s.as_str().parse::<#inner_ty>()
                            .map(Some)
                            .map_err(|_| rocketmq_error::RocketmqError::DeserializeHeaderError(#parse_error.to_string()))?,
                        None => None,
                    },
                }
            }
            // Primitive (required)
            (TypeCategory::Primitive, false, true) => {
                let ty = &self.ty;
                let parse_error = format!("Parse {} field error", self.camel_key);
                quote! {
                    #field_ident: #local_ident
                        .ok_or_else(|| rocketmq_error::RocketmqError::DeserializeHeaderError(#missing_msg.to_string()))?
                        .as_str()
                        .parse::<#ty>()
                        .map_err(|_| rocketmq_error::RocketmqError::DeserializeHeaderError(#parse_error.to_string()))?,
                }
            }
            // Primitive (optional with default)
            (TypeCategory::Primitive, false, false) => {
                let ty = &self.ty;
                quote! {
                    #field_ident: #local_ident
                        .and_then(|s| s.as_str().parse::<#ty>().ok())
                        .unwrap_or_default(),
                }
            }
        }
    }
}

pub(super) fn request_header_codec_inner_v2(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let struct_name = &input.ident;

    // Extract named fields
    let fields = match input.data {
        Data::Struct(value) => match value.fields {
            Fields::Named(f) => f.named,
            _ => return quote! {}.into(),
        },
        _ => return quote! {}.into(),
    };

    // Build metadata for all fields using the optimized FieldMetadata struct
    let field_metas: Vec<FieldMetadata> = fields.iter().map(FieldMetadata::from_field).collect();

    // Generate constant declarations
    let const_decls: Vec<_> = field_metas.iter().map(|m| m.gen_const_decl()).collect();

    // Generate to_map() implementation with optimized type handling
    let to_map_stmts: Vec<_> = field_metas.iter().map(|m| m.gen_to_map()).collect();
    let fields_count = field_metas.len();

    // Generate from_map() implementation with single-pass iteration
    let local_decls: Vec<_> = field_metas.iter().filter_map(|m| m.gen_local_decl()).collect();

    let match_arms: Vec<_> = field_metas.iter().filter_map(|m| m.gen_match_arm()).collect();

    let construct_fields: Vec<_> = field_metas.iter().map(|m| m.gen_field_construct()).collect();

    // Generate final implementation
    let expanded = quote! {
        impl #struct_name {
            #(#const_decls)*
        }

        impl crate::protocol::command_custom_header::CommandCustomHeader for #struct_name {
            fn to_map(&self) -> Option<std::collections::HashMap<cheetah_string::CheetahString, cheetah_string::CheetahString>> {
                // Pre-allocate with exact capacity to avoid rehashing
                let mut map = std::collections::HashMap::with_capacity(#fields_count);
                #(#to_map_stmts)*
                Some(map)
            }
        }

        impl crate::protocol::command_custom_header::FromMap for #struct_name {
            type Error = rocketmq_error::RocketMQError;
            type Target = Self;

            fn from(map: &std::collections::HashMap<cheetah_string::CheetahString, cheetah_string::CheetahString>) -> Result<Self::Target, Self::Error> {
                // Declare local variables for capturing values
                #(#local_decls)*

                // Single-pass iteration over map entries
                // This is O(m) instead of O(n * log m) for multiple lookups
                for (k, v) in map.iter() {
                    match k.as_str() {
                        #(#match_arms)*
                        _ => {
                            // Silently ignore unknown keys for forward compatibility
                        }
                    }
                }

                // Construct struct with validation
                Ok(#struct_name {
                    #(#construct_fields)*
                })
            }
        }
    };

    TokenStream::from(expanded)
}
