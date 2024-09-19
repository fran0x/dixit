use crate::parquet::LogicalType::Timestamp;
use crate::parquet::TimeUnit::{Micros, Millis, Nanos};

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Attribute, Data, DeriveInput, Fields, Lit, Meta, NestedMeta};

/// A procedural macro that implements the `Persistable` trait for a given struct or enum.
///
/// This macro generates the `schema` and `append` methods, which are used to persist
/// data structures into Parquet format.
pub fn persist_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let schema_body = generate_schema_body(&input.data, name);
    let append_body = generate_append_body(&input.data, name);

    let expanded = quote! {
        impl record_persist::Persistable for #name {

            fn schema(fields: &mut Vec<parquet::schema::types::TypePtr>, prefix: core::option::Option<&str>, repetition_override: Option<parquet::basic::Repetition>, logical_type: Option<parquet::basic::LogicalType>) {
                use record_persist::row::*;
                use record_persist::*;
                use parquet::basic::Type as PhysicalType;

                #schema_body
            }

            fn append(&self, row: &mut record_persist::row::RowBuffer) -> anyhow::Result<(), ::parquet::errors::ParquetError> {
                use record_persist::row::*;
                use record_persist::*;
                use parquet::basic::Type as PhysicalType;

                #append_body
                Ok(())
            }
        }
    };

    TokenStream::from(expanded)
}

/// Generates the schema body based on the data type of the struct or enum.
///
/// This function handles named fields, unnamed fields, and enums separately, generating the appropriate
/// schema code for each case. For structs, it iterates over the fields and generates schema entries for each
/// non-ignored field. For enums, it adds a BYTE_ARRAY field to represent the enum variant.
fn generate_schema_body(data: &Data, name: &syn::Ident) -> proc_macro2::TokenStream {
    match data {
        Data::Struct(ref data) => match &data.fields {
            Fields::Named(fields) => {
                let field_schemas = fields.named.iter().filter_map(|f| {
                    let field_name = &f.ident;
                    let field_type = &f.ty;

                    let persist_attrs = parse_persist_attributes(&f.attrs);
                    if persist_attrs.ignore {
                        None
                    } else {
                        let logical_type_code = if let Some(logical_type) = persist_attrs.logical_type {
                            let logical_type_tokens = logical_type_to_tokens(&logical_type);
                            quote! {
                                Some(#logical_type_tokens)
                            }
                        } else {
                            quote! {
                                None
                            }
                        };

                        Some(quote! {
                            let name = stringify!(#field_name);
                            let name = match prefix {
                                Some(p) => format!("{}_{}", p, name),
                                None => name.to_string(),
                            };
                            <#field_type>::schema(fields, Some(&name), repetition_override, #logical_type_code);
                        })
                    }
                });

                quote! {
                    #(#field_schemas)*
                }
            }
            Fields::Unnamed(fields) => {
                let field_schemas = fields.unnamed.iter().enumerate().map(|(i, f)| {
                    let field_type = &f.ty;
                    let index = syn::Index::from(i);
                    let persist_attrs = parse_persist_attributes(&f.attrs);
                    let logical_type_code = if let Some(logical_type) = persist_attrs.logical_type {
                        let logical_type_tokens = logical_type_to_tokens(&logical_type);
                        quote! {
                            Some(#logical_type_tokens)
                        }
                    } else {
                        quote! {
                            None
                        }
                    };

                    Some(quote! {
                        let name = match prefix {
                            Some(p) => format!("{}_{}", p, #index),
                            None => stringify!(#index).to_string(),
                        };
                        <#field_type>::schema(fields, Some(&name), repetition_override, #logical_type_code);
                    })
                });

                quote! {
                    #(#field_schemas)*
                }
            }
            _ => quote! {
                return Err(::parquet::errors::ParquetError::General(format!("Unimplemented field type: {:?}", #name)));
            },
        },
        Data::Enum(_) => {
            quote! {
                fields.push(
                    parquet::schema::types::Type::primitive_type_builder(
                        &prefix.unwrap_or_else(|| stringify!(#name)),
                        PhysicalType::BYTE_ARRAY,
                    )
                    .with_repetition(repetition_override.unwrap_or(parquet::basic::Repetition::REQUIRED))
                    .with_logical_type(Some(parquet::basic::LogicalType::String))
                    .build()
                    .unwrap()
                    .into(),
                );
            }
        }
        _ => quote! {
            return Err(::parquet::errors::ParquetError::General(format!("Unimplemented data type: {:?}", #name)));
        },
    }
}

fn logical_type_to_tokens(logical_type: &LogicalType) -> proc_macro2::TokenStream {
    let unit_tokens = match logical_type {
        Timestamp(Nanos) => quote! { parquet::format::TimeUnit::NANOS(parquet::format::NanoSeconds::new()) },
        Timestamp(Micros) => quote! { parquet::format::TimeUnit::MICROS(parquet::format::MicroSeconds::new()) },
        Timestamp(Millis) => quote! { parquet::format::TimeUnit::MILLIS(parquet::format::MilliSeconds::new()) },
    };
    quote! {
        parquet::basic::LogicalType::Timestamp {
            is_adjusted_to_u_t_c: true,
            unit: #unit_tokens
        }
    }
}

/// Generates the body for appending data to a Parquet row buffer.
///
/// This function handles named fields, unnamed fields, and enums separately, generating the appropriate
/// append code for each case. For structs, it iterates over the fields and appends each non-ignored
/// field's value to the row buffer. For enums, it adds the string representation of the enum variant.
fn generate_append_body(data: &Data, name: &syn::Ident) -> proc_macro2::TokenStream {
    match data {
        Data::Struct(ref data) => match &data.fields {
            Fields::Named(fields) => {
                let field_appends = fields.named.iter().filter_map(|f| {
                    let field_name = &f.ident;

                    let persist_attrs = parse_persist_attributes(&f.attrs);

                    if persist_attrs.ignore {
                        None
                    } else {
                        Some(quote! {
                            self.#field_name.append(row)?;
                        })
                    }
                });

                quote! {
                    #(#field_appends)*
                }
            }
            Fields::Unnamed(fields) => {
                let field_appends = fields.unnamed.iter().enumerate().map(|(i, _)| {
                    let index = syn::Index::from(i);
                    Some(quote! {
                        self.#index.append(row)?;
                    })
                });

                quote! {
                    #(#field_appends)*
                }
            }
            _ => quote! {
                return Err(::parquet::errors::ParquetError::General(format!("Unimplemented field type: {:?}", #name)));
            },
        },
        Data::Enum(ref data) => {
            let match_arms = data.variants.iter().map(|v| {
                let variant_name = &v.ident;
                let variant_str = variant_name.to_string();

                // Handle enum variants with no arguments, one argument, or multiple arguments
                match v.fields {
                    Fields::Unit => {
                        quote! {
                            #name::#variant_name => {
                                row.push(parquet::record::Field::Str(#variant_str.to_string()));
                            }
                        }
                    }
                    Fields::Unnamed(_) | Fields::Named(_) => {
                        quote! {
                            #name::#variant_name(..) => {
                                row.push(parquet::record::Field::Str(#variant_str.to_string()));
                            }
                        }
                    }
                }
            });

            quote! {
                match self {
                    #(#match_arms),*
                }
            }
        }
        _ => quote! {
            return Err(::parquet::errors::ParquetError::General(format!("Unimplemented data type: {:?}", #name)));
        },
    }
}

struct PersistAttributes {
    ignore: bool,
    logical_type: Option<LogicalType>,
}

enum TimeUnit {
    Nanos,
    Micros,
    Millis,
}

enum LogicalType {
    Timestamp(TimeUnit),
}

fn parse_persist_attributes(attrs: &Vec<Attribute>) -> PersistAttributes {
    let mut persist_attributes = PersistAttributes {
        ignore: false,
        logical_type: None,
    };

    for attr in attrs {
        if attr.path.is_ident("persist") {
            if let Ok(Meta::List(meta_list)) = attr.parse_meta() {
                for nested_meta in meta_list.nested {
                    if let NestedMeta::Meta(Meta::NameValue(meta_name_value)) = nested_meta {
                        if meta_name_value.path.is_ident("ignore") {
                            if let Lit::Bool(lit_bool) = meta_name_value.lit {
                                persist_attributes.ignore = lit_bool.value;
                            }
                        }
                    }
                }
            }
        }
        if attr.path.is_ident("persist_timestamp") {
            if let Ok(Meta::List(meta_list)) = attr.parse_meta() {
                for nested_meta in meta_list.nested {
                    if let NestedMeta::Meta(Meta::NameValue(meta_name_value)) = nested_meta {
                        if meta_name_value.path.is_ident("unit") {
                            if let Lit::Str(lit_str) = meta_name_value.lit {
                                persist_attributes.logical_type = match lit_str.value().as_str() {
                                    "ns" => Some(Timestamp(Nanos)),
                                    "ms" => Some(Timestamp(Millis)),
                                    "us" => Some(Timestamp(Micros)),
                                    _ => None,
                                };
                            }
                        }
                    }
                }
            }
        }
    }
    persist_attributes
}
