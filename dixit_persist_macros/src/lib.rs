use proc_macro::TokenStream;

mod parquet;

#[proc_macro_derive(Persist, attributes(persist_timestamp, persist))]
pub fn parquet_record_writer(input: TokenStream) -> TokenStream {
    parquet::persist_derive(input)
}
