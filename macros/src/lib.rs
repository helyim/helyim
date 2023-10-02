use heck::ToUpperCamelCase;
use proc_macro::TokenStream;
use quote::{quote, ToTokens};
use syn::{
    punctuated::Punctuated, FnArg, Ident, ImplItem, ImplItemFn, ItemImpl, Pat, Result, ReturnType,
    Token,
};

#[proc_macro_attribute]
pub fn event_fn(_args: TokenStream, input: TokenStream) -> TokenStream {
    let item_impl = syn::parse_macro_input!(input as syn::ItemImpl);
    let struct_type = item_impl.self_ty.to_token_stream().into();
    let struct_name = syn::parse_macro_input!(struct_type as syn::Ident);
    match do_expand(item_impl, struct_name) {
        Ok(token_stream) => token_stream.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn do_expand(item_impl: ItemImpl, struct_name: Ident) -> Result<proc_macro2::TokenStream> {
    let event_name = Ident::new(&format!("{}Event", struct_name), struct_name.span());
    let event_tx_name = Ident::new(&format!("{}EventTx", struct_name), struct_name.span());

    let mut fn_list = Vec::new();
    for item in item_impl.items.iter() {
        if let ImplItem::Fn(f) = item {
            fn_list.push(f);
        }
    }

    let gen_enum = generate_event_enum(&event_name, &fn_list)?;
    let gen_fn = generate_event_tx(&event_name, &event_tx_name, &fn_list)?;

    Ok(quote! {
        #item_impl
        #gen_enum
        #gen_fn
    })
}

fn generate_event_enum(
    event_name: &Ident,
    fn_list: &[&ImplItemFn],
) -> Result<proc_macro2::TokenStream> {
    let mut variants = Vec::new();
    for func in fn_list {
        let mut has_self = false;
        let mut args: Punctuated<proc_macro2::TokenStream, Token![,]> = Punctuated::new();
        for input in func.sig.inputs.iter() {
            match input {
                FnArg::Receiver(_) => {
                    has_self = true;
                }
                FnArg::Typed(pat_type) => {
                    if let Pat::Ident(ident) = pat_type.pat.as_ref() {
                        let ident = &ident.ident;
                        let ty = &pat_type.ty;
                        args.push(quote!(#ident : #ty));
                    }
                }
            }
        }
        if !has_self {
            continue;
        }

        let variant = func.sig.ident.to_string().to_upper_camel_case();
        let variant = Ident::new(&variant, func.sig.ident.span());

        let args = if !args.is_empty() {
            quote!(#args,)
        } else {
            quote!()
        };

        let variant = match &func.sig.output {
            ReturnType::Default => {
                quote! { #variant { #args } }
            }
            ReturnType::Type(_, typ) => {
                quote! { #variant { #args tx: futures::channel::oneshot::Sender<#typ> } }
            }
        };
        variants.push(variant);
    }
    Ok(quote! {
        pub enum #event_name {
            #(#variants),*
        }
    })
}

fn generate_event_tx(
    event_name: &syn::Ident,
    event_tx_name: &syn::Ident,
    fn_list: &[&ImplItemFn],
) -> Result<proc_macro2::TokenStream> {
    let mut func_token_streams = proc_macro2::TokenStream::new();

    for func in fn_list {
        let mut has_self = false;
        let mut args: Punctuated<proc_macro2::TokenStream, Token![,]> = Punctuated::new();
        let mut args_without_type: Punctuated<proc_macro2::TokenStream, Token![,]> =
            Punctuated::new();
        for input in func.sig.inputs.iter() {
            match input {
                FnArg::Receiver(_) => {
                    has_self = true;
                }
                FnArg::Typed(pat_type) => {
                    if let Pat::Ident(ident) = pat_type.pat.as_ref() {
                        let ident = &ident.ident;
                        let ty = &pat_type.ty;
                        args.push(quote!(#ident : #ty));
                        args_without_type.push(quote!(#ident));
                    }
                }
            }
        }
        if !has_self {
            continue;
        }

        let variant = func.sig.ident.to_string().to_upper_camel_case();
        let variant = Ident::new(&variant, func.sig.ident.span());

        let func_name = &func.sig.ident;
        let args = if !args.is_empty() {
            quote!(, #args)
        } else {
            quote!()
        };

        let func = match &func.sig.output {
            ReturnType::Default => {
                quote! {
                    pub fn #func_name(&self #args) ->  Result<()> {
                        self.0.unbounded_send(#event_name::#variant {
                            #args_without_type
                        })?;
                        Ok(())
                    }
                }
            }
            ReturnType::Type(_, typ) => {
                let typ_str = typ.to_token_stream().to_string().replace(' ', "");
                let (result, typ) = if typ_str.starts_with("Result<") {
                    (quote!(rx.await?), quote!(#typ))
                } else {
                    (quote!(Ok(rx.await?)), quote!(Result<#typ>))
                };
                let args_without_type = if !args_without_type.is_empty() {
                    quote!(#args_without_type,)
                } else {
                    quote!()
                };
                quote! {
                    pub async fn #func_name(&self #args) -> #typ  {
                        let (tx, rx) = futures::channel::oneshot::channel();
                        self.0.unbounded_send(#event_name::#variant {
                            #args_without_type tx
                        })?;
                        #result
                    }
                }
            }
        };

        func_token_streams.extend(func);
    }

    Ok(quote! {
        #[derive(Debug, Clone)]
        pub struct #event_tx_name(futures::channel::mpsc::UnboundedSender<#event_name>);

        impl #event_tx_name {
            pub fn new(tx: futures::channel::mpsc::UnboundedSender<#event_name>) -> Self {
                Self(tx)
            }

            #func_token_streams
        }
    })
}
