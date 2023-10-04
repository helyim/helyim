use heck::{ToSnakeCase, ToUpperCamelCase};
use proc_macro2::TokenStream;
use quote::{quote, ToTokens};
use syn::{
    punctuated::Punctuated, Attribute, FnArg, Ident, ImplItem, ImplItemFn, ItemImpl, Pat, PatType,
    Result, ReturnType, Token, Type, Visibility,
};

#[proc_macro_attribute]
pub fn event_fn(
    _attr: proc_macro::TokenStream,
    input: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let item_impl = syn::parse_macro_input!(input as syn::ItemImpl);
    let struct_type = item_impl.self_ty.to_token_stream().into();
    let struct_name = syn::parse_macro_input!(struct_type as syn::Ident);
    match do_expand(item_impl, struct_name) {
        Ok(token_stream) => token_stream.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn do_expand(item_impl: ItemImpl, struct_name: Ident) -> Result<TokenStream> {
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
    let gen_loop = generate_event_loop(&struct_name, &event_name, &fn_list)?;
    let token_stream = quote! {
        #item_impl

        #gen_enum

        #gen_fn

        #gen_loop
    };

    #[cfg(feature = "pretty_print")]
    {
        let file = syn::parse_file(&token_stream.to_string())?;
        let pretty = prettyplease::unparse(&file);

        use std::{fs, io::Write};
        let mut pretty_file = fs::OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open("pretty.rs")
            .expect("Unable to open file");
        pretty_file
            .write_all(pretty.as_bytes())
            .expect("Unable to write file");
    }

    Ok(token_stream)
}

fn generate_event_enum(event_name: &Ident, fn_list: &[&ImplItemFn]) -> Result<TokenStream> {
    let mut variants = Vec::new();
    for func in fn_list {
        if skip_fn(func) {
            continue;
        }
        let mut args: Punctuated<TokenStream, Token![,]> = Punctuated::new();

        for input in func.sig.inputs.iter() {
            if let FnArg::Typed(PatType { pat, ty, .. }) = input {
                if let Pat::Ident(ident) = pat.as_ref() {
                    let ident = &ident.ident;
                    args.push(quote!(#ident : #ty));
                }
            }
        }

        let variant = get_variant_ident(&func.sig.ident);
        let args = parse_args(args, false);

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
) -> Result<TokenStream> {
    let mut func_token_streams = TokenStream::new();
    for func in fn_list {
        if skip_fn(func) {
            continue;
        }
        let mut args: Punctuated<TokenStream, Token![,]> = Punctuated::new();
        let mut args_without_type: Punctuated<TokenStream, Token![,]> = Punctuated::new();
        for input in func.sig.inputs.iter() {
            if let FnArg::Typed(PatType { pat, ty, .. }) = input {
                if let Pat::Ident(ident) = pat.as_ref() {
                    let ident = &ident.ident;
                    args.push(quote!(#ident : #ty));
                    args_without_type.push(quote!(#ident));
                }
            }
        }

        let variant = get_variant_ident(&func.sig.ident);

        let func_name = &func.sig.ident;
        let args = parse_args(args, true);

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
                let (result, typ) = parse_result(typ);
                let args_without_type = parse_args(args_without_type, false);
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

fn generate_event_loop(
    struct_name: &syn::Ident,
    event_name: &syn::Ident,
    fn_list: &[&ImplItemFn],
) -> Result<TokenStream> {
    let mut variants = Vec::new();
    let instance_name = get_instance_ident(struct_name);
    for func in fn_list {
        if skip_fn(func) {
            continue;
        }
        let mut args_without_type: Punctuated<TokenStream, Token![,]> = Punctuated::new();
        let mut is_self_ref = true;
        for input in func.sig.inputs.iter() {
            match input {
                FnArg::Typed(PatType { pat, .. }) => {
                    if let Pat::Ident(ident) = pat.as_ref() {
                        let ident = &ident.ident;
                        args_without_type.push(quote!(#ident));
                    }
                }
                FnArg::Receiver(recv) => {
                    is_self_ref = recv.reference.is_some();
                }
            }
        }

        let variant = get_variant_ident(&func.sig.ident);
        let args_without_type = parse_args(args_without_type, false);
        let caller = generate_caller(
            &instance_name,
            &func.sig.ident,
            is_self_ref,
            &args_without_type,
            &func.sig.output,
            func.sig.asyncness.is_some(),
        );
        let variant = match &func.sig.output {
            ReturnType::Default => {
                quote! {
                    #event_name::#variant { #args_without_type } => {
                        #caller
                    }
                }
            }
            ReturnType::Type(..) => {
                quote! {
                    #event_name::#variant { #args_without_type tx } => {
                        #caller
                    }
                }
            }
        };
        variants.push(variant);
    }
    let (loop_name, rx_name) = get_loop_and_rx_name(&instance_name);
    Ok(quote! {
        pub async fn #loop_name(
            mut #instance_name: #struct_name,
            mut #rx_name: futures::channel::mpsc::UnboundedReceiver<#event_name>,
            mut shutdown: async_broadcast::Receiver<()>,
        ) {
            use futures::StreamExt;
            loop {
                tokio::select! {
                    Some(event) = #rx_name.next() => {
                        match event {
                            #(#variants),*
                        }
                    }
                    _ = shutdown.recv() => {
                        break;
                    }
                }
            }
        }
    })
}

fn get_variant_ident(name: &syn::Ident) -> syn::Ident {
    let variant = name.to_string().to_upper_camel_case();
    Ident::new(&variant, name.span())
}

fn get_instance_ident(name: &syn::Ident) -> syn::Ident {
    let instance = name.to_string().to_snake_case();
    Ident::new(&instance, name.span())
}

fn parse_args(args: Punctuated<TokenStream, Token![,]>, left_comma: bool) -> TokenStream {
    if !args.is_empty() {
        if left_comma {
            quote!(,#args)
        } else {
            quote!(#args,)
        }
    } else {
        quote!()
    }
}

fn parse_result(typ: &Type) -> (TokenStream, TokenStream) {
    let type_str = typ.to_token_stream().to_string().replace(' ', "");
    if type_str.starts_with("Result<") {
        (quote!(rx.await?), quote!(#typ))
    } else {
        (quote!(Ok(rx.await?)), quote!(Result<#typ>))
    }
}

fn not_self_fn(args: &Punctuated<FnArg, Token![,]>) -> bool {
    !args.iter().any(|arg| matches!(arg, FnArg::Receiver(_)))
}

fn not_pub_fn(vis: &Visibility) -> bool {
    !matches!(vis, Visibility::Public(_))
}

fn ignore_fn(attrs: &[Attribute]) -> bool {
    attrs.iter().any(|attr| attr.path().is_ident("ignore"))
}

fn skip_fn(func: &ImplItemFn) -> bool {
    not_self_fn(&func.sig.inputs) || not_pub_fn(&func.vis) || ignore_fn(&func.attrs)
}

fn generate_caller(
    instance: &Ident,
    fn_name: &Ident,
    is_self_ref: bool,
    args: &TokenStream,
    return_type: &ReturnType,
    is_async: bool,
) -> TokenStream {
    let async_ident = if is_async { quote!(.await) } else { quote!() };
    let mut caller = quote!(#instance.#fn_name(#args)#async_ident);
    caller = match return_type {
        ReturnType::Type(..) => quote! { let _ = tx.send(#caller); },
        ReturnType::Default => quote!(#caller;),
    };
    if !is_self_ref {
        caller = quote! { #caller break; }
    }
    caller
}

fn get_loop_and_rx_name(name: &Ident) -> (Ident, Ident) {
    let loop_name = format!("{name}_loop");
    let loop_name = Ident::new(&loop_name, name.span());
    let rx_name = format!("{name}_rx");
    let rx_name = Ident::new(&rx_name, name.span());
    (loop_name, rx_name)
}
