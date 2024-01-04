pub mod extractor;

use std::time::Duration;

use axum::response::Html;
use bytes::Bytes;
use hyper::{body::to_bytes, header::CONTENT_LENGTH, Body, Method, Request};
use once_cell::sync::Lazy;
use url::Url;

use crate::{errors::Result, images::FAVICON_ICO, util::http::extractor::HYPER_CLIENT, PHRASE};

pub const HTTP_DATE_FORMAT: &str = "%a, %d %b %Y %H:%M:%S GMT";

pub async fn delete(url: &str, params: &[(&str, &str)]) -> Result<Bytes> {
    request(url, params, Method::DELETE, None).await
}

pub async fn get(url: &str, params: &[(&str, &str)]) -> Result<Bytes> {
    request(url, params, Method::GET, None).await
}

pub async fn post(url: &str, params: &[(&str, &str)], body: &[u8]) -> Result<Bytes> {
    request(url, params, Method::POST, Some(body)).await
}

async fn request(
    url: &str,
    params: &[(&str, &str)],
    method: Method,
    body: Option<&[u8]>,
) -> Result<Bytes> {
    let url = Url::parse_with_params(url, params)?;

    let request = Request::builder().method(method).uri(url.as_str());
    let request = match body {
        Some(body) => request
            .header(CONTENT_LENGTH, body.len())
            .body(Body::from(body.to_vec()))?,
        None => request.header(CONTENT_LENGTH, 0).body(Body::empty())?,
    };

    let mut response = HYPER_CLIENT.request(request).await?;
    let body = to_bytes(response.body_mut()).await?;
    Ok(body)
}

pub async fn default_handler() -> Html<&'static str> {
    Html(PHRASE)
}

pub async fn favicon_handler<'a>() -> Result<&'a [u8]> {
    FAVICON_ICO.bytes()
}

pub static HTTP_CLIENT: Lazy<reqwest::Client> = Lazy::new(|| {
    reqwest::Client::builder()
        .pool_idle_timeout(Duration::from_secs(30))
        .build()
        .expect("HTTP CLIENT initialize failed")
});
