pub mod extractor;

use std::time::Duration;

use axum::response::Html;
use bytes::Bytes;
use once_cell::sync::Lazy;
use reqwest::Body;
use url::Url;

use crate::{errors::Result, images::FAVICON_ICO, PHRASE};

pub const HTTP_DATE_FORMAT: &str = "%a, %d %b %Y %H:%M:%S GMT";

pub async fn get<U: AsRef<str>>(url: U, params: &[(&str, &str)]) -> Result<Bytes> {
    let url = Url::parse_with_params(url.as_ref(), params)?;
    Ok(HTTP_CLIENT.get(url).send().await?.bytes().await?)
}

pub async fn post<U: AsRef<str>, B: Into<Body>>(
    url: U,
    params: &[(&str, &str)],
    body: B,
) -> Result<Bytes> {
    let url = Url::parse_with_params(url.as_ref(), params)?;
    Ok(HTTP_CLIENT
        .post(url)
        .body(body)
        .send()
        .await?
        .bytes()
        .await?)
}

pub async fn delete<U: AsRef<str>>(url: U, params: &[(&str, &str)]) -> Result<Bytes> {
    let url = Url::parse_with_params(url.as_ref(), params)?;
    Ok(HTTP_CLIENT.delete(url).send().await?.bytes().await?)
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
