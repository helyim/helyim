use std::time::Duration;

use axum::response::Html;
use bytes::Bytes;
use hyper::{HeaderMap, Method};
use once_cell::sync::Lazy;
use reqwest::{Body, Response};
use url::Url;

use crate::{images::FAVICON_ICO, PHRASE};

pub const HTTP_DATE_FORMAT: &str = "%a, %d %b %Y %H:%M:%S GMT";

pub async fn get<U: AsRef<str>>(url: U, params: &[(&str, &str)]) -> Result<Bytes, HttpError> {
    let url = Url::parse_with_params(url.as_ref(), params)?;
    Ok(HTTP_CLIENT.get(url).send().await?.bytes().await?)
}

pub async fn post<U: AsRef<str>, B: Into<Body>>(
    url: U,
    params: &[(&str, &str)],
    body: B,
) -> Result<Bytes, HttpError> {
    let url = Url::parse_with_params(url.as_ref(), params)?;
    Ok(HTTP_CLIENT
        .post(url)
        .body(body)
        .send()
        .await?
        .bytes()
        .await?)
}

pub async fn delete<U: AsRef<str>>(url: U, params: &[(&str, &str)]) -> Result<Bytes, HttpError> {
    let url = Url::parse_with_params(url.as_ref(), params)?;
    Ok(HTTP_CLIENT.delete(url).send().await?.bytes().await?)
}

pub async fn request<U: AsRef<str>, B: Into<Body>>(
    url: U,
    method: Method,
    params: &[(&str, &str)],
    body: B,
    headers: HeaderMap,
) -> Result<Response, HttpError> {
    let url = Url::parse_with_params(url.as_ref(), params)?;
    Ok(HTTP_CLIENT
        .request(method, url)
        .body(body)
        .headers(headers)
        .send()
        .await?)
}

pub struct FormOrJson<T>(pub T);

pub async fn default_handler() -> Html<&'static str> {
    Html(PHRASE)
}

pub async fn favicon_handler<'a>() -> &'a [u8] {
    FAVICON_ICO.bytes()
}

pub static HTTP_CLIENT: Lazy<reqwest::Client> = Lazy::new(|| {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .connect_timeout(Duration::from_secs(10))
        .pool_idle_timeout(Duration::from_secs(30))
        .http2_keep_alive_timeout(Duration::from_secs(60))
        .build()
        .expect("HTTP CLIENT initialize failed")
});

#[derive(thiserror::Error, Debug)]
pub enum HttpError {
    #[error("error: {0}")]
    Box(#[from] Box<dyn std::error::Error + Sync + Send>),

    #[error("Hyper error: {0}")]
    Hyper(#[from] hyper::Error),
    #[error("Axum http error: {0}")]
    AxumHttp(#[from] axum::http::Error),
    #[error("Reqwest error: {0}")]
    Reqwest(#[from] reqwest::Error),

    #[error("Url parse error: {0}")]
    UrlParse(#[from] url::ParseError),
}
