use std::time::Duration;

use axum::{
    body::Body as AxumBody,
    http::{
        header::{InvalidHeaderValue, ToStrError, CONTENT_RANGE, X_CONTENT_TYPE_OPTIONS},
        HeaderValue, Response as AxumResponse, StatusCode,
    },
    response::Html,
};
use bytes::Bytes;
use http_range::HttpRange;
use hyper::{header::CONTENT_TYPE, HeaderMap, Method};
use once_cell::sync::Lazy;
use reqwest::{
    multipart::{Form, Part},
    Body, Response,
};
use url::Url;

use crate::{anyhow, images::FAVICON_ICO, PHRASE};

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

    #[error("InvalidHeaderValue: {0}")]
    InvalidHeaderValue(#[from] InvalidHeaderValue),
    #[error("ToStr error: {0}")]
    ToStr(#[from] ToStrError),
    #[error("Mime FromStr error: {0}")]
    FromStr(#[from] mime::FromStrError),

    #[error("Url parse error: {0}")]
    UrlParse(#[from] url::ParseError),
}

pub fn set_etag(res: &mut AxumResponse<AxumBody>, etag: &str) -> Result<(), InvalidHeaderValue> {
    if !etag.is_empty() {
        if etag.starts_with("\"") {
            res.headers_mut()
                .insert("ETag", HeaderValue::from_str(etag)?);
        } else {
            res.headers_mut().insert(
                "ETag",
                HeaderValue::from_str(format!("\"{etag}\"").as_str())?,
            );
        }
    }
    Ok(())
}

pub async fn read_url(file_url: &str, offset: i64, size: u32) -> Result<Bytes, HttpError> {
    let mut headers = HeaderMap::new();
    headers.insert(
        "Range",
        HeaderValue::from_str(&format!("bytes={offset}-{}", offset + size as i64))?,
    );

    let response = request(file_url, Method::GET, &[], Bytes::new(), headers).await?;
    if response.status().as_u16() >= 400 {
        return Err(anyhow!("GET {} error: {}", file_url, response.status()));
    }

    Ok(response.bytes().await?)
}

pub fn http_error(
    response: &mut AxumResponse<AxumBody>,
    status: StatusCode,
    error: String,
) -> Result<(), HttpError> {
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_str("text/plain; charset=utf-8")?,
    );
    response.headers_mut().insert(
        X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_str("text/plain; charset=utf-8")?,
    );

    *response.status_mut() = status;
    *response.body_mut() = AxumBody::from(error);

    Ok(())
}

pub fn content_range(range: HttpRange, size: u64) -> String {
    format!(
        "bytes {}-{}/{}",
        range.start,
        range.start + range.length - 1,
        size
    )
}

pub fn ranges_mime_size(
    ranges: &[HttpRange],
    content_type: &str,
    content_size: u64,
) -> Result<u64, HttpError> {
    let mut form = Form::new();
    let mut encode_size = 0;
    for ra in ranges {
        let part = Part::bytes(&[]).headers(mime_header(*ra, content_type, content_size)?);
        form = form.part("", part);
        encode_size += ra.length;
    }

    if let Some(len) = form.compute_length() {
        encode_size += len;
    }
    Ok(encode_size)
}

pub fn mime_header(
    range: HttpRange,
    content_type: &str,
    content_size: u64,
) -> Result<HeaderMap, HttpError> {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_str(content_type)?);
    headers.insert(
        CONTENT_RANGE,
        HeaderValue::from_str(&content_range(range, content_size))?,
    );
    Ok(headers)
}
