use std::time::Duration;

use axum::{
    body::Body as AxumBody,
    extract::multipart::MultipartError,
    http::{
        header::{ToStrError, CONTENT_RANGE, X_CONTENT_TYPE_OPTIONS},
        HeaderValue, Response as AxumResponse, StatusCode,
    },
    response::Html,
};
use bytes::Bytes;
use futures_util::StreamExt;
use http_range::HttpRange;
use hyper::{
    header::{InvalidHeaderName, InvalidHeaderValue, CONTENT_TYPE},
    HeaderMap, Method,
};
use hyper::header::RANGE;
use once_cell::sync::Lazy;
use reqwest::{
    header::ETAG,
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
    params: Option<&[(&str, &str)]>,
    body: Option<B>,
    headers: Option<HeaderMap>,
    multipart: Option<Form>,
) -> Result<Response, HttpError> {
    let url = match params {
        Some(params) => Url::parse_with_params(url.as_ref(), params)?,
        None => Url::parse(url.as_ref())?,
    };
    let mut builder = HTTP_CLIENT.request(method, url);
    if let Some(body) = body {
        builder = builder.body(body);
    }
    if let Some(headers) = headers {
        builder = builder.headers(headers);
    }
    if let Some(multipart) = multipart {
        builder = builder.multipart(multipart);
    }

    Ok(builder.send().await?)
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
    #[error("Multipart error: {0}")]
    Multipart(#[from] MultipartError),

    #[error("InvalidHeaderName: {0}")]
    InvalidHeaderName(#[from] InvalidHeaderName),
    #[error("InvalidHeaderValue: {0}")]
    InvalidHeaderValue(#[from] InvalidHeaderValue),
    #[error("ToStr error: {0}")]
    ToStr(#[from] ToStrError),
    #[error("Mime FromStr error: {0}")]
    FromStr(#[from] mime::FromStrError),

    #[error("Serde Json error: {0}")]
    SerdeJson(#[from] serde_json::Error),

    #[error("Url parse error: {0}")]
    UrlParse(#[from] url::ParseError),
}

pub fn set_etag(res: &mut AxumResponse<AxumBody>, etag: &str) -> Result<(), InvalidHeaderValue> {
    if !etag.is_empty() {
        if etag.starts_with("\"") {
            res.headers_mut().insert(ETAG, HeaderValue::from_str(etag)?);
        } else {
            res.headers_mut()
                .insert(ETAG, HeaderValue::from_str(format!("\"{etag}\"").as_str())?);
        }
    }
    Ok(())
}

pub fn get_etag(headers: &HeaderMap) -> Result<String, HttpError> {
    let etag = match headers.get(ETAG) {
        Some(etag) => etag
            .to_str()?
            .trim_start_matches("\"")
            .trim_end_matches("\"")
            .to_string(),
        None => String::new(),
    };

    Ok(etag)
}

pub async fn read_url(file_url: &str, offset: i64, size: u32) -> Result<Bytes, HttpError> {
    let mut headers = HeaderMap::new();
    headers.insert(
        RANGE,
        HeaderValue::from_str(&format!("bytes={offset}-{}", offset + size as i64))?,
    );

    let response = request(
        file_url,
        Method::GET,
        None,
        None::<Bytes>,
        Some(headers),
        None,
    )
    .await?;
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
        HeaderValue::from_str("nosniff")?,
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

pub async fn ranges_mime_size(
    ranges: &[HttpRange],
    content_type: &str,
    content_size: u64,
) -> Result<u64, HttpError> {
    let mut form = Form::new();
    let mut encode_size = 0;
    for ra in ranges {
        let part = Part::bytes(&[])
            .mime_str(content_type)?
            .headers(range_header(*ra, content_size)?);
        let name = format!("{}-{}", ra.start, ra.start + ra.length - 1);
        let mut stream = form.part_stream(name, part);
        while let Some(Ok(data)) = stream.next().await {
            encode_size += data.len() as u64;
        }

        encode_size += ra.length;
    }
    Ok(encode_size)
}

pub fn range_header(range: HttpRange, content_size: u64) -> Result<HeaderMap, HttpError> {
    let mut headers = HeaderMap::new();
    headers.insert(
        CONTENT_RANGE,
        HeaderValue::from_str(&content_range(range, content_size))?,
    );
    Ok(headers)
}
