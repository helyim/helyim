use std::time::Duration;

use bytes::Bytes;
use hyper::{body::to_bytes, header::CONTENT_LENGTH, Body, Client, Method, Request};
use url::Url;

use crate::errors::Result;

pub async fn delete(url: &str, params: &Vec<(&str, &str)>) -> Result<Bytes> {
    request(url, params, Method::DELETE, None).await
}

pub async fn get(url: &str, params: &Vec<(&str, &str)>) -> Result<Bytes> {
    request(url, params, Method::GET, None).await
}

pub async fn post(url: &str, params: &Vec<(&str, &str)>, body: &[u8]) -> Result<Bytes> {
    request(url, params, Method::POST, Some(body)).await
}

async fn request(
    url: &str,
    params: &Vec<(&str, &str)>,
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

    let client = Client::builder()
        .pool_idle_timeout(Duration::from_secs(30))
        .build_http();
    let mut response = client.request(request).await?;

    let body = to_bytes(response.body_mut()).await?;
    Ok(body)
}
