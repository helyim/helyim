use std::{sync::OnceLock, time::Duration};

use axum::{
    extract::{FromRequest, Query, Request},
    http::{header::CONTENT_TYPE, Method, StatusCode},
    response::{Html, IntoResponse},
    Form, Json, RequestExt,
};
use bytes::Bytes;
use once_cell::sync::Lazy;
use reqwest::{Body, Client, ClientBuilder};
use serde::de::DeserializeOwned;
use tracing::error;
use url::Url;

use crate::{errors::Result, images::FAVICON_ICO, PHRASE};

pub static HTTP_CLIENT: Lazy<HttpClient> = Lazy::new(HttpClient::new);

pub struct HttpClient {
    client: OnceLock<Client>,
}

impl HttpClient {
    pub fn new() -> Self {
        Self {
            client: OnceLock::new(),
        }
    }

    fn try_init(&self) -> Result<&Client> {
        Ok(self.client.get_or_init(|| {
            ClientBuilder::new()
                .pool_idle_timeout(Duration::from_secs(30))
                .connect_timeout(Duration::from_secs(10))
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap()
        }))
    }

    pub async fn get<U: AsRef<str>>(&self, url: U, params: &[(&str, &str)]) -> Result<Bytes> {
        let url = Url::parse_with_params(url.as_ref(), params)?;
        Ok(self.try_init()?.get(url).send().await?.bytes().await?)
    }

    pub async fn post<U: AsRef<str>, B: Into<Body>>(
        &self,
        url: U,
        params: &[(&str, &str)],
        body: B,
    ) -> Result<Bytes> {
        let url = Url::parse_with_params(url.as_ref(), params)?;
        Ok(self
            .try_init()?
            .post(url)
            .body(body)
            .send()
            .await?
            .bytes()
            .await?)
    }

    pub async fn delete<U: AsRef<str>>(&self, url: U, params: &[(&str, &str)]) -> Result<Bytes> {
        let url = Url::parse_with_params(url.as_ref(), params)?;
        Ok(self.try_init()?.delete(url).send().await?.bytes().await?)
    }
}

pub struct FormOrJson<T>(pub T);

#[async_trait::async_trait]
impl<T, S> FromRequest<S> for FormOrJson<T>
where
    Json<T>: FromRequest<S>,
    Form<T>: FromRequest<S>,
    T: DeserializeOwned + 'static,
    S: Send + Sync,
{
    type Rejection = axum::response::Response;

    async fn from_request(req: Request, _state: &S) -> std::result::Result<Self, Self::Rejection> {
        match req.method() {
            &Method::GET | &Method::HEAD => {
                let Query(payload) = req
                    .extract::<Query<T>, _>()
                    .await
                    .map_err(|err| err.into_response())?;

                return Ok(Self(payload));
            }
            &Method::POST => {
                let content_type = req
                    .headers()
                    .get(CONTENT_TYPE)
                    .and_then(|value| value.to_str().ok())
                    .ok_or_else(|| StatusCode::BAD_REQUEST.into_response())?;

                if content_type.starts_with("application/json") {
                    let Json(payload) = req
                        .extract::<Json<T>, _>()
                        .await
                        .map_err(|err| err.into_response())?;

                    return Ok(Self(payload));
                } else if content_type.starts_with("application/x-www-form-urlencoded") {
                    let Form(payload) = req
                        .extract::<Form<T>, _>()
                        .await
                        .map_err(|err| err.into_response())?;

                    return Ok(Self(payload));
                } else {
                    error!(
                        "invalid http request: url: {}, method: {}, content type: {content_type}",
                        req.uri(),
                        req.method()
                    );
                }
            }
            _ => error!(
                "invalid http request: url: {}, method: {}",
                req.uri(),
                req.method()
            ),
        }

        Err(StatusCode::BAD_REQUEST.into_response())
    }
}

pub async fn default_handler() -> Html<&'static str> {
    Html(PHRASE)
}

pub async fn favicon_handler<'a>() -> &'a [u8] {
    FAVICON_ICO.bytes()
}
