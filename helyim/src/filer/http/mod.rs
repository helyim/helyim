use axum::{
    body::Body,
    extract::State,
    http::{
        header::{ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_TYPE},
        HeaderValue, Method, Response,
    },
    response::IntoResponse,
};
use faststr::FastStr;
use hyper::StatusCode;
use nom::Slice;
use tracing::{error, info, warn};

mod extractor;

use crate::{
    filer::{
        entry::Entry,
        file_chunk::total_size,
        http::extractor::{GetOrHeadExtractor, ListDir},
        FilerError, FilerRef,
    },
    util::{
        args::FilerOptions,
        file::file_name,
        http::{request, HttpError},
    },
};

const TOPICS_DIR: &str = "/topics";

#[derive(Clone)]
pub struct FilerState {
    pub filer: FilerRef,
    pub options: FilerOptions,
}

impl FilerState {
    pub async fn list_directory_handler(
        &self,
        extractor: GetOrHeadExtractor,
    ) -> Result<Response<Body>, FilerError> {
        let mut path = extractor.uri.path();
        if path.ends_with('/') && path.len() > 1 {
            path = &path[..path.len() - 1];
        }

        let mut limit = extractor.list_dir.limit;
        if limit == 0 {
            limit = 100;
        }

        let mut last_filename = extractor.list_dir.last_file_name;
        match self
            .filer
            .list_directory_entries(path, &last_filename, false, limit)
            .await
        {
            Ok(entries) => {
                let should_display_load_more = limit as usize == entries.len();
                if path == "/" {
                    path = "";
                }

                if !entries.is_empty() {
                    if let Some(path) = file_name(&entries[entries.len() - 1].full_path) {
                        last_filename = FastStr::new(path);
                    }
                }

                info!(
                    "list directory {path}, last file: {last_filename}, limit: {limit}, {} items",
                    entries.len()
                );

                let response = Response::builder().header(CONTENT_TYPE, "application/json");
                let body = serde_json::to_vec(&ListDir {
                    path: FastStr::new(path),
                    entries,
                    limit,
                    last_filename,
                    should_display_load_more,
                })?;

                Ok(response
                    .body(Body::from(body))
                    .map_err(HttpError::AxumHttp)?)
            }
            Err(err) => {
                info!(
                    "list directory {path} error, last file: {last_filename}, limit: {limit}, \
                     {err}"
                );
                Err(FilerError::NotFound)
            }
        }
    }

    async fn handle_single_chunk(
        &self,
        extractor: GetOrHeadExtractor,
        response: &mut Response<Body>,
        entry: &Entry,
    ) -> Result<(), FilerError> {
        let file_id = entry.chunks[0].get_fid();
        let mut url = match self.filer.master_client.lookup_file_id(&file_id) {
            Ok(url) => url,
            Err(err) => {
                error!("lookup file id: {file_id} failed, error: {err}");
                *response.status_mut() = StatusCode::NOT_FOUND;
                return Ok(());
            }
        };

        if self.options.redirect_on_read {
            warn!("read redirect is not supported.");
            *response.status_mut() = StatusCode::NOT_IMPLEMENTED;
            return Ok(());
        }

        if let Some(query) = extractor.uri.query() {
            url = format!("{url}?{query}");
        }

        let resp = request(
            url,
            extractor.method,
            &[],
            extractor.body,
            extractor.headers,
        )
        .await?;

        *response.status_mut() = resp.status();

        for (name, value) in resp.headers() {
            response.headers_mut().insert(name.clone(), value.clone());
        }

        *response.body_mut() = Body::from(resp.bytes().await.map_err(HttpError::Reqwest)?);

        todo!()
    }
}

pub async fn get_or_head_handler(
    State(state): State<FilerState>,
    extractor: GetOrHeadExtractor,
) -> Result<Response<Body>, FilerError> {
    let mut path = extractor.uri.path();
    if path.ends_with("/") && path.len() > 1 {
        path = path.slice(..path.len() - 1);
    }

    let mut response = Response::new(Body::empty());
    match state.filer.find_entry(&path).await {
        Err(err) => {
            if path == "/" {
                return state.list_directory_handler(extractor).await;
            }
            info!("Path {path} is not found, error: {err}");
            *response.status_mut() = StatusCode::NOT_FOUND;
            return Ok(response);
        }
        Ok(None) => {
            if path == "/" {
                return state.list_directory_handler(extractor).await;
            }
            info!("Path {path} is not found");
            *response.status_mut() = StatusCode::NOT_FOUND;
            return Ok(response);
        }
        Ok(Some(entry)) => {
            if entry.is_directory() {
                if state.options.disable_dir_listing {
                    *response.status_mut() = StatusCode::METHOD_NOT_ALLOWED;
                    return Ok(response);
                }
                return state.list_directory_handler(extractor).await;
            }

            if entry.chunks.is_empty() {
                info!("no file chunks for {path}, attr = {:?}", entry.attr);
                *response.status_mut() = StatusCode::NO_CONTENT;
                return Ok(response);
            }

            response
                .headers_mut()
                .insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
            if extractor.method == Method::HEAD {
                let len = total_size(entry.chunks.as_slice());
                response
                    .headers_mut()
                    .insert(CONTENT_LENGTH, HeaderValue::from(len));
                return Ok(response);
            }

            if entry.chunks.len() == 1 {}
        }
    }
    todo!()
}

pub async fn healthz_handler(
    State(state): State<FilerState>,
) -> Result<Response<Body>, FilerError> {
    let filer_store = state.filer.filer_store()?;
    match filer_store.find_entry(TOPICS_DIR).await {
        Err(err) if matches!(err, FilerError::NotFound) => {
            warn!("filerHealthzHandler FindEntry: {}", err);
            Ok(StatusCode::SERVICE_UNAVAILABLE.into_response())
        }
        _ => Ok(StatusCode::OK.into_response()),
    }
}
