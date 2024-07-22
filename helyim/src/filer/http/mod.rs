use axum::{body::Body, extract::State, http::Response, response::IntoResponse};
use hyper::StatusCode;
use nom::Slice;
use tracing::warn;

mod extractor;

use crate::filer::{http::extractor::GetOrHeadExtractor, FilerError, FilerRef};

const TOPICS_DIR: &str = "/topics";

#[derive(Clone)]
pub struct FilerState {
    pub filer: FilerRef,
}

impl FilerState {
    pub async fn list_directory_handler(
        &self,
        _extractor: GetOrHeadExtractor,
    ) -> Result<Response<Body>, FilerError> {
        todo!()
    }
}

pub async fn get_or_head_handler(
    State(_state): State<FilerState>,
    extractor: GetOrHeadExtractor,
) -> Result<Response<Body>, FilerError> {
    let mut _path = extractor.uri.path();
    if _path.ends_with("/") && _path.len() > 1 {
        _path = _path.slice(.._path.len() - 1);
    }

    // match state.filer.find_entry(&path) {
    //     Err(err) => {
    //         if path == "/" {
    //             state.list_directory_handler(extractor).await?;
    //         }
    //     }
    // }
    todo!()
}

pub async fn healthz_handler(State(state): State<FilerState>) -> Result<Response<Body>, FilerError> {
    let filer_store = state.filer.filer_store()?;
    match filer_store.find_entry(TOPICS_DIR).await {
        Err(err) if matches!(err, FilerError::NotFound) => {
            warn!("filerHealthzHandler FindEntry: {}", err);
            Ok(StatusCode::SERVICE_UNAVAILABLE.into_response())
        }
        _ => Ok(StatusCode::OK.into_response())
    }
}
