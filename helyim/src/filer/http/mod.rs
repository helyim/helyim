use axum::{body::Body, extract::State, http::Response};
use nom::Slice;

mod extractor;

use crate::filer::{http::extractor::GetOrHeadExtractor, FilerError, FilerRef};

#[derive(Clone)]
pub struct FilerState {
    pub filer: FilerRef,
}

impl FilerState {
    pub async fn list_directory_handler(
        &self,
        extractor: GetOrHeadExtractor,
    ) -> Result<Response<Body>, FilerError> {
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

    // match state.filer.find_entry(&path) {
    //     Err(err) => {
    //         if path == "/" {
    //             state.list_directory_handler(extractor).await?;
    //         }
    //     }
    // }
    todo!()
}
