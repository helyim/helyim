use axum::{extract::State, response::Response};
use hyper::Body;

use super::FilerRef;

use crate::{errors::Result, util::http::extractor::{GetOrHeadExtractor, PostExtractor, DeleteExtractor}};

#[derive(Clone)]
pub struct FilerState {
    pub filer: FilerRef,
    pub read_redirect: bool,
}

pub async fn get_or_head_handler(
    State(ctx): State<FilerState>,
    extractor: GetOrHeadExtractor,
) -> Result<Response<Body>> {
    todo!()
}

pub async fn post_handler(
    State(ctx): State<FilerState>,
    extractor: PostExtractor,
) -> Result<Response<Body>> {
    todo!()
}

pub async fn delete_handler(
    State(ctx): State<FilerState>,
    extractor: DeleteExtractor,
) -> Result<Response<Body>> {
    todo!()
}
