use axum::http::{HeaderMap, Uri};
use axum_macros::FromRequest;

#[derive(Debug, FromRequest)]
pub struct GetOrHeadExtractor {
    pub uri: Uri,
    pub headers: HeaderMap,
}
