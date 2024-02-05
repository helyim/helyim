// use std::collections::HashMap;

use axum::response::Response;
use helyim_proto::filer::FileChunk;
use hyper::Body;

use crate::{
    client::MasterClient, errors::Result, filer::file_chunks::view_from_chunks, util::http::get,
};

pub async fn stream_content(
    master_client: &MasterClient,
    response: &mut Response<Body>,
    chunks: &mut [FileChunk],
    offset: i64,
    size: u64,
) -> Result<()> {
    let chunk_views = view_from_chunks(chunks, offset, size);

    // let mut file_id_to_url = HashMap::new();

    for chunk_view in chunk_views {
        let url = master_client.lookup_file_id(&chunk_view.file_id)?;
        // file_id_to_url.insert(chunk_view.file_id, url);
        let mut params = Vec::new();
        let end = chunk_view.offset + size as i64;
        let range = format!("bytes={}-{end}", chunk_view.offset);
        params.push(("Range", range.as_str()));
        let body = get(&url, &params).await?;

        *response.body_mut() = Body::from(body);
    }

    Ok(())
}
