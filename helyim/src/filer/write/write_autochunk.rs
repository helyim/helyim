use axum::response::Response;
use faststr::FastStr;
use hyper::{header::CONTENT_LENGTH, Body, Method};
use tracing::info;

use crate::{errors::Result, filer::api::FilerState, util::http::extractor::PostExtractor};

pub fn auto_chunk(
    ctx: &FilerState,
    extractor: &PostExtractor,
    replication: &FastStr,
    collection: &FastStr,
    data_center: &FastStr,
    response: &mut Response<Body>,
) -> Result<bool> {
    if extractor.method != Method::POST {
        info!(
            "AutoChunking not supported for method: {}",
            extractor.method
        );
        return Ok(false);
    }

    let max_mb = extractor.query.max_mb.map_or(ctx.options.max_mb, |m| m);
    if max_mb <= 0 {
        info!("AutoChunking not enabled");
        return Ok(false);
    }
    info!("AutoChunking level set to {max_mb} (MB)");

    let chunk_size = (1024 * 1024 * max_mb) as usize;
    let mut content_length = 0;

    if let Some(content_length_header) = extractor.headers.get(CONTENT_LENGTH) {
        if let Ok(content_length_header) = content_length_header.to_str()?.parse::<usize>() {
            if content_length_header < chunk_size {
                info!(
                    "Content-Length of, {content_length_header}, is less than the chunk size of, \
                     {chunk_size}, so autoChunking will be skipped."
                );
                return Ok(false);
            }
            content_length = content_length_header;
        }
    }
    if content_length == 0 {
        info!("Content-Length value is missing or unexpected so autoChunking will be skipped.");
        return Ok(false);
    }

    todo!()
}

fn do_auto_chunk(
    ctx: &FilerState,
    extractor: &PostExtractor,
    content_length: &usize,
    chunk_size: &usize,
    replication: &FastStr,
    collection: &FastStr,
    data_center: &FastStr,
    response: &mut Response<Body>,
) -> Result<()> {
    // let current_len = extractor.body.len();
    // while current_len > 0 {
    //     let chunk_len = if current_len >= *chunk_size {
    //         chunk_size
    //     } else {
    //         &current_len
    //     };
    // }

    todo!()
}
