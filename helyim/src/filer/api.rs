use axum::{extract::State, http::HeaderValue, response::Response};
use faststr::FastStr;
use hyper::{
    header::{
        ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, ETAG, LAST_MODIFIED, LOCATION,
        RANGE,
    },
    Body, Method, StatusCode,
};

use super::{
    entry::Entry,
    file_chunks::{etag, total_size},
    stream::stream_content,
    util::{parse_range, sum_ranges_size},
    FilerRef,
};
use crate::{
    errors::Result,
    storage::FileId,
    util::{
        file::guess_mimetype,
        http::{
            extractor::{DeleteExtractor, GetOrHeadExtractor, PostExtractor},
            get,
        },
        time::get_time,
    },
};

#[derive(Clone)]
pub struct FilerState {
    pub filer: FilerRef,
    pub read_redirect: bool,
    pub disable_dir_listing: bool,
}

pub async fn get_or_head_handler(
    State(ctx): State<FilerState>,
    extractor: GetOrHeadExtractor,
) -> Result<Response<Body>> {
    let mut response = Response::new(Body::empty());
    let path = extractor.uri.path();
    if let Some(mut entry) = ctx.filer.find_entry(path).await? {
        if entry.is_directory() {
            if ctx.disable_dir_listing {
                *response.status_mut() = StatusCode::METHOD_NOT_ALLOWED;
                return Ok(response);
            }

            // todo: list dir
        }

        if entry.chunks.is_empty() {
            *response.status_mut() = StatusCode::NO_CONTENT;
            return Ok(response);
        }

        response
            .headers_mut()
            .insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
        response.headers_mut().insert(
            CONTENT_LENGTH,
            HeaderValue::from(entry.chunks.iter().fold(0, |acc, x| acc + x.size)),
        );
        response.headers_mut().insert(
            LAST_MODIFIED,
            HeaderValue::from(
                get_time(entry.attr.mtime)
                    .expect("SystemTime before UNIX EPOCH")
                    .as_secs(),
            ),
        );
        // fill etag
        set_etag(&mut response, etag(&entry.chunks))?;

        if extractor.method == Method::HEAD {
            return Ok(response);
        }

        if entry.chunks.len() == 1 {
            handle_single_chunk(&ctx, &entry, &mut response).await?;
            return Ok(response);
        } else {
            handle_multiple_chunks(&ctx, &extractor, &mut entry, &mut response).await?;
            return Ok(response);
        }
    } else {
        *response.status_mut() = StatusCode::NOT_FOUND;
        return Ok(response);
    }
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

pub async fn handle_single_chunk(
    ctx: &FilerState,
    entry: &Entry,
    response: &mut Response<Body>,
) -> Result<()> {
    assert_eq!(
        entry.chunks.len(),
        1,
        "entry chunks len not is 1, len: {}",
        entry.chunks.len()
    );
    if let Some(file_id) = &entry.chunks[0].fid {
        let url = ctx
            .filer
            .master_client
            .lookup_file_id(&format!("{}", FileId::from(file_id)))?;

        if ctx.read_redirect {
            response
                .headers_mut()
                .insert(LOCATION, HeaderValue::from_str(&url)?);
            *response.status_mut() = StatusCode::FOUND;

            return Ok(());
        }

        let body = get(&url, &[]).await?;
        *response.body_mut() = Body::from(body);
        *response.status_mut() = StatusCode::ACCEPTED;

        return Ok(());
    }

    Ok(())
}

pub async fn handle_multiple_chunks(
    ctx: &FilerState,
    extractor: &GetOrHeadExtractor,
    entry: &mut Entry,
    response: &mut Response<Body>,
) -> Result<()> {
    let mut mime_type = entry.attr.mime.clone();
    if mime_type.is_empty() {
        mime_type = guess_mimetype(&entry.path);
    }

    if !mime_type.is_empty() {
        response
            .headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_str(&mime_type)?);
    }

    let total_size = total_size(&entry.chunks);
    if let Some(header_range) = extractor.headers.get(RANGE) {
        let ranges = parse_range(header_range, total_size)?;
        if sum_ranges_size(&ranges) > total_size {
            // The total number of bytes in all the ranges
            // is larger than the size of the file by
            // itself, so this is probably an attack, or a
            // dumb client.  Ignore the range request.
            return Ok(());
        }

        if ranges.len() == 0 {
            return Ok(());
        } else if ranges.len() == 1 {
            let ra = &ranges[0];
            response
                .headers_mut()
                .insert(CONTENT_LENGTH, HeaderValue::from(ra.length));
            response.headers_mut().insert(
                CONTENT_RANGE,
                HeaderValue::from_str(&ra.content_range(total_size)).unwrap(),
            );
            *response.status_mut() = StatusCode::PARTIAL_CONTENT;
            stream_content(
                &ctx.filer.master_client,
                response,
                &mut entry.chunks,
                ra.start as i64,
                ra.length,
            ).await?;

            return Ok(());
        }
    } else {
        response
            .headers_mut()
            .insert(CONTENT_LENGTH, HeaderValue::from(total_size));
        stream_content(
            &ctx.filer.master_client,
            response,
            &mut entry.chunks,
            0,
            total_size,
        )
        .await?;
        return Ok(());
    }
    Ok(())
}

pub fn set_etag(response: &mut Response<Body>, etag: FastStr) -> Result<()> {
    if !etag.is_empty() {
        if etag.starts_with("\"") {
            response
                .headers_mut()
                .insert(ETAG, HeaderValue::from_str(&etag)?);
        } else {
            response
                .headers_mut()
                .insert(ETAG, HeaderValue::from_str(&format!("\"{}\"", etag))?);
        }
    }

    Ok(())
}
