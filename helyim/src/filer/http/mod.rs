use std::{collections::HashMap, path::PathBuf};

use axum::{
    body::Body,
    extract::State,
    http::{
        header::{ACCEPT_RANGES, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE},
        HeaderValue, Method, Response,
    },
};
use bytes::{Bytes, BytesMut};
use faststr::FastStr;
use futures_util::StreamExt;
use http_range::HttpRange;
use hyper::StatusCode;
use nom::Slice;
use reqwest::multipart::{Form, Part};
use rustix::path::Arg;
use tonic::service::AxumBody;
use tracing::{error, info, warn};

mod extractor;

use crate::{
    filer::{
        entry::Entry,
        file_chunk::{etag, total_size, ChunkView},
        http::extractor::{GetOrHeadExtractor, ListDir},
        FilerError, FilerRef,
    },
    util::{
        args::FilerOptions,
        file::file_name,
        http::{
            content_range, http_error, mime_header, ranges_mime_size, read_url, request, set_etag,
            HttpError,
        },
    },
};

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
        let file_id = entry.chunks[0].fid.unwrap_or_default();
        let mut url = match self
            .filer
            .master_client
            .lookup_file_id(&file_id.to_fid_str())
        {
            Ok(url) => url,
            Err(err) => {
                error!(
                    "lookup file id: {} failed, error: {err}",
                    file_id.to_fid_str()
                );
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

        match request(
            &url,
            extractor.method,
            &[],
            extractor.body,
            extractor.headers,
        )
        .await
        {
            Ok(resp) => {
                *response.status_mut() = resp.status();
                *response.headers_mut() = resp.headers().clone();
                *response.body_mut() = Body::from(resp.bytes().await.map_err(HttpError::Reqwest)?);
                Ok(())
            }
            Err(err) => {
                error!("failing to connect to volume server: {url}, error: {err}");
                Err(FilerError::Http(err))
            }
        }
    }

    async fn handle_chunks(
        &self,
        extractor: GetOrHeadExtractor,
        response: &mut Response<Body>,
        entry: &mut Entry,
    ) -> Result<(), FilerError> {
        let mut mime_type = entry.mime.to_string();
        if mime_type.is_empty() {
            let path = PathBuf::from(&entry.full_path);
            if let Some(ext) = path.extension() {
                if !ext.is_empty() {
                    mime_type = mime_guess::from_ext(ext.as_str()?)
                        .first_or_octet_stream()
                        .to_string();
                }
            }
        }

        if !mime_type.is_empty() {
            response.headers_mut().insert(
                CONTENT_TYPE,
                HeaderValue::from_str(&mime_type).map_err(HttpError::InvalidHeaderValue)?,
            );
        }

        set_etag(response, &etag(entry.chunks.as_slice()))
            .map_err(HttpError::InvalidHeaderValue)?;

        let total_size = total_size(entry.chunks.as_slice());

        let range = match extractor.headers.get("Range") {
            Some(range) => range,
            None => return Ok(()),
        };

        if range.is_empty() {
            response
                .headers_mut()
                .insert(CONTENT_LENGTH, HeaderValue::from(total_size));

            match self.write_content(entry, 0, total_size).await {
                Ok(data) => {
                    *response.body_mut() = AxumBody::from(data);
                }
                Err(err) => {
                    http_error(response, StatusCode::INTERNAL_SERVER_ERROR, err.to_string())?;
                }
            }
            return Ok(());
        }
        let ranges = match HttpRange::parse(range.to_str().map_err(HttpError::ToStr)?, total_size) {
            Ok(ranges) => ranges,
            Err(_) => {
                http_error(
                    response,
                    StatusCode::RANGE_NOT_SATISFIABLE,
                    "Http range parsed error".to_string(),
                )?;
                return Ok(());
            }
        };
        let ranges_sum: u64 = ranges.iter().map(|r| r.length).sum();
        if ranges_sum > total_size {
            return Ok(());
        }

        if ranges.is_empty() {
            return Ok(());
        }

        if ranges.len() == 1 {
            let ra = ranges[0];
            response
                .headers_mut()
                .insert(CONTENT_LENGTH, HeaderValue::from(ra.length));
            response.headers_mut().insert(
                CONTENT_RANGE,
                HeaderValue::from_str(&content_range(ra, total_size))
                    .map_err(HttpError::InvalidHeaderValue)?,
            );
            *response.status_mut() = StatusCode::PARTIAL_CONTENT;

            if let Err(err) = self.write_content(entry, ra.start as i64, ra.length).await {
                http_error(response, StatusCode::INTERNAL_SERVER_ERROR, err.to_string())?;
            }
            return Ok(());
        }

        // process multiple ranges
        for ra in ranges.iter() {
            if ra.start > total_size {
                http_error(
                    response,
                    StatusCode::RANGE_NOT_SATISFIABLE,
                    "Out Of Range".to_string(),
                )?;
                return Ok(());
            }
        }

        let send_size = ranges_mime_size(&ranges, &mime_type, total_size)?;

        let mut form = Form::new();
        response.headers_mut().insert(
            CONTENT_TYPE,
            HeaderValue::from_str(&format!(
                "multipart/byteranges; boundary={}",
                form.boundary()
            ))
            .map_err(HttpError::InvalidHeaderValue)?,
        );

        let (_, outputs) = async_scoped::TokioScope::scope_and_block(|s| {
            for range in ranges.iter() {
                s.spawn(async {
                    let headers = match mime_header(*range, &mime_type, total_size) {
                        Ok(headers) => headers,
                        Err(err) => {
                            error!("create mime headers error: {err}");
                            return Err(FilerError::Http(err));
                        }
                    };
                    let data = match self
                        .write_content(entry, range.start as i64, range.length)
                        .await
                    {
                        Ok(data) => data,
                        Err(err) => {
                            error!("write content error: {err}");
                            return Err(err);
                        }
                    };
                    let part = Part::bytes(data.to_vec()).headers(headers);
                    let name = format!("{}-{}", range.start, range.length);

                    Ok((name, part))
                });
            }
        });

        let mut body = BytesMut::new();
        for output in outputs {
            let (name, part) = output??;
            let mut stream = form.part_stream(name, part);
            while let Some(Ok(data)) = stream.next().await {
                body.extend_from_slice(&data);
            }
        }

        if response.headers().get(CONTENT_ENCODING).is_none() {
            response
                .headers_mut()
                .insert(CONTENT_LENGTH, HeaderValue::from(send_size));
        }

        *response.status_mut() = StatusCode::PARTIAL_CONTENT;
        *response.body_mut() = Body::from(body.freeze());

        Ok(())
    }

    async fn write_content(
        &self,
        entry: &Entry,
        offset: i64,
        size: u64,
    ) -> Result<Bytes, FilerError> {
        let chunk_views = ChunkView::view_from_chunks(entry.chunks.as_slice(), offset, size);

        let mut file_id_map = HashMap::new();

        for chunk_view in chunk_views.iter() {
            let url = self
                .filer
                .master_client
                .lookup_file_id(&chunk_view.fid.to_fid_str())?;
            file_id_map.insert(chunk_view.fid, url);
        }

        let mut buf = BytesMut::new();
        for chunk_view in chunk_views.iter() {
            if let Some(url) = file_id_map.get(&chunk_view.fid) {
                buf.extend_from_slice(&read_url(url, chunk_view.offset, chunk_view.size).await?);
            }
        }

        Ok(buf.freeze())
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
    match state.filer.find_entry(path).await {
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
        Ok(Some(mut entry)) => {
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

            if entry.chunks.len() == 1 {
                state
                    .handle_single_chunk(extractor, &mut response, &entry)
                    .await?;
            } else {
                state
                    .handle_chunks(extractor, &mut response, &mut entry)
                    .await?;
            }
        }
    }
    Ok(response)
}
