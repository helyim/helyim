use std::{
    collections::HashMap,
    io::{ErrorKind, Read},
    path::PathBuf,
    time::SystemTime,
};

use axum::{
    body::Body,
    extract::State,
    http::{
        header::{
            ACCEPT_RANGES, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, RANGE,
        },
        HeaderValue, Method, Response,
    },
};
use bytes::{Buf, Bytes, BytesMut};
use faststr::FastStr;
use futures_util::StreamExt;
use helyim_common::{
    anyhow,
    file::file_name,
    http::{
        content_range, get_etag, http_error, range_header, ranges_mime_size, read_url, request,
        set_etag, trim_trailing_slash, HttpError,
    },
    operation::{upload, UploadResult},
    time::now,
};
use helyim_proto::filer::FileChunk;
use http_range::HttpRange;
use hyper::StatusCode;
use multer::Multipart;
use reqwest::{
    multipart::{Form, Part},
    Url,
};
use rustix::{
    path::Arg,
    process::{getgid, getuid},
};
use tracing::{error, info, warn};

use crate::{
    entry::{Attr, Entry},
    file_chunk::{etag, total_size, ChunkView},
    filer::{FilerError, FilerRef},
    http::extractor::{
        parse_boundary, DeleteExtractor, FilerPostResult, GetOrHeadExtractor, ListDir,
        PostExtractor,
    },
    operation::{assign, AssignRequest},
    FilerOptions,
};

mod extractor;

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
        let mut path = trim_trailing_slash(extractor.uri.path());

        let mut limit = extractor.list_dir.limit.unwrap_or(100);
        if limit == 0 {
            limit = 100;
        }

        let mut last_filename = extractor.list_dir.last_file_name.unwrap_or_default();
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
        let file_id = &entry.chunks[0].fid;
        let mut url = match self.filer.master_client.lookup_file_id(file_id) {
            Ok(url) => url,
            Err(err) => {
                error!("lookup file id: {} failed, error: {err}", file_id);
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
            None,
            Some(extractor.body),
            Some(extractor.headers),
            None,
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
        entry: &Entry,
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

        let range = match extractor.headers.get(RANGE) {
            Some(range) => range,
            None => return Ok(()),
        };

        if range.is_empty() {
            response
                .headers_mut()
                .insert(CONTENT_LENGTH, HeaderValue::from(total_size));

            match self.write_content(entry, 0, total_size).await {
                Ok(data) => {
                    *response.body_mut() = Body::from(data);
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
            // The total number of bytes in all the ranges
            // is larger than the size of the file by
            // itself, so this is probably an attack, or a
            // dumb client.  Ignore the range request.
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

        let send_size = ranges_mime_size(&ranges, &mime_type, total_size).await?;

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
                    let headers = match range_header(*range, total_size) {
                        Ok(headers) => headers,
                        Err(err) => {
                            error!("create range headers error: {err}");
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
                    let part = Part::bytes(data.to_vec())
                        .mime_str(&mime_type)
                        .map_err(HttpError::Reqwest)?
                        .headers(headers);
                    let name = format!("{}-{}", range.start, range.start + range.length - 1);

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
            let url = self.filer.master_client.lookup_file_id(&chunk_view.fid)?;
            file_id_map.insert(&chunk_view.fid, url);
        }

        let mut buf = BytesMut::new();
        for chunk_view in chunk_views.iter() {
            if let Some(url) = file_id_map.get(&chunk_view.fid) {
                buf.extend_from_slice(&read_url(url, chunk_view.offset, chunk_view.size).await?);
            }
        }

        Ok(buf.freeze())
    }

    async fn auto_chunk(
        &self,
        response: &mut Response<Body>,
        extractor: &mut PostExtractor,
        replication: FastStr,
        collection: FastStr,
        data_center: FastStr,
    ) -> Result<bool, FilerError> {
        if extractor.method != Method::POST {
            info!(
                "auto chunking not supported for method {}",
                extractor.method
            );
            return Ok(false);
        }

        let mut max_mb = extractor.query.max_mb.unwrap_or_default();
        if max_mb <= 0 && self.options.max_mb > 0 {
            max_mb = self.options.max_mb;
        }
        if max_mb <= 0 {
            info!("auto chunking is not enabled");
            return Ok(false);
        }
        info!("auto chunking level set to {max_mb} (MB)");

        let chunk_size = 1024 * 1024 * max_mb;
        let mut content_len = 0;

        if let Some(len) = extractor.headers.get(CONTENT_LENGTH) {
            content_len = len.to_str().map_err(HttpError::ToStr)?.parse()?;
            if content_len <= chunk_size {
                info!("content-length: {content_len} is less than the chunk size: {chunk_size}");
                return Ok(false);
            }
        }
        if content_len <= 0 {
            info!("content-length value is missing or unexpected, auto chunking will be skipped");
            http_error(
                response,
                StatusCode::LENGTH_REQUIRED,
                "Content-Length is required for auto-chunking".to_string(),
            )?;
            return Ok(false);
        }

        match self
            .do_auto_chunk(
                response,
                extractor,
                content_len as u64,
                chunk_size as u32,
                replication,
                collection,
                data_center,
            )
            .await
        {
            Ok(reply) => {
                *response.status_mut() = StatusCode::CREATED;
                *response.body_mut() = Body::from(serde_json::to_vec(&reply)?);
            }
            Err(err) => {
                *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                *response.body_mut() = Body::from(err.to_string());
            }
        }
        Ok(true)
    }

    #[allow(clippy::too_many_arguments)]
    async fn do_auto_chunk(
        &self,
        response: &mut Response<Body>,
        extractor: &mut PostExtractor,
        content_len: u64,
        chunk_size: u32,
        replication: FastStr,
        collection: FastStr,
        data_center: FastStr,
    ) -> Result<FilerPostResult, FilerError> {
        let boundary = parse_boundary(&extractor.headers)?;
        let mut multipart = Multipart::new(
            Body::from(extractor.body.clone()).into_data_stream(),
            boundary,
        );
        let part = match multipart.next_field().await {
            Ok(Some(part)) => part,
            Ok(None) => {
                error!("get nothing from multipart");
                return Err(anyhow!("get nothing from multipart"));
            }
            Err(err) => {
                error!("get multipart error: {err}");
                return Err(HttpError::Multer(err).into());
            }
        };

        let filename = match part.file_name() {
            Some(name) => name.to_string(),
            None => String::new(),
        };

        let mut file_chunks = vec![];
        let mut total_bytes_read = 0;
        let tmp_buffer_size = 2048;
        let mut tmp_buffer = vec![0u8; tmp_buffer_size];
        let mut chunk_buf = vec![0u8; chunk_size as usize + tmp_buffer_size];
        let mut chunk_buf_offset = 0;
        let mut chunk_offset = 0;

        let mut read_fully = false;
        let mut read_err = None;

        let mut result = FilerPostResult {
            name: filename.clone(),
            ..Default::default()
        };

        let part_bytes = part.bytes().await.map_err(HttpError::Multer)?;
        let mut part_reader = part_bytes.reader();

        while total_bytes_read < content_len {
            tmp_buffer.clear();

            let bytes_read = match part_reader.read(&mut tmp_buffer) {
                Ok(bytes_read) => bytes_read,
                Err(err) => {
                    if err.kind() == ErrorKind::UnexpectedEof {
                        read_fully = true;
                    }
                    read_err = Some(err);
                    0
                }
            };

            let bytes_to_copy = &tmp_buffer[..bytes_read];
            chunk_buf[chunk_buf_offset..chunk_buf_offset + bytes_read]
                .copy_from_slice(bytes_to_copy);
            chunk_buf_offset += bytes_read;

            if chunk_buf_offset >= chunk_size as usize
                || read_fully
                || (chunk_buf_offset > 0 && bytes_read == 0)
            {
                let (file_id, url) = self
                    .assign_new_file_info(
                        response,
                        extractor,
                        replication.clone(),
                        collection.clone(),
                        data_center.clone(),
                    )
                    .await?;

                let chunk_name = format!("{}_chunk_{}", filename, file_chunks.len() + 1);
                match upload(
                    &url,
                    chunk_name,
                    chunk_buf[..chunk_buf_offset].to_vec(),
                    "application/octet-stream",
                    None,
                )
                .await
                {
                    Ok(upload) => {
                        info!(
                            "chunk upload result. name: {}, size: {}, fid: {file_id}",
                            upload.name, upload.size
                        );

                        let chunk = FileChunk {
                            fid: file_id.to_string(),
                            offset: chunk_offset,
                            size: chunk_buf_offset as u64,
                            mtime: now().as_millis() as i64,
                            ..Default::default()
                        };

                        file_chunks.push(chunk);

                        // reset variables for the next chunk
                        chunk_buf_offset = 0;
                        chunk_offset = total_bytes_read as i64 + bytes_read as i64;
                    }
                    Err(err) => {
                        error!("chunk upload failed, fid: {file_id}, error: {err}");
                        return Err(err.into());
                    }
                }
            }

            total_bytes_read += bytes_read as u64;
            if bytes_read == 0 || read_fully {
                break;
            }

            if let Some(err) = read_err {
                return Err(err.into());
            }
        }

        let mut path = extractor.uri.path().to_string();
        if path.ends_with("/") && !filename.is_empty() {
            path = format!("{path}{filename}");
        }

        info!("saving entry, path: {path}");

        let now = SystemTime::now();
        let ttl = match &extractor.query.ttl {
            Some(ttl) => ttl.parse()?,
            None => 0,
        };
        let entry = Entry {
            full_path: path.clone(),
            attr: Attr {
                mtime: now,
                crtime: now,
                mode: 0o600,
                uid: getuid().as_raw(),
                gid: getgid().as_raw(),
                mime: FastStr::empty(),
                replication,
                collection,
                ttl,
                username: FastStr::empty(),
                group_names: vec![],
            },
            chunks: file_chunks,
        };

        if let Err(err) = self.filer.create_entry(&entry).await {
            self.filer.delete_chunks(&entry.chunks)?;
            result.error = err.to_string();
            error!("failed to write {path} to filer server， error: {err}");
            return Err(err);
        }
        Ok(result)
    }

    async fn assign_new_file_info(
        &self,
        response: &mut Response<Body>,
        extractor: &PostExtractor,
        replication: FastStr,
        collection: FastStr,
        data_center: FastStr,
    ) -> Result<(FastStr, FastStr), FilerError> {
        let request = AssignRequest {
            count: Some(1),
            replication: Some(replication),
            collection: Some(collection),
            ttl: extractor.query.ttl.clone(),
            data_center: Some(data_center),
            ..Default::default()
        };
        match assign(&self.filer.current_master(), request).await {
            Ok(assign) => {
                let url = format!("http://{}/{}/", assign.url, assign.fid);
                Ok((assign.fid, FastStr::new(url)))
            }
            Err(err) => {
                error!("assign file id error: {err}");
                *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                *response.body_mut() = Body::from("assign file id error".to_string());
                Err(FilerError::Box(Box::new(err)))
            }
        }
    }
}

pub async fn get_or_head_handler(
    State(state): State<FilerState>,
    extractor: GetOrHeadExtractor,
) -> Result<Response<Body>, FilerError> {
    let path = trim_trailing_slash(extractor.uri.path());

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

            if entry.chunks.len() == 1 {
                state
                    .handle_single_chunk(extractor, &mut response, &entry)
                    .await?;
            } else {
                state
                    .handle_chunks(extractor, &mut response, &entry)
                    .await?;
            }
        }
    }
    Ok(response)
}

pub async fn post_handler(
    State(state): State<FilerState>,
    mut extractor: PostExtractor,
) -> Result<Response<Body>, FilerError> {
    let replication = match &extractor.query.replication {
        Some(replication) => replication.clone(),
        None => state.options.default_replication.clone(),
    };
    let collection = match &extractor.query.collection {
        Some(collection) => collection.clone(),
        None => state.options.collection.clone(),
    };
    let data_center = match &extractor.query.data_center {
        Some(data_center) => data_center.clone(),
        None => state.options.data_center.clone(),
    };

    let mut response = Response::new(Body::empty());
    let auto_chunked = state
        .auto_chunk(
            &mut response,
            &mut extractor,
            replication.clone(),
            collection.clone(),
            data_center.clone(),
        )
        .await?;
    if auto_chunked {
        return Ok(response);
    }

    let (file_id, url) = state
        .assign_new_file_info(
            &mut response,
            &extractor,
            replication.clone(),
            collection.clone(),
            data_center.clone(),
        )
        .await?;

    if file_id.is_empty() || url.is_empty() {
        error!(
            "failed to allocate volume for {}, collection: {collection}, data_center: \
             {data_center}",
            extractor.uri
        );
        return Ok(response);
    }

    let mut url = Url::parse(&url).map_err(HttpError::UrlParse)?;
    let cm = extractor.query.cm.unwrap_or_default();
    if cm {
        url.query_pairs_mut().append_pair("cm", "true");
    }

    let resp = match request(
        &url,
        extractor.method,
        None,
        Some(extractor.body),
        Some(extractor.headers),
        None,
    )
    .await
    {
        Ok(resp) => resp,
        Err(err) => {
            error!(
                "failed to connect to volume server: {}, error: {err}",
                extractor.uri
            );
            *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
            *response.body_mut() = Body::from(err.to_string());
            return Ok(response);
        }
    };

    let etag = get_etag(resp.headers())?;
    let upload: UploadResult =
        serde_json::from_slice(&resp.bytes().await.map_err(HttpError::Reqwest)?)?;
    if !upload.error.is_empty() {
        *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
        *response.body_mut() = Body::from(upload.error);
        return Ok(response);
    }

    let mut path = extractor.uri.path().to_string();
    if path.ends_with("/") {
        if !upload.name.is_empty() {
            path = format!("{path}{}", upload.name);
        } else {
            state
                .filer
                .delete_file_id_tx
                .unbounded_send(file_id.clone())?;
            info!("can not to write to folder: {path} without a filename");
            *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
            *response.body_mut() =
                Body::from(format!("can not write to folder {path} without a filename"));
            return Ok(response);
        }
    }

    let mut crtime = SystemTime::now();
    if let Ok(Some(existing_entry)) = state.filer.find_entry(&path).await {
        if existing_entry.is_directory() {
            path = format!("{path}/{}", upload.name);
        } else {
            crtime = existing_entry.crtime;
        }
    }

    let mut entry = Entry {
        full_path: path.clone(),
        attr: Attr {
            mtime: SystemTime::now(),
            crtime,
            mode: 0o660,
            uid: getuid().as_raw(),
            gid: getgid().as_raw(),
            mime: Default::default(),
            replication,
            collection,
            ttl: extractor.query.ttl.unwrap_or_default().parse()?,
            username: Default::default(),
            group_names: vec![],
        },
        chunks: vec![FileChunk {
            fid: file_id.to_string(),
            size: upload.size as u64,
            mtime: now().as_millis() as i64,
            etag: etag.clone(),
            ..Default::default()
        }],
    };

    let mime = mime_guess::from_path(&path);
    entry.attr.mime = FastStr::new(mime.first_or_octet_stream().type_().as_str());

    if let Err(err) = state.filer.create_entry(&entry).await {
        state.filer.delete_chunks(&entry.chunks)?;
        error!("failed to write {path} to filer server, error: {err}");
        *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
        *response.body_mut() = Body::from(err.to_string());
        return Ok(response);
    }

    let reply = FilerPostResult {
        name: upload.name,
        size: upload.size as u32,
        error: upload.error,
        fid: file_id.to_string(),
        url: url.to_string(),
    };

    set_etag(&mut response, &etag).map_err(HttpError::InvalidHeaderValue)?;
    *response.status_mut() = StatusCode::CREATED;
    *response.body_mut() = Body::from(serde_json::to_vec(&reply)?);
    Ok(response)
}

pub async fn delete_handler(
    State(state): State<FilerState>,
    extractor: DeleteExtractor,
) -> Result<Response<Body>, FilerError> {
    let recursive = extractor.query.recursive.unwrap_or_default();
    let path = extractor.uri.path();
    let mut response = Response::new(Body::empty());
    match state
        .filer
        .delete_entry_meta_and_data(path, recursive, true)
        .await
    {
        Ok(_) => {
            *response.status_mut() = StatusCode::NO_CONTENT;
        }
        Err(err) => {
            error!("deleting {path} error: {err}");
            *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
            *response.body_mut() = Body::from(err.to_string());
        }
    }
    Ok(response)
}
