use std::{
    collections::HashMap, convert::Infallible, io::Read, result::Result as StdResult, str::FromStr,
    sync::Arc,
};

use axum::{
    body::Body,
    extract::State,
    http::{
        header::{
            HeaderName, HeaderValue, ACCEPT_ENCODING, ACCEPT_RANGES, CONTENT_ENCODING,
            CONTENT_LENGTH, ETAG, IF_MODIFIED_SINCE, IF_NONE_MATCH, LAST_MODIFIED,
        },
        Response, StatusCode,
    },
    Json,
};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::stream::once;
use helyim_common::{
    anyhow,
    consts::PAIR_NAME_PREFIX,
    crc, http,
    http::{parse_boundary, HttpError, HTTP_DATE_FORMAT},
    operation::{ParseUpload, UploadResult},
    parser::parse_url_path,
    time::now,
    ttl::Ttl,
    types::VolumeId,
};
use helyim_topology::volume::VolumeInfo;
use libflate::gzip::Decoder;
use mime_guess::mime;
use multer::Multipart;
use serde_json::{json, Value};
use tracing::{debug, error, info};

use crate::{
    http::extractor::{DeleteExtractor, GetOrHeadExtractor, PostExtractor},
    needle::{Needle, NeedleError, NeedleMapType},
    operation::Looker,
    store::StoreRef,
    volume::VolumeError,
};

pub mod erasure_coding;
mod extractor;

#[derive(Clone)]
pub struct StorageState {
    pub store: StoreRef,
    pub needle_map_type: NeedleMapType,
    pub read_redirect: bool,
    pub pulse: u64,
    pub looker: Arc<Looker>,
}

pub async fn status_handler(State(state): State<StorageState>) -> Json<Value> {
    let mut infos: Vec<VolumeInfo> = vec![];
    for location in state.store.locations().iter() {
        for volume in location.volumes.iter() {
            infos.push(volume.get_volume_info());
        }
    }

    let stat = json!({
        "version": "0.1",
        "volumes": &infos,
    });

    Json(stat)
}

pub async fn delete_handler(
    State(state): State<StorageState>,
    extractor: DeleteExtractor,
) -> Result<Json<Value>, NeedleError> {
    let (vid, fid, _, _) = parse_url_path(extractor.uri.path())?;
    let is_replicate = extractor.query.r#type == Some("replicate".into());

    let mut needle = Needle::new_with_fid(fid)?;

    let cookie = needle.cookie;
    let _ = state.store.read_volume_needle(vid, &mut needle).await?;
    if cookie != needle.cookie {
        info!(
            "cookie not match from {:?} recv: {}, file is {}",
            extractor.host, cookie, needle.cookie
        );
        return Err(NeedleError::CookieNotMatch(needle.cookie, cookie));
    }

    let size = replicate_delete(&state, extractor.uri.path(), vid, &mut needle, is_replicate)
        .await
        .map_err(|err| NeedleError::Box(err.into()))?;
    let size = json!({ "size": size });

    Ok(Json(size))
}

async fn replicate_delete(
    state: &StorageState,
    path: &str,
    vid: VolumeId,
    needle: &mut Needle,
    is_replicate: bool,
) -> Result<usize, VolumeError> {
    let local_url = format!("{}:{}", state.store.ip, state.store.port);
    let size = state.store.delete_volume_needle(vid, needle).await?;
    if is_replicate {
        return Ok(size);
    }

    if let Some(volume) = state.store.find_volume(vid) {
        if !volume.need_to_replicate() {
            return Ok(size);
        }
    }

    let params = vec![("type", "replicate")];
    let mut volume_locations = state
        .looker
        .lookup(vec![vid], &state.store.current_master.read().await)
        .await?;

    if let Some(volume_location) = volume_locations.pop() {
        async_scoped::TokioScope::scope_and_block(|s| {
            for location in volume_location.locations.iter() {
                if location.url == local_url {
                    continue;
                }
                s.spawn(async {
                    let url = format!("http://{}{}", &location.url, path);
                    if let Err(err) = http::delete(&url, &params).await.and_then(|body| {
                        match serde_json::from_slice::<Value>(&body) {
                            Ok(value) => {
                                if let Some(err) = value["error"].as_str() {
                                    if !err.is_empty() {
                                        return Err(anyhow!("delete {} err: {err}", location.url));
                                    }
                                }
                                Ok(())
                            }
                            Err(err) => {
                                error!("replicate delete: serde json error: {err}");
                                Err(anyhow!("replicate delete: serde json error: {err}"))
                            }
                        }
                    }) {
                        error!("replicate delete failed, error: {err}");
                    }
                });
            }
        });
    }
    Ok(size)
}

pub async fn post_handler(
    State(state): State<StorageState>,
    extractor: PostExtractor,
) -> Result<Json<UploadResult>, VolumeError> {
    let (vid, _, _, _) = parse_url_path(extractor.uri.path())?;
    let is_replicate = extractor.query.r#type == Some("replicate".into());

    let mut needle = if is_replicate {
        bincode::deserialize(&extractor.body)?
    } else {
        new_needle_from_request(&extractor).await?
    };

    let size =
        replicate_write(&state, extractor.uri.path(), vid, &mut needle, is_replicate).await?;
    debug!("written needle size: {size}, is_replicate: {is_replicate}");

    let mut upload = UploadResult {
        size,
        ..Default::default()
    };
    if needle.has_name() {
        upload.name = String::from_utf8(needle.name.to_vec())?;
    }

    // TODO: add etag support
    Ok(Json(upload))
}

async fn replicate_write(
    state: &StorageState,
    path: &str,
    vid: VolumeId,
    needle: &mut Needle,
    is_replicate: bool,
) -> Result<usize, VolumeError> {
    let local_url = format!("{}:{}", state.store.ip, state.store.port);
    let size = state.store.write_volume_needle(vid, needle).await?;
    // if the volume is replica, it will return needle directly.
    if is_replicate {
        return Ok(size);
    }

    if let Some(volume) = state.store.find_volume(vid) {
        if !volume.need_to_replicate() {
            return Ok(size);
        }
    }

    let params = vec![("type", "replicate")];
    let data = Bytes::from(bincode::serialize(&needle)?);

    let mut volume_locations = state
        .looker
        .lookup(vec![vid], &state.store.current_master.read().await)
        .await?;

    if let Some(volume_location) = volume_locations.pop() {
        async_scoped::TokioScope::scope_and_block(|s| {
            for location in volume_location.locations.iter() {
                if location.url == local_url {
                    continue;
                }
                s.spawn(async {
                    let url = format!("http://{}{}", location.url, path);
                    if let Err(err) =
                        http::post(&url, &params, data.clone())
                            .await
                            .and_then(|body| match serde_json::from_slice::<Value>(&body) {
                                Ok(value) => {
                                    if let Some(err) = value["error"].as_str() {
                                        if !err.is_empty() {
                                            return Err(anyhow!(
                                                "write {} err: {err}",
                                                location.url
                                            ));
                                        }
                                    }
                                    Ok(())
                                }
                                Err(err) => {
                                    error!("replicate write: serde json error: {err}");
                                    Err(anyhow!("replicate write: serde json error: {err}"))
                                }
                            })
                    {
                        error!("replicate write failed, error: {err}");
                    }
                });
            }
        });
    }

    Ok(size)
}

async fn new_needle_from_request(extractor: &PostExtractor) -> Result<Needle, VolumeError> {
    let mut parse_upload = parse_upload(extractor).await?;
    debug!(
        "parsed upload file -> name: {}, mime_type: {}, is_chunked_file: {}",
        parse_upload.filename, parse_upload.mime_type, parse_upload.is_chunked_file
    );

    let mut needle = Needle {
        data: Bytes::from(parse_upload.data),
        ..Default::default()
    };

    if !parse_upload.pair_map.is_empty() {
        needle.set_has_pairs();
        needle.pairs =
            Bytes::from(serde_json::to_vec(&parse_upload.pair_map).map_err(HttpError::SerdeJson)?);
    }

    if !parse_upload.filename.is_empty() {
        needle.name = Bytes::from(parse_upload.filename);
        needle.set_name();
    }

    if parse_upload.mime_type.len() < 256 {
        needle.mime = Bytes::from(parse_upload.mime_type);
        needle.set_has_mime();
    }

    if parse_upload.modified_time == 0 {
        parse_upload.modified_time = now().as_millis() as u64;
    }
    needle.last_modified = parse_upload.modified_time;
    needle.set_has_last_modified_date();

    if parse_upload.ttl.minutes() != 0 {
        needle.ttl = parse_upload.ttl;
        needle.set_has_ttl();
    }

    if parse_upload.is_chunked_file {
        needle.set_is_chunk_manifest();
    }

    needle.checksum = crc::checksum(&needle.data);

    let path = extractor.uri.path();
    let start = path.find(',').map(|idx| idx + 1).unwrap_or(0);
    let end = path.rfind('.').unwrap_or(path.len());
    needle.parse_path(&path[start..end])?;

    Ok(needle)
}

async fn parse_upload(extractor: &PostExtractor) -> Result<ParseUpload, VolumeError> {
    let mut pair_map = HashMap::new();
    for (header_name, header_value) in extractor.headers.iter() {
        if header_name.as_str().starts_with(PAIR_NAME_PREFIX) {
            pair_map.insert(
                header_name.to_string(),
                String::from_utf8(header_value.as_bytes().to_vec())?,
            );
        }
    }

    let boundary = parse_boundary(&extractor.headers)?;

    let stream = once(async move { StdResult::<Bytes, Infallible>::Ok(extractor.body.clone()) });
    let mut mpart = Multipart::new(stream, boundary);

    // get first file with filename
    let mut filename = String::new();
    let mut data = vec![];
    let mut post_mtype = String::new();
    while let Ok(Some(field)) = mpart.next_field().await {
        if let Some(name) = field.file_name() {
            if !name.is_empty() {
                filename = name.to_string();
                if let Some(content_type) = field.content_type() {
                    post_mtype.push_str(content_type.type_().as_str());
                    post_mtype.push('/');
                    post_mtype.push_str(content_type.subtype().as_str());
                }
                data.extend(field.bytes().await.map_err(HttpError::Multer)?);
                break;
            }
        }
    }

    let is_chunked_file = extractor.query.cm.unwrap_or(false);

    let mut guess_mtype = String::new();
    if !is_chunked_file {
        if let Some(idx) = filename.find('.') {
            let ext = &filename[idx..];
            let m = mime_guess::from_ext(ext).first_or_octet_stream();
            if m.type_() != mime::APPLICATION || m.subtype() != mime::OCTET_STREAM {
                guess_mtype.push_str(m.type_().as_str());
                guess_mtype.push('/');
                guess_mtype.push_str(m.subtype().as_str());
            }
        }

        if !post_mtype.is_empty() && guess_mtype != post_mtype {
            guess_mtype = post_mtype;
        }
    }

    let modified_time = extractor.query.ts.unwrap_or(0);

    let ttl = match &extractor.query.ttl {
        Some(s) => Ttl::new(s).unwrap_or_default(),
        None => Ttl::default(),
    };

    let resp = ParseUpload {
        filename,
        data,
        mime_type: guess_mtype,
        pair_map,
        modified_time,
        ttl,
        is_chunked_file,
    };

    Ok(resp)
}

pub async fn get_or_head_handler(
    State(state): State<StorageState>,
    extractor: GetOrHeadExtractor,
) -> Result<Response<Body>, VolumeError> {
    let (vid, fid, _filename, _ext) = parse_url_path(extractor.uri.path())?;

    let mut response = Response::new(Body::empty());

    let has_volume = state.store.has_volume(vid);
    let has_ec_volume = state.store.has_ec_volume(vid);
    if !has_volume && !has_ec_volume {
        // TODO: support read redirect
        if !state.read_redirect {
            info!("volume {} is not belongs to this server", vid);
            *response.status_mut() = StatusCode::NOT_FOUND;
            return Ok(response);
        }
    }

    let mut needle = Needle::new_with_fid(fid)?;
    let cookie = needle.cookie;

    let mut count = 0;
    if has_volume {
        count = state.store.read_volume_needle(vid, &mut needle).await?;
    } else if has_ec_volume {
        count = state.store.read_ec_shard_needle(vid, &mut needle).await?;
    }

    if count == 0 {
        *response.status_mut() = StatusCode::NOT_FOUND;
        return Ok(response);
    }

    if needle.cookie != cookie {
        return Err(NeedleError::CookieNotMatch(needle.cookie, cookie).into());
    }

    // TODO: ignore datetime parsing error
    if needle.last_modified != 0 {
        let datetime: DateTime<Utc> = DateTime::from_timestamp_millis(needle.last_modified as i64)
            .expect("invalid or out-of-range datetime");
        let last_modified = datetime.format(HTTP_DATE_FORMAT).to_string();
        response.headers_mut().insert(
            LAST_MODIFIED,
            HeaderValue::from_str(last_modified.as_str()).map_err(HttpError::InvalidHeaderValue)?,
        );

        if let Some(since) = extractor.headers.get(IF_MODIFIED_SINCE) {
            if !since.is_empty() {
                let since = DateTime::parse_from_str(
                    since.to_str().map_err(HttpError::ToStr)?,
                    HTTP_DATE_FORMAT,
                )?
                .with_timezone(&Utc);
                if since.timestamp() <= needle.last_modified as i64 {
                    *response.status_mut() = StatusCode::NOT_MODIFIED;
                    return Ok(response);
                }
            }
        }
    }

    let etag = needle.etag();
    if let Some(if_none_match) = extractor.headers.get(IF_NONE_MATCH) {
        if if_none_match == etag.as_str() {
            *response.status_mut() = StatusCode::NOT_MODIFIED;
            return Ok(response);
        }
    }
    response.headers_mut().insert(
        ETAG,
        HeaderValue::from_str(etag.as_str()).map_err(HttpError::InvalidHeaderValue)?,
    );

    if needle.has_pairs() {
        // only accept string type value
        let pairs: Value = serde_json::from_slice(&needle.pairs).map_err(HttpError::SerdeJson)?;
        if let Some(map) = pairs.as_object() {
            for (k, v) in map {
                if let Some(value) = v.as_str() {
                    response.headers_mut().insert(
                        HeaderName::from_str(k).map_err(HttpError::InvalidHeaderName)?,
                        HeaderValue::from_str(value).map_err(HttpError::InvalidHeaderValue)?,
                    );
                }
            }
        }
    }

    if needle.is_gzipped() {
        match extractor.headers.get(ACCEPT_ENCODING) {
            Some(value) => {
                if value.to_str().map_err(HttpError::ToStr)?.contains("gzip") {
                    response
                        .headers_mut()
                        .insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
                }
            }
            None => {
                let mut decoded = Vec::new();
                {
                    let mut decoder = Decoder::new(&needle.data[..])?;
                    decoder.read_to_end(&mut decoded)?;
                }
                needle.data = Bytes::from(decoded);
            }
        }
    }

    response
        .headers_mut()
        .insert(CONTENT_LENGTH, HeaderValue::from(needle.data_size()));
    response
        .headers_mut()
        .insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    *response.body_mut() = Body::from(needle.data);
    *response.status_mut() = StatusCode::ACCEPTED;

    Ok(response)
}
