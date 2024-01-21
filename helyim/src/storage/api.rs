use std::{
    collections::HashMap, convert::Infallible, io::Read, result::Result as StdResult, str::FromStr,
    sync::Arc,
};

use axum::{
    extract::State,
    headers::{HeaderName, HeaderValue},
    http::{
        header::{
            ACCEPT_ENCODING, ACCEPT_RANGES, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE, ETAG,
            IF_MODIFIED_SINCE, IF_NONE_MATCH, LAST_MODIFIED,
        },
        Response, StatusCode,
    },
    Json,
};
use bytes::Bytes;
use chrono::{DateTime, NaiveDateTime, Utc};
use futures::stream::once;
use hyper::Body;
use libflate::gzip::Decoder;
use mime_guess::mime;
use multer::Multipart;
use serde_json::{json, Value};
use tracing::{error, info};

use crate::{
    anyhow,
    errors::Result,
    operation::{Looker, ParseUpload, Upload},
    storage::{
        crc,
        needle::{Needle, NeedleMapType, PAIR_NAME_PREFIX},
        store::StoreRef,
        NeedleError, Ttl, VolumeId, VolumeInfo,
    },
    util,
    util::{
        http::{
            extractor::{DeleteExtractor, GetOrHeadExtractor, PostExtractor},
            HTTP_DATE_FORMAT,
        },
        parser::parse_url_path,
        time::now,
    },
};

#[derive(Clone)]
pub struct StorageState {
    pub store: StoreRef,
    pub needle_map_type: NeedleMapType,
    pub read_redirect: bool,
    pub pulse: u64,
    pub looker: Arc<Looker>,
}

pub async fn status_handler(State(ctx): State<StorageState>) -> Result<Json<Value>> {
    let mut infos: Vec<VolumeInfo> = vec![];
    for location in ctx.store.locations().iter() {
        for volume in location.volumes.iter() {
            infos.push(volume.get_volume_info());
        }
    }

    let stat = json!({
        "version": "0.1",
        "volumes": &infos,
    });

    Ok(Json(stat))
}

pub async fn delete_handler(
    State(ctx): State<StorageState>,
    extractor: DeleteExtractor,
) -> Result<Json<Value>> {
    let (vid, fid, _, _) = parse_url_path(extractor.uri.path())?;
    let is_replicate = extractor.query.r#type == Some("replicate".into());

    let mut needle = Needle::new_with_fid(fid)?;

    let cookie = needle.cookie;
    let _ = ctx.store.read_volume_needle(vid, &mut needle).await?;
    if cookie != needle.cookie {
        info!(
            "cookie not match from {:?} recv: {}, file is {}",
            extractor.host, cookie, needle.cookie
        );
        return Err(NeedleError::CookieNotMatch(needle.cookie, cookie).into());
    }

    let size = replicate_delete(&ctx, extractor.uri.path(), vid, &mut needle, is_replicate).await?;
    let size = json!({ "size": size });

    Ok(Json(size))
}

async fn replicate_delete(
    ctx: &StorageState,
    path: &str,
    vid: VolumeId,
    needle: &mut Needle,
    is_replicate: bool,
) -> Result<u32> {
    let local_url = format!("{}:{}", ctx.store.ip, ctx.store.port);
    let size = ctx.store.delete_volume_needle(vid, needle).await?;
    if is_replicate {
        return Ok(size);
    }

    if let Some(volume) = ctx.store.find_volume(vid) {
        if !volume.need_to_replicate() {
            return Ok(size);
        }
    }

    let params = vec![("type", "replicate")];
    let mut volume_locations = ctx
        .looker
        .lookup(vec![vid], &ctx.store.current_master.read().await)
        .await?;

    if let Some(volume_location) = volume_locations.pop() {
        async_scoped::TokioScope::scope_and_block(|s| {
            for location in volume_location.locations.iter() {
                if location.url == local_url {
                    continue;
                }
                s.spawn(async {
                    let url = format!("http://{}{}", &location.url, path);
                    if let Err(err) = util::http::delete(&url, &params).await.and_then(|body| {
                        let value: Value = serde_json::from_slice(&body)?;
                        if let Some(err) = value["error"].as_str() {
                            if !err.is_empty() {
                                return Err(anyhow!("delete {} err: {err}", location.url));
                            }
                        }
                        Ok(())
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
    State(ctx): State<StorageState>,
    extractor: PostExtractor,
) -> Result<Json<Upload>> {
    let (vid, _, _, _) = parse_url_path(extractor.uri.path())?;
    let is_replicate = extractor.query.r#type == Some("replicate".into());

    let mut needle = if is_replicate {
        bincode::deserialize(&extractor.body)?
    } else {
        new_needle_from_request(&extractor).await?
    };

    let size = replicate_write(&ctx, extractor.uri.path(), vid, &mut needle, is_replicate).await?;
    let mut upload = Upload {
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
    ctx: &StorageState,
    path: &str,
    vid: VolumeId,
    needle: &mut Needle,
    is_replicate: bool,
) -> Result<u32> {
    let local_url = format!("{}:{}", ctx.store.ip, ctx.store.port);
    let size = ctx.store.write_volume_needle(vid, needle).await?;
    // if the volume is replica, it will return needle directly.
    if is_replicate {
        return Ok(size);
    }

    if let Some(volume) = ctx.store.find_volume(vid) {
        if !volume.need_to_replicate() {
            return Ok(size);
        }
    }

    let params = vec![("type", "replicate")];
    let data = bincode::serialize(&needle)?;

    let mut volume_locations = ctx
        .looker
        .lookup(vec![vid], &ctx.store.current_master.read().await)
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
                        util::http::post(&url, &params, &data)
                            .await
                            .and_then(|body| {
                                let value: Value = serde_json::from_slice(&body)?;
                                if let Some(err) = value["error"].as_str() {
                                    if !err.is_empty() {
                                        return Err(anyhow!("write {} err: {err}", location.url));
                                    }
                                }
                                Ok(())
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

async fn new_needle_from_request(extractor: &PostExtractor) -> Result<Needle> {
    let mut parse_upload = parse_upload(extractor).await?;

    let mut needle = Needle {
        data: Bytes::from(parse_upload.data),
        ..Default::default()
    };

    if !parse_upload.pair_map.is_empty() {
        needle.set_has_pairs();
        needle.pairs = Bytes::from(serde_json::to_vec(&parse_upload.pair_map)?);
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

fn get_boundary(extractor: &PostExtractor) -> Result<String> {
    const BOUNDARY: &str = "boundary=";

    return match extractor.headers.get(CONTENT_TYPE) {
        Some(content_type) => {
            let ct = content_type.to_str()?;
            match ct.find(BOUNDARY) {
                Some(idx) => Ok(ct[idx + BOUNDARY.len()..].to_string()),
                None => Err(anyhow!("no boundary")),
            }
        }
        None => Err(anyhow!("no content type")),
    };
}

async fn parse_upload(extractor: &PostExtractor) -> Result<ParseUpload> {
    let mut pair_map = HashMap::new();
    for (header_name, header_value) in extractor.headers.iter() {
        if header_name.as_str().starts_with(PAIR_NAME_PREFIX) {
            pair_map.insert(
                header_name.to_string(),
                String::from_utf8(header_value.as_bytes().to_vec())?,
            );
        }
    }

    let boundary = get_boundary(extractor)?;

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
                data.extend(field.bytes().await?);
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
    State(ctx): State<StorageState>,
    extractor: GetOrHeadExtractor,
) -> Result<Response<Body>> {
    let (vid, fid, _filename, _ext) = parse_url_path(extractor.uri.path())?;

    let mut response = Response::new(Body::empty());
    if !ctx.store.has_volume(vid) {
        // TODO: support read redirect
        if !ctx.read_redirect {
            info!("volume {} is not belongs to this server", vid);
            *response.status_mut() = StatusCode::NOT_FOUND;
            return Ok(response);
        }
    }

    let mut needle = Needle::new_with_fid(fid)?;
    let cookie = needle.cookie;
    let _ = ctx.store.read_volume_needle(vid, &mut needle).await?;
    if needle.cookie != cookie {
        return Err(NeedleError::CookieNotMatch(needle.cookie, cookie).into());
    }

    // TODO: ignore datetime parsing error
    if needle.last_modified != 0 {
        let datetime: DateTime<Utc> = DateTime::from_naive_utc_and_offset(
            NaiveDateTime::from_timestamp_millis(needle.last_modified as i64)
                .expect("invalid or out-of-range datetime"),
            Utc,
        );
        let last_modified = datetime.format(HTTP_DATE_FORMAT).to_string();
        response.headers_mut().insert(
            LAST_MODIFIED,
            HeaderValue::from_str(last_modified.as_str())?,
        );

        if let Some(since) = extractor.headers.get(IF_MODIFIED_SINCE) {
            if !since.is_empty() {
                let since = DateTime::parse_from_str(since.to_str()?, HTTP_DATE_FORMAT)?
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
    response
        .headers_mut()
        .insert(ETAG, HeaderValue::from_str(etag.as_str())?);

    if needle.has_pairs() {
        // only accept string type value
        let pairs: Value = serde_json::from_slice(&needle.pairs)?;
        if let Some(map) = pairs.as_object() {
            for (k, v) in map {
                if let Some(value) = v.as_str() {
                    response
                        .headers_mut()
                        .insert(HeaderName::from_str(k)?, HeaderValue::from_str(value)?);
                }
            }
        }
    }

    if needle.is_gzipped() {
        match extractor.headers.get(ACCEPT_ENCODING) {
            Some(value) => {
                if value.to_str()?.contains("gzip") {
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
