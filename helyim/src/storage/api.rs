use std::{
    collections::HashMap, convert::Infallible, io::Read, result::Result as StdResult, str::FromStr,
    sync::Arc,
};

use axum::{
    extract::{Query, State},
    headers::{HeaderName, HeaderValue, Host},
    http::{
        header::{
            ACCEPT_ENCODING, ACCEPT_RANGES, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE, ETAG,
            IF_MODIFIED_SINCE, IF_NONE_MATCH, LAST_MODIFIED,
        },
        HeaderMap, Response, StatusCode, Uri,
    },
    Json, TypedHeader,
};
use axum_macros::FromRequest;
use bytes::Bytes;
use chrono::{DateTime, NaiveDateTime, Utc};
use faststr::FastStr;
use futures::stream::once;
use ginepro::LoadBalancedChannel;
use helyim_proto::helyim_client::HelyimClient;
use hyper::Body;
use libflate::gzip::Decoder;
use mime_guess::mime;
use multer::Multipart;
use nom::{
    branch::alt,
    bytes::complete::take_till,
    character::complete::{alphanumeric1, char, digit1},
    combinator::opt,
    sequence::{pair, tuple},
    IResult,
};
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::{error, trace, warn};

use crate::{
    anyhow,
    errors::Result,
    operation::{Looker, ParseUpload, Upload},
    storage::{
        crc,
        needle::{Needle, PAIR_NAME_PREFIX},
        needle_map::NeedleMapType,
        store::StoreRef,
        NeedleError, Ttl, VolumeError, VolumeId, VolumeInfo,
    },
    util,
    util::{time::now, HTTP_DATE_FORMAT},
};

#[derive(Clone)]
pub struct StorageContext {
    pub store: StoreRef,
    pub needle_map_type: NeedleMapType,
    pub read_redirect: bool,
    pub pulse_seconds: u64,
    pub client: HelyimClient<LoadBalancedChannel>,
    pub looker: Arc<Looker>,
}

pub async fn status_handler(State(ctx): State<StorageContext>) -> Result<Json<Value>> {
    let mut infos: Vec<VolumeInfo> = vec![];
    for location in ctx.store.read().await.locations().iter() {
        for (_, volume) in location.read().await.get_volumes().iter() {
            infos.push(volume.read().await.get_volume_info());
        }
    }

    let stat = json!({
        "version": "0.1",
        "volumes": &infos,
    });

    Ok(Json(stat))
}

#[derive(Debug, FromRequest)]
pub struct DeleteExtractor {
    // only the last field can implement `FromRequest`
    // other fields must only implement `FromRequestParts`
    uri: Uri,
    #[from_request(via(TypedHeader))]
    host: Host,
    #[from_request(via(Query))]
    query: StorageQuery,
}

#[derive(Debug, Deserialize)]
pub struct StorageQuery {
    r#type: Option<FastStr>,
    // is chunked file
    cm: Option<bool>,
    ttl: Option<FastStr>,
    // last modified
    ts: Option<u64>,
}

pub async fn delete_handler(
    State(mut ctx): State<StorageContext>,
    extractor: DeleteExtractor,
) -> Result<Json<Value>> {
    let (vid, fid, _, _) = parse_url_path(extractor.uri.path())?;
    let is_replicate = extractor.query.r#type == Some("replicate".into());
    let mut needle = Needle::new_with_fid(fid)?;

    trace!(
        "delete needle {}, client host: {}",
        needle.id,
        extractor.host
    );

    let cookie = needle.cookie;
    ctx.store
        .read()
        .await
        .read_volume_needle(vid, &mut needle)
        .await?;
    if cookie != needle.cookie {
        error!(
            "cookie not match, expect {}, but got {}",
            needle.cookie, cookie
        );
        return Err(NeedleError::CookieNotMatch(needle.cookie, cookie).into());
    }

    let size = replicate_delete(
        &mut ctx,
        extractor.uri.path(),
        vid,
        &mut needle,
        is_replicate,
    )
    .await?;
    let size = json!({ "size": size });

    trace!(
        "delete needle {} success, size: {size}, client host: {}",
        needle.id,
        extractor.host
    );
    Ok(Json(size))
}

async fn replicate_delete(
    ctx: &mut StorageContext,
    path: &str,
    vid: VolumeId,
    needle: &mut Needle,
    is_replicate: bool,
) -> Result<u32> {
    let local_url = format!(
        "{}:{}",
        ctx.store.read().await.ip,
        ctx.store.read().await.port
    );
    let size = ctx
        .store
        .write()
        .await
        .delete_volume_needle(vid, needle)
        .await?;
    if is_replicate {
        return Ok(size);
    }

    if let Some(volume) = ctx.store.read().await.find_volume(vid).await? {
        if !volume.read().await.need_to_replicate() {
            return Ok(size);
        }
    }

    let params = vec![("type", "replicate")];
    let mut volume_locations = ctx.looker.lookup(vec![vid], &mut ctx.client).await?;

    if let Some(volume_location) = volume_locations.pop() {
        async_scoped::TokioScope::scope_and_block(|s| {
            for location in volume_location.locations.iter() {
                if location.url == local_url {
                    continue;
                }
                s.spawn(async {
                    let url = format!("http://{}{}", &location.url, path);
                    trace!(
                        "replicate delete needle {}, volume: {vid}, location: {}, path: {}",
                        needle.id,
                        location.url,
                        path
                    );
                    if let Err(err) = util::delete(&url, &params).await.and_then(|body| {
                        let value: Value = serde_json::from_slice(&body)?;
                        if let Some(err) = value["error"].as_str() {
                            if !err.is_empty() {
                                error!(
                                    "failed to delete needle {} in location: {}, volume: {vid}, \
                                     error: {err}",
                                    needle.id, location.url
                                );
                                return Err(NeedleError::ReplicateDelete(
                                    vid,
                                    needle.id,
                                    location.url.clone(),
                                    err.to_string(),
                                )
                                .into());
                            }
                        }
                        trace!(
                            "replicate delete needle {} success, volume: {vid}, location: {}",
                            needle.id,
                            location.url
                        );
                        Ok(())
                    }) {
                        error!(
                            "replicate delete needle {} failed, volume: {vid}, error: {err}",
                            needle.id
                        );
                    }
                });
            }
        });
    }
    Ok(size)
}

#[derive(Debug, FromRequest)]
pub struct PostExtractor {
    // only the last field can implement `FromRequest`
    // other fields must only implement `FromRequestParts`
    uri: Uri,
    headers: HeaderMap,
    #[from_request(via(TypedHeader))]
    host: Host,
    #[from_request(via(Query))]
    query: StorageQuery,
    body: Bytes,
}

pub async fn post_handler(
    State(mut ctx): State<StorageContext>,
    extractor: PostExtractor,
) -> Result<Json<Upload>> {
    let (vid, _, _, _) = parse_url_path(extractor.uri.path())?;
    let is_replicate = extractor.query.r#type == Some("replicate".into());

    let mut needle = if is_replicate {
        let needle = bincode::deserialize::<Needle>(&extractor.body)?;
        trace!(
            "receive replicate write needle request from {}, needle {}",
            extractor.host,
            needle.id
        );
        needle
    } else {
        new_needle_from_request(&extractor).await?
    };

    let size = replicate_write(
        &mut ctx,
        extractor.uri.path(),
        vid,
        &mut needle,
        is_replicate,
    )
    .await?;
    let mut upload = Upload {
        size,
        ..Default::default()
    };
    if needle.has_name() {
        upload.name = String::from_utf8(needle.name.to_vec())?;
    }

    trace!("write needle {} success, volume: {vid}", needle.id);
    // TODO: add etag support
    Ok(Json(upload))
}

async fn replicate_write(
    ctx: &mut StorageContext,
    path: &str,
    vid: VolumeId,
    needle: &mut Needle,
    is_replicate: bool,
) -> Result<u32> {
    let local_url = format!(
        "{}:{}",
        ctx.store.read().await.ip,
        ctx.store.read().await.port
    );
    let size = ctx
        .store
        .write()
        .await
        .write_volume_needle(vid, needle)
        .await?;
    // if the volume is replica, it will return needle directly.
    if is_replicate {
        return Ok(size);
    }

    if let Some(volume) = ctx.store.read().await.find_volume(vid).await? {
        if !volume.read().await.need_to_replicate() {
            return Ok(size);
        }
    }

    let params = vec![("type", "replicate")];
    let data = bincode::serialize(&needle)?;

    let mut volume_locations = ctx.looker.lookup(vec![vid], &mut ctx.client).await?;

    if let Some(volume_location) = volume_locations.pop() {
        async_scoped::TokioScope::scope_and_block(|s| {
            for location in volume_location.locations.iter() {
                if location.url == local_url {
                    continue;
                }
                s.spawn(async {
                    let url = format!("http://{}{}", location.url, path);
                    trace!(
                        "replicate write needle {}, volume: {vid}, location: {}, path: {}",
                        needle.id,
                        location.url,
                        path
                    );
                    if let Err(err) = util::post(&url, &params, &data).await.and_then(|body| {
                        let value: Value = serde_json::from_slice(&body)?;
                        if let Some(err) = value["error"].as_str() {
                            if !err.is_empty() {
                                error!(
                                    "failed to write needle {} in location: {}, volume: {vid}, \
                                     error: {err}",
                                    needle.id, location.url
                                );
                                return Err(NeedleError::ReplicateWrite(
                                    vid,
                                    needle.id,
                                    location.url.clone(),
                                    err.to_string(),
                                )
                                .into());
                            }
                        }
                        trace!(
                            "replicate write needle {} success, volume: {vid}, location: {}",
                            needle.id,
                            location.url
                        );
                        Ok(())
                    }) {
                        error!(
                            "replicate write needle {} failed, volume: {vid}, error: {err}",
                            needle.id
                        );
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

#[derive(Debug, FromRequest)]
pub struct GetOrHeadExtractor {
    uri: Uri,
    headers: HeaderMap,
}

pub async fn get_or_head_handler(
    State(ctx): State<StorageContext>,
    extractor: GetOrHeadExtractor,
) -> Result<Response<Body>> {
    let (vid, fid, _filename, _ext) = parse_url_path(extractor.uri.path())?;

    let mut response = Response::new(Body::empty());
    if !ctx.store.read().await.has_volume(vid).await? {
        // TODO: support read redirect
        if !ctx.read_redirect {
            warn!("volume {} is not belongs to this server", vid);
            *response.status_mut() = StatusCode::NOT_FOUND;
            return Ok(response);
        }
    }

    let mut needle = Needle::new_with_fid(fid)?;
    let cookie = needle.cookie;
    let _ = ctx
        .store
        .read()
        .await
        .read_volume_needle(vid, &mut needle)
        .await?;
    if needle.cookie != cookie {
        error!(
            "cookie not match, expect {}, but got {}",
            needle.cookie, cookie
        );
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

fn parse_url_path(input: &str) -> StdResult<(VolumeId, &str, Option<&str>, &str), VolumeError> {
    let (vid, fid, filename, ext) = tuple((
        char('/'),
        parse_vid_fid,
        opt(pair(char('/'), parse_filename)),
    ))(input)
    .map(|(input, (_, (vid, fid), filename))| {
        (vid, fid, filename.map(|(_, filename)| filename), input)
    })?;
    Ok((vid.parse()?, fid, filename, ext))
}

fn parse_vid_fid(input: &str) -> IResult<&str, (&str, &str)> {
    let (input, (vid, _, fid)) =
        tuple((digit1, alt((char('/'), char(','))), alphanumeric1))(input)?;
    Ok((input, (vid, fid)))
}

fn parse_filename(input: &str) -> IResult<&str, &str> {
    let (input, filename) = take_till(|c| c == '.')(input)?;
    Ok((input, filename))
}

#[cfg(test)]
mod tests {
    use crate::storage::api::{parse_filename, parse_url_path, parse_vid_fid};

    #[test]
    pub fn test_parse_vid_fid() {
        let (input, (vid, fid)) = parse_vid_fid("3/01637037d6").unwrap();
        assert_eq!(vid, "3");
        assert_eq!(fid, "01637037d6");
        assert_eq!(input, "");

        let (input, (vid, fid)) = parse_vid_fid("3/01637037d6/").unwrap();
        assert_eq!(vid, "3");
        assert_eq!(fid, "01637037d6");
        assert_eq!(input, "/");

        let (input, (vid, fid)) = parse_vid_fid("3,01637037d6").unwrap();
        assert_eq!(vid, "3");
        assert_eq!(fid, "01637037d6");
        assert_eq!(input, "");

        let (input, (vid, fid)) = parse_vid_fid("3,01637037d6/").unwrap();
        assert_eq!(vid, "3");
        assert_eq!(fid, "01637037d6");
        assert_eq!(input, "/");
    }

    #[test]
    pub fn test_parse_filename() {
        let (input, filename) = parse_filename("my_preferred_name.jpg").unwrap();
        assert_eq!(filename, "my_preferred_name");
        assert_eq!(input, ".jpg");

        let (input, filename) = parse_filename("my_preferred_name").unwrap();
        assert_eq!(filename, "my_preferred_name");
        assert_eq!(input, "");

        let (input, filename) = parse_filename("").unwrap();
        assert_eq!(filename, "");
        assert_eq!(input, "");
    }

    #[test]
    pub fn test_parse_path() {
        let (vid, fid, filename, ext) =
            parse_url_path("/3/01637037d6/my_preferred_name.jpg").unwrap();
        assert_eq!(vid, 3);
        assert_eq!(fid, "01637037d6");
        assert_eq!(filename, Some("my_preferred_name"));
        assert_eq!(ext, ".jpg");

        let (vid, fid, filename, ext) = parse_url_path("/3/01637037d6/my_preferred_name").unwrap();
        assert_eq!(vid, 3);
        assert_eq!(fid, "01637037d6");
        assert_eq!(filename, Some("my_preferred_name"));
        assert_eq!(ext, "");

        let (vid, fid, filename, ext) = parse_url_path("/3/01637037d6.jpg").unwrap();
        assert_eq!(vid, 3);
        assert_eq!(fid, "01637037d6");
        assert_eq!(filename, None);
        assert_eq!(ext, ".jpg");

        let (vid, fid, filename, ext) = parse_url_path("/30,01637037d6.jpg").unwrap();
        assert_eq!(vid, 30);
        assert_eq!(fid, "01637037d6");
        assert_eq!(filename, None);
        assert_eq!(ext, ".jpg");

        let (vid, fid, filename, ext) = parse_url_path("/300/01637037d6").unwrap();
        assert_eq!(vid, 300);
        assert_eq!(fid, "01637037d6");
        assert_eq!(filename, None);
        assert_eq!(ext, "");

        let (vid, fid, filename, ext) = parse_url_path("/300,01637037d6").unwrap();
        assert_eq!(vid, 300);
        assert_eq!(fid, "01637037d6");
        assert_eq!(filename, None);
        assert_eq!(ext, "");
    }
}
