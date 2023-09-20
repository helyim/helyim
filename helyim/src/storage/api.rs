use std::{
    collections::HashMap, convert::Infallible, fmt::Display, io::Read, ops::Add,
    result::Result as StdResult, str::FromStr, sync::Arc, time, time::Duration,
};

use axum::{
    extract::{Query, State},
    headers,
    http::{HeaderMap, Response},
    response::{Html, IntoResponse},
    Json, TypedHeader,
};
use axum_macros::FromRequest;
use bytes::Bytes;
use faststr::FastStr;
use futures::{lock::Mutex, stream::once};
use hyper::{
    header::{
        HeaderValue, ACCEPT_ENCODING, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE,
        IF_MODIFIED_SINCE, IF_NONE_MATCH, LAST_MODIFIED,
    },
    http::HeaderName,
    Body, Method, StatusCode,
};
use libflate::gzip::Decoder;
use mime_guess::mime;
use multer::Multipart;
use nom::{
    branch::alt,
    bytes::complete::take_till,
    character::complete::{alphanumeric1, char as nom_char, digit1},
    combinator::opt,
    sequence::{pair, tuple},
    IResult,
};
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::info;

use crate::{
    anyhow,
    errors::{Error, Result},
    images::FAVICON_ICO,
    operation::{LookerEventTx, Upload},
    storage::{
        crc,
        needle::{Needle, PAIR_NAME_PREFIX},
        needle_map::NeedleMapType,
        store::Store,
        Ttl, VolumeId, VolumeInfo,
    },
    util,
    util::time::now,
    PHRASE,
};

#[derive(Clone)]
pub struct StorageContext {
    pub store: Arc<Mutex<Store>>,
    pub needle_map_type: NeedleMapType,
    pub read_redirect: bool,
    pub pulse_seconds: u64,
    pub looker: LookerEventTx,
}

pub async fn status_handler(State(ctx): State<StorageContext>) -> Result<Json<Value>> {
    let store = ctx.store.lock().await;

    let mut infos: Vec<VolumeInfo> = vec![];
    for location in store.locations.iter() {
        for (_, volume) in location.volumes.iter() {
            let volume_info = volume.volume_info().await?;
            infos.push(volume_info);
        }
    }

    let stat = json!({
        "version": "0.1",
        "volumes": &infos,
    });

    Ok(Json(stat))
}

pub async fn volume_clean_handler(State(ctx): State<StorageContext>) -> Result<()> {
    let mut store = ctx.store.lock().await;
    for location in store.locations.iter_mut() {
        for (vid, volume) in location.volumes.iter() {
            info!("start compacting volume {vid}.");
            volume.compact(0).await?;
            info!("compact volume {vid} success.");
        }
    }

    Ok(())
}

pub async fn volume_commit_compact_handler(State(ctx): State<StorageContext>) -> Result<()> {
    let mut store = ctx.store.lock().await;
    for location in store.locations.iter_mut() {
        for (vid, volume) in location.volumes.iter() {
            info!("start committing compacted volume {vid}.");
            volume.commit_compact().await?;
            info!("commit compacted volume {vid} success.");
        }
    }

    Ok(())
}

pub async fn fallback_handler(
    State(ctx): State<StorageContext>,
    extractor: StorageExtractor,
) -> Result<FallbackResponse> {
    match extractor.method {
        Method::GET => match extractor.uri.path() {
            "/" => Ok(FallbackResponse::Default(Html(PHRASE))),
            "/favicon.ico" => Ok(FallbackResponse::Favicon),
            _ => get_or_head_handler(State(ctx), extractor).await,
        },
        Method::HEAD => get_or_head_handler(State(ctx), extractor).await,
        Method::POST => post_handler(State(ctx), extractor).await,
        Method::DELETE => delete_handler(State(ctx), extractor).await,
        _ => Ok(FallbackResponse::Default(Html(PHRASE))),
    }
}

pub enum FallbackResponse {
    Favicon,
    Default(Html<&'static str>),
    GetOrHead(Response<Body>),
    Post(Json<Upload>),
    Delete(Json<Value>),
}

impl IntoResponse for FallbackResponse {
    fn into_response(self) -> axum::response::Response {
        match self {
            FallbackResponse::GetOrHead(res) => res.into_response(),
            FallbackResponse::Post(json) => json.into_response(),
            FallbackResponse::Delete(json) => json.into_response(),
            FallbackResponse::Default(default) => default.into_response(),
            FallbackResponse::Favicon => FAVICON_ICO.bytes().into_response(),
        }
    }
}

#[derive(Debug, FromRequest)]
pub struct StorageExtractor {
    // only the last field can implement `FromRequest`
    // other fields must only implement `FromRequestParts`
    uri: axum::http::Uri,
    method: Method,
    headers: HeaderMap,
    #[from_request(via(TypedHeader))]
    host: headers::Host,
    #[from_request(via(Query))]
    query: StorageQuery,
    body: Bytes,
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
    State(ctx): State<StorageContext>,
    extractor: StorageExtractor,
) -> Result<FallbackResponse> {
    let (vid, fid, _, _) = parse_url_path(extractor.uri.path())?;
    let is_replicate = extractor.query.r#type == Some("replicate".into());

    let mut needle = Needle::default();
    needle.parse_path(fid)?;

    let cookie = needle.cookie;

    {
        let mut store = ctx.store.lock().await;
        needle = store.read_volume_needle(vid, needle).await?;
        if cookie != needle.cookie {
            info!(
                "cookie not match from {:?} recv: {}, file is {}",
                extractor.host, cookie, needle.cookie
            );
            return Err(Error::CookieNotMatch(needle.cookie, cookie));
        }
    }

    let size = replicate_delete(&ctx, extractor.uri.path(), vid, needle, is_replicate).await?;
    let size = json!({ "size": size });

    Ok(FallbackResponse::Delete(Json(size)))
}

async fn replicate_delete(
    ctx: &StorageContext,
    path: &str,
    vid: VolumeId,
    needle: Needle,
    is_replicate: bool,
) -> Result<u32> {
    let mut store = ctx.store.lock().await;
    let local_url = format!("{}:{}", store.ip, store.port);
    let size = store.delete_volume_needle(vid, needle).await?;
    if is_replicate {
        return Ok(size);
    }

    if let Some(volume) = store.find_volume(vid) {
        if !volume.need_to_replicate().await? {
            return Ok(size);
        }
    }

    let params = vec![("type", "replicate")];
    let mut volume_locations = ctx.looker.lookup(vec![vid]).await?;

    if let Some(volume_location) = volume_locations.pop() {
        // TODO concurrent replicate
        for location in volume_location.locations.iter() {
            if location.url == local_url {
                continue;
            }
            let url = format!("http://{}{}", &location.url, path);
            util::delete(&url, &params).await.and_then(|body| {
                let value: Value = serde_json::from_slice(&body)?;
                if let Some(err) = value["error"].as_str() {
                    return if err.is_empty() {
                        Ok(())
                    } else {
                        Err(anyhow!("write {} err: {err}", location.url))
                    };
                }

                Ok(())
            })?;
        }
    }
    Ok(size)
}

pub async fn post_handler(
    State(ctx): State<StorageContext>,
    extractor: StorageExtractor,
) -> Result<FallbackResponse> {
    let (vid, _, _, _) = parse_url_path(extractor.uri.path())?;
    let is_replicate = extractor.query.r#type == Some("replicate".into());

    let mut needle = if !is_replicate {
        new_needle_from_request(&extractor).await?
    } else {
        bincode::deserialize(&extractor.body)?
    };

    needle = replicate_write(&ctx, extractor.uri.path(), vid, needle, is_replicate).await?;
    let mut upload = Upload {
        size: needle.data_size(),
        ..Default::default()
    };
    if needle.has_name() {
        upload.name = String::from_utf8(needle.name.to_vec())?;
    }

    // TODO: add etag support
    Ok(FallbackResponse::Post(Json(upload)))
}

async fn replicate_write(
    ctx: &StorageContext,
    path: &str,
    vid: VolumeId,
    mut needle: Needle,
    is_replicate: bool,
) -> Result<Needle> {
    let mut store = ctx.store.lock().await;
    let local_url = format!("{}:{}", store.ip, store.port);
    needle = store.write_volume_needle(vid, needle).await?;
    if is_replicate {
        return Ok(needle);
    }

    if let Some(volume) = store.find_volume(vid) {
        if !volume.need_to_replicate().await? {
            return Ok(needle);
        }
    }

    let params = vec![("type", "replicate")];
    let data = bincode::serialize(&needle)?;

    let mut volume_locations = ctx.looker.lookup(vec![vid]).await?;

    if let Some(volume_location) = volume_locations.pop() {
        // TODO concurrent replicate
        for location in volume_location.locations.iter() {
            if location.url == local_url {
                continue;
            }
            let url = format!("http://{}{}", location.url, path);
            util::post(&url, &params, &data).await.and_then(|body| {
                let value: Value = serde_json::from_slice(&body)?;
                if let Some(err) = value["error"].as_str() {
                    return if err.is_empty() {
                        Ok(())
                    } else {
                        Err(anyhow!("write {} err: {err}", location.url))
                    };
                }
                Ok(())
            })?;
        }
    }

    Ok(needle)
}

async fn new_needle_from_request(extractor: &StorageExtractor) -> Result<Needle> {
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
        parse_upload.modified_time = now().as_secs();
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

pub fn get_boundary(extractor: &StorageExtractor) -> Result<String> {
    const BOUNDARY: &str = "boundary=";

    if extractor.method != Method::POST {
        return Err(anyhow!("parse multipart err: not post request"));
    }

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

pub struct ParseUpload {
    pub filename: String,
    pub data: Vec<u8>,
    pub mime_type: String,
    pub pair_map: HashMap<String, String>,
    pub modified_time: u64,
    pub ttl: Ttl,
    pub is_chunked_file: bool,
}

impl Display for ParseUpload {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "filename: {}, data_len: {}, mime_type: {}, ttl minutes: {}, is_chunked_file: {}",
            self.filename,
            self.data.len(),
            self.mime_type,
            self.ttl.minutes(),
            self.is_chunked_file
        )
    }
}

pub async fn parse_upload(extractor: &StorageExtractor) -> Result<ParseUpload> {
    let mut filename = String::new();
    let mut data = vec![];
    let mut mime_type = String::new();
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
    let mut post_mtype = String::new();
    while let Ok(Some(field)) = mpart.next_field().await {
        if let Some(name) = field.file_name() {
            filename = name.to_string();
            if let Some(content_type) = field.content_type() {
                post_mtype.push_str(content_type.type_().as_str());
                post_mtype.push('/');
                post_mtype.push_str(content_type.subtype().as_str());
                data.clear();
                data.extend(field.bytes().await?);
            }
        }

        if !filename.is_empty() {
            break;
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
            mime_type = post_mtype.clone();
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
        mime_type,
        pair_map,
        modified_time,
        ttl,
        is_chunked_file,
    };

    Ok(resp)
}

pub async fn get_or_head_handler(
    State(ctx): State<StorageContext>,
    extractor: StorageExtractor,
) -> Result<FallbackResponse> {
    let (vid, fid, _filename, _ext) = parse_url_path(extractor.uri.path())?;
    let mut needle = Needle::default();
    needle.parse_path(fid)?;
    let cookie = needle.cookie;

    let mut store = ctx.store.lock().await;
    let mut response = Response::new(Body::empty());

    if !store.has_volume(vid) {
        // TODO: support read redirect
        if !ctx.read_redirect {
            info!("volume is not belongs to this server, volume: {}", vid);
            *response.status_mut() = StatusCode::NOT_FOUND;
            return Ok(FallbackResponse::GetOrHead(response));
        }
    }

    needle = store.read_volume_needle(vid, needle).await?;
    if needle.cookie != cookie {
        return Err(Error::CookieNotMatch(needle.cookie, cookie));
    }

    if needle.last_modified != 0 {
        let modified = time::UNIX_EPOCH.add(Duration::new(needle.last_modified, 0));
        let modified = HeaderValue::from(
            modified
                .duration_since(time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        );
        response
            .headers_mut()
            .insert(LAST_MODIFIED, modified.clone());

        if let Some(since) = extractor.headers.get(IF_MODIFIED_SINCE) {
            if since <= modified {
                *response.status_mut() = StatusCode::NOT_MODIFIED;
                return Ok(FallbackResponse::GetOrHead(response));
            }
        }
    }

    let etag = needle.etag();
    if let Some(not_match) = extractor.headers.get(IF_NONE_MATCH) {
        if not_match == etag.as_str() {
            *response.status_mut() = StatusCode::NOT_MODIFIED;
            return Ok(FallbackResponse::GetOrHead(response));
        }
    }

    if needle.has_pairs() {
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

    let mut _mtype = String::new();
    if !needle.mime.is_empty() && !needle.mime.starts_with(b"application/octet-stream") {
        _mtype = String::from_utf8(needle.mime.to_vec())?;
    }

    if needle.is_gzipped() {
        let all_headers = extractor.headers.get_all(ACCEPT_ENCODING);
        let mut gzip = false;
        for value in all_headers.iter() {
            if value.to_str()?.contains("gzip") {
                gzip = true;
                break;
            }
        }
        if gzip {
            response
                .headers_mut()
                .insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
        } else {
            let mut decoded = Vec::new();
            {
                let mut decoder = Decoder::new(&needle.data[..])?;
                decoder.read_to_end(&mut decoded)?;
            }
            needle.data = Bytes::from(decoded);
        }
    }

    response
        .headers_mut()
        .insert(CONTENT_LENGTH, HeaderValue::from(needle.data.len()));
    *response.body_mut() = Body::from(needle.data);
    *response.status_mut() = StatusCode::ACCEPTED;

    Ok(FallbackResponse::GetOrHead(response))
}

fn parse_url_path(input: &str) -> Result<(VolumeId, &str, Option<&str>, Option<&str>)> {
    let (_, ((vid, fid), filename, ext)) =
        tuple((parse_vid_fid, opt(parse_filename), opt(parse_ext)))(input)?;
    Ok((vid.parse()?, fid, filename, ext))
}

fn parse_vid_fid(input: &str) -> IResult<&str, (&str, &str)> {
    let (input, (_, vid, _, fid)) = tuple((
        nom_char('/'),
        digit1,
        alt((nom_char('/'), nom_char(','))),
        alphanumeric1,
    ))(input)?;
    Ok((input, (vid, fid)))
}

fn parse_filename(input: &str) -> IResult<&str, &str> {
    let (input, (_, filename)) = pair(nom_char('/'), take_till(|c| c == '.'))(input)?;
    Ok((input, filename))
}

fn parse_ext(input: &str) -> IResult<&str, &str> {
    let (ext, _) = nom_char('.')(input)?;
    Ok((ext, ext))
}

#[cfg(test)]
mod tests {
    use crate::storage::api::{parse_ext, parse_filename, parse_url_path, parse_vid_fid};

    #[test]
    pub fn test_parse_vid_fid() {
        let (input, (vid, fid)) = parse_vid_fid("/3/01637037d6").unwrap();
        assert_eq!(vid, "3");
        assert_eq!(fid, "01637037d6");
        assert_eq!(input, "");

        let (input, (vid, fid)) = parse_vid_fid("/3/01637037d6/").unwrap();
        assert_eq!(vid, "3");
        assert_eq!(fid, "01637037d6");
        assert_eq!(input, "/");

        let (input, (vid, fid)) = parse_vid_fid("/3,01637037d6").unwrap();
        assert_eq!(vid, "3");
        assert_eq!(fid, "01637037d6");
        assert_eq!(input, "");

        let (input, (vid, fid)) = parse_vid_fid("/3,01637037d6/").unwrap();
        assert_eq!(vid, "3");
        assert_eq!(fid, "01637037d6");
        assert_eq!(input, "/");
    }

    #[test]
    pub fn test_parse_filename() {
        let (input, filename) = parse_filename("/my_preferred_name.jpg").unwrap();
        assert_eq!(filename, "my_preferred_name");
        assert_eq!(input, ".jpg");

        let (input, filename) = parse_filename("/my_preferred_name").unwrap();
        assert_eq!(filename, "my_preferred_name");
        assert_eq!(input, "");

        let (input, filename) = parse_filename("/").unwrap();
        assert_eq!(filename, "");
        assert_eq!(input, "");
    }

    #[test]
    pub fn test_parse_ext() {
        let (input, ext) = parse_ext(".jpg").unwrap();
        assert_eq!(input, ext);
        assert_eq!(ext, "jpg");
    }

    #[test]
    pub fn test_parse_path() {
        let (vid, fid, filename, ext) = parse_url_path("/3/01637037d6/my_preferred_name.jpg").unwrap();
        assert_eq!(vid, 3);
        assert_eq!(fid, "01637037d6");
        assert_eq!(filename, Some("my_preferred_name"));
        assert_eq!(ext, Some("jpg"));

        let (vid, fid, filename, ext) = parse_url_path("/3/01637037d6/my_preferred_name").unwrap();
        assert_eq!(vid, 3);
        assert_eq!(fid, "01637037d6");
        assert_eq!(filename, Some("my_preferred_name"));
        assert_eq!(ext, None);

        let (vid, fid, filename, ext) = parse_url_path("/3/01637037d6.jpg").unwrap();
        assert_eq!(vid, 3);
        assert_eq!(fid, "01637037d6");
        assert_eq!(filename, None);
        assert_eq!(ext, Some("jpg"));

        let (vid, fid, filename, ext) = parse_url_path("/3,01637037d6.jpg").unwrap();
        assert_eq!(vid, 3);
        assert_eq!(fid, "01637037d6");
        assert_eq!(filename, None);
        assert_eq!(ext, Some("jpg"));

        let (vid, fid, filename, ext) = parse_url_path("/3/01637037d6").unwrap();
        assert_eq!(vid, 3);
        assert_eq!(fid, "01637037d6");
        assert_eq!(filename, None);
        assert_eq!(ext, None);

        let (vid, fid, filename, ext) = parse_url_path("/3,01637037d6").unwrap();
        assert_eq!(vid, 3);
        assert_eq!(fid, "01637037d6");
        assert_eq!(filename, None);
        assert_eq!(ext, None);
    }
}
