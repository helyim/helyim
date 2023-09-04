use std::{
    collections::HashMap, convert::Infallible, fmt::Display, io::Read, ops::Add, path::Path,
    result::Result as StdResult, str::FromStr, sync::Arc, time, time::Duration,
};

use axum::{
    extract::{Query, State},
    headers,
    http::HeaderMap,
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
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::{debug, info};

use crate::{
    anyhow,
    errors::{Error, Result},
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
    Response,
};

#[derive(Clone)]
pub struct StorageContext {
    pub store: Arc<Mutex<Store>>,
    pub needle_map_type: NeedleMapType,
    pub read_redirect: bool,
    pub pulse_seconds: u64,
    pub master_node: FastStr,
    pub looker: LookerEventTx,
}

pub async fn status_handler(State(ctx): State<StorageContext>) -> Result<Json<Value>> {
    let store = ctx.store.lock().await;

    let mut infos: Vec<VolumeInfo> = vec![];
    for location in store.locations.iter() {
        for (_, v) in location.volumes.iter() {
            let vinfo = v.get_volume_info();
            infos.push(vinfo);
        }
    }

    let stat = json!({
        "version": "0.1",
        "volumes": &infos,
    });

    Ok(Json(stat))
}

#[derive(Debug, Deserialize)]
pub struct AssignVolumeRequest {
    volume: Option<FastStr>,
    replication: Option<FastStr>,
    ttl: Option<FastStr>,
    preallocate: Option<i64>,
    collection: Option<FastStr>,
}

pub async fn assign_volume_handler(
    State(ctx): State<StorageContext>,
    Query(request): Query<AssignVolumeRequest>,
) -> Result<()> {
    let mut store = ctx.store.lock().await;
    store.add_volume(
        &request.volume.unwrap_or_default(),
        &request.collection.unwrap_or_default(),
        ctx.needle_map_type,
        &request.replication.unwrap_or_default(),
        &request.ttl.unwrap_or_default(),
        request.preallocate.unwrap_or_default(),
    )?;

    Ok(())
}

pub fn get_boundary(extractor: &PostExtractor) -> Result<String> {
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

pub struct ParseUploadResp {
    pub filename: String,
    pub data: Vec<u8>,
    pub mime_type: String,
    pub pair_map: HashMap<String, String>,
    pub modified_time: u64,
    pub ttl: Ttl,
    pub is_chunked_file: bool,
}

impl Display for ParseUploadResp {
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

pub async fn parse_upload(extractor: &PostExtractor) -> Result<ParseUploadResp> {
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

    // get first file with file_name
    let mut post_mtype = String::new();
    while let Ok(Some(field)) = mpart.next_field().await {
        debug!("field name: {:?}", field.name());
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
            mime_type = post_mtype.clone(); // only return if not deductible, so my can save it only
                                            // when can't deductible from file name
                                            // guess_mtype = post_mtype.clone();
        }
        // don't auto gzip and change filename like seaweed
    }

    let modified_time = extractor.query.ts.unwrap_or(0);

    let ttl = match &extractor.query.ttl {
        Some(s) => Ttl::new(s).unwrap_or_default(),
        None => Ttl::default(),
    };

    let resp = ParseUploadResp {
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

#[derive(FromRequest)]
pub struct DeleteExtractor {
    uri: axum::http::Uri,
    #[from_request(via(TypedHeader))]
    host: headers::Host,
    #[from_request(via(Query))]
    query: DeleteQuery,
}

#[derive(Deserialize)]
pub struct DeleteQuery {
    r#type: Option<FastStr>,
}

pub async fn delete_handler(
    State(ctx): State<StorageContext>,
    extractor: DeleteExtractor,
) -> Result<Value> {
    let (svid, fid, _, _, _) = parse_url_path(extractor.uri.path());
    let vid = svid.parse::<u32>()?;
    let is_replicate = extractor.query.r#type == Some("replicate".into());

    let mut needle = Needle::default();
    needle.parse_path(&fid)?;

    let cookie = needle.cookie;

    {
        let mut store = ctx.store.lock().await;
        store.read_volume_needle(vid, &mut needle)?;
        if cookie != needle.cookie {
            info!(
                "cookie not match from {:?} recv: {}, file is {}",
                extractor.host, cookie, needle.cookie
            );
            return Err(Error::CookieNotMatch(needle.cookie, cookie));
        }
    }

    let size = replicate_delete(&ctx, extractor.uri.path(), vid, &mut needle, is_replicate).await?;
    let size = json!({ "size": size });

    Ok(size)
}

async fn replicate_delete(
    ctx: &StorageContext,
    path: &str,
    vid: VolumeId,
    n: &mut Needle,
    is_replicate: bool,
) -> Result<u32> {
    let mut s = ctx.store.lock().await;
    let local_url = format!("{}:{}", s.ip, s.port);
    let size = s.delete_volume_needle(vid, n).await?;
    if is_replicate {
        return Ok(size);
    }

    if let Some(volume) = s.find_volume_mut(vid) {
        if !volume.need_to_replicate() {
            return Ok(size);
        }
    }

    let params = vec![("type", "replicate")];
    let res = ctx.looker.lookup(vid).await?;
    debug!("get lookup res: {:?}", res);

    // TODO concurrent replicate
    for location in res.locations.iter() {
        if location.url == local_url {
            continue;
        }
        let url = format!("http://{}{}", &location.url, path);
        util::delete(&url, &params).await.and_then(|body| {
            let value: serde_json::Value = serde_json::from_slice(&body)?;
            if let Some(err) = value["error"].as_str() {
                return if err.is_empty() {
                    Ok(())
                } else {
                    Err(anyhow!("write {} err: {}", location.url, err))
                };
            }

            Ok(())
        })?;
    }
    Ok(size)
}

#[derive(FromRequest)]
pub struct PostExtractor {
    // only the last field can implement `FromRequest`
    // other fields must only implement `FromRequestParts`
    uri: axum::http::Uri,
    method: Method,
    headers: HeaderMap,
    #[from_request(via(TypedHeader))]
    host: headers::Host,
    #[from_request(via(Query))]
    query: PostQuery,
    body: Bytes,
}

#[derive(Deserialize)]
pub struct PostQuery {
    r#type: Option<FastStr>,
    // is chunked file
    cm: Option<bool>,
    ttl: Option<FastStr>,
    // last modified
    ts: Option<u64>,
}

pub async fn post_handler(
    State(ctx): State<StorageContext>,
    extractor: PostExtractor,
) -> Result<Json<Upload>> {
    let (svid, _, _, _, _) = parse_url_path(extractor.uri.path());
    let vid = svid.parse::<u32>()?;
    let is_replicate = extractor.query.r#type == Some("replicate".into());

    let mut needle = if !is_replicate {
        new_needle_from_request(&extractor).await?
    } else {
        bincode::deserialize(&extractor.body)?
    };

    let size = replicate_write(&ctx, extractor.uri.path(), vid, &mut needle, is_replicate).await?;
    let mut upload = Upload::default();
    if needle.has_name() {
        upload.name = String::from_utf8(needle.name.to_vec())?;
    }
    upload.size = size;

    // TODO: add etag support
    Ok(Json(upload))
}

async fn new_needle_from_request(extractor: &PostExtractor) -> Result<Needle> {
    let mut resp = parse_upload(extractor).await?;

    let mut needle = Needle {
        data: Bytes::from(resp.data),
        ..Default::default()
    };

    if !resp.pair_map.is_empty() {
        needle.set_has_pairs();
        needle.pairs = Bytes::from(serde_json::to_vec(&resp.pair_map)?);
    }

    if !resp.filename.is_empty() {
        needle.name = Bytes::from(resp.filename);
        needle.set_name();
    }

    if resp.mime_type.len() < 256 {
        needle.mime = Bytes::from(resp.mime_type);
        needle.set_has_mime();
    }

    // if resp.is_gzipped {
    //     n.set_gzipped();
    // }

    if resp.modified_time == 0 {
        resp.modified_time = now().as_secs();
    }
    needle.last_modified = resp.modified_time;
    needle.set_has_last_modified_date();

    if resp.ttl.minutes() != 0 {
        needle.ttl = resp.ttl;
        needle.set_has_ttl();
    }

    if resp.is_chunked_file {
        needle.set_is_chunk_manifest();
    }

    needle.checksum = crc::checksum(&needle.data);

    let path = extractor.uri.path();
    let start = path.find(',').map(|idx| idx + 1).unwrap_or(0);
    let end = path.rfind('.').unwrap_or(path.len());

    needle.parse_path(&path[start..end])?;

    Ok(needle)
}

#[derive(FromRequest)]
pub struct GetOrHeadExtractor {
    uri: axum::http::Uri,
    headers: HeaderMap,
}

pub async fn get_or_head_handler(
    State(ctx): State<StorageContext>,
    extractor: GetOrHeadExtractor,
) -> Result<Response> {
    let (svid, fid, mut filename, _ext, _) = parse_url_path(extractor.uri.path());

    let vid = svid.parse::<u32>()?;

    let mut needle = Needle::default();
    needle.parse_path(&fid)?;
    let cookie = needle.cookie;

    let mut store = ctx.store.lock().await;
    let mut resp = Response::new(Body::empty());

    if !store.has_volume(vid) {
        // TODO support read redirect
        if !ctx.read_redirect {
            info!("volume is not belongs to this server, volume: {}", vid);
            *resp.status_mut() = StatusCode::NOT_FOUND;
            return Ok(resp);
        }
    }

    store.read_volume_needle(vid, &mut needle)?;
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
        resp.headers_mut().insert(LAST_MODIFIED, modified.clone());

        if let Some(since) = extractor.headers.get(IF_MODIFIED_SINCE) {
            if since <= modified {
                *resp.status_mut() = StatusCode::NOT_MODIFIED;
                return Ok(resp);
            }
        }
    }

    let etag = needle.etag();
    if let Some(not_match) = extractor.headers.get(IF_NONE_MATCH) {
        if not_match == etag.as_str() {
            *resp.status_mut() = StatusCode::NOT_MODIFIED;
            return Ok(resp);
        }
    }

    if needle.has_pairs() {
        let pairs: Value = serde_json::from_slice(&needle.pairs)?;
        if let Some(map) = pairs.as_object() {
            for (k, v) in map {
                if let Some(value) = v.as_str() {
                    resp.headers_mut()
                        .insert(HeaderName::from_str(k)?, HeaderValue::from_str(value)?);
                }
            }
        }
    }

    if !needle.name.is_empty() && filename.is_empty() {
        filename = String::from_utf8(needle.name.to_vec())?;
    }

    let mut mtype = String::new();
    if !needle.mime.is_empty() && !needle.mime.starts_with(b"application/octet-stream") {
        mtype = String::from_utf8(needle.mime.to_vec())?;
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
            resp.headers_mut()
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

    resp = write_response_content(&filename, &mtype, resp, needle.data);
    *resp.status_mut() = StatusCode::ACCEPTED;

    Ok(resp)
}

fn write_response_content(
    _filename: &str,
    _mtype: &str,
    mut resp: Response,
    data: Bytes,
) -> Response {
    // TODO handle range contenttype and...
    resp.headers_mut()
        .insert(CONTENT_LENGTH, HeaderValue::from(data.len()));
    *resp.body_mut() = Body::from(data);
    resp
}

// support following format
// http://localhost:8080/3/01637037d6/my_preferred_name.jpg
// http://localhost:8080/3/01637037d6.jpg
// http://localhost:8080/3,01637037d6.jpg
// http://localhost:8080/3/01637037d6
// http://localhost:8080/3,01637037d6
// @return vid, fid, filename, ext, is_volume_id_only
// /3/01637037d6/my_preferred_name.jpg -> (3,01637037d6,my_preferred_name.jpg,jpg,false)
fn parse_url_path(path: &str) -> (String, String, String, String, bool) {
    let vid: String;
    let mut fid;
    let filename;
    let mut ext = String::default();
    let mut is_volume_id_only = false;

    let parts: Vec<&str> = path.split('/').collect();
    debug!("parse url path: {}", path);
    match parts.len() {
        4 => {
            vid = parts[1].to_string();
            fid = parts[2].to_string();
            filename = parts[3].to_string();

            // must be valid utf8
            ext = Path::new(&filename)
                .extension()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string();
        }
        3 => {
            filename = String::default();

            vid = parts[1].to_string();
            fid = parts[2].to_string();
            if let Some(idx) = parts[2].rfind('.') {
                let (fid_str, ext_str) = parts[2].split_at(idx);
                fid = fid_str.to_string();
                ext = ext_str.to_string();
            }
        }
        _ => {
            filename = String::default();
            let mut end = path.len();

            if let Some(dot) = path.rfind('.') {
                let start = dot + 1;
                ext = path[start..].to_string();
                end = start - 1;
            }

            match path.rfind(',') {
                Some(sep) => {
                    let start = sep + 1;
                    fid = path[start..end].to_string();
                    end = start - 1;
                }
                None => {
                    fid = String::default();
                    is_volume_id_only = true;
                }
            }

            vid = path[1..end].to_string();
        }
    };

    (vid, fid, filename, ext, is_volume_id_only)
}

async fn replicate_write(
    ctx: &StorageContext,
    path: &str,
    vid: VolumeId,
    n: &mut Needle,
    is_replicate: bool,
) -> Result<u32> {
    let mut s = ctx.store.lock().await;
    let local_url = format!("{}:{}", s.ip, s.port);
    let size = s.write_volume_needle(vid, n).await?;
    if is_replicate {
        return Ok(size);
    }

    if let Some(volume) = s.find_volume_mut(vid) {
        if !volume.need_to_replicate() {
            return Ok(size);
        }
    }

    let params = vec![("type", "replicate")];
    let data = bincode::serialize(n)?;

    let res = ctx.looker.lookup(vid).await?;
    debug!("get lookup res: {:?}", res);

    // TODO concurrent replicate
    for location in res.locations.iter() {
        if location.url == local_url {
            continue;
        }
        let url = format!("http://{}{}", location.url, path);
        util::post(&url, &params, &data).await.and_then(|body| {
            let value: serde_json::Value = serde_json::from_slice(&body)?;
            if let Some(err) = value["error"].as_str() {
                return if err.is_empty() {
                    Ok(())
                } else {
                    Err(anyhow!("write {} err: {}", location.url, err))
                };
            }
            Ok(())
        })?;
    }

    Ok(size)
}
