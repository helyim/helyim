#[macro_use]
pub mod macros;

mod http;
use std::{collections::HashMap, fmt::Display};

pub use http::{delete, get, post};
use hyper::{
    header::{HeaderValue, CONTENT_LENGTH, CONTENT_TYPE},
    Body, StatusCode,
};
use serde_json::json;
use url::Url;

use crate::{errors::Result, Request, Response, DEFAULT};

pub mod time;

pub fn get_request_params(req: &Request) -> HashMap<String, String> {
    // need base or will parse err
    let s = format!("http://127.0.0.1{}", req.uri());
    let url = Url::parse(&s).unwrap();
    let pairs = url.query_pairs().into_owned();
    pairs.collect()
}

///  returns the boolean value represented by the string. It accepts 1, t, T, TRUE, true, True, 0,
/// f, F, FALSE, false, False. Any other value returns an error.
pub fn parse_bool(s: &str) -> Result<bool> {
    match s {
        "1" => Ok(true),
        "t" => Ok(true),
        "T" => Ok(true),
        "TRUE" => Ok(true),
        "true" => Ok(true),
        "True" => Ok(true),
        "0" => Ok(false),
        "f" => Ok(false),
        "F" => Ok(false),
        "FALSE" => Ok(false),
        "false" => Ok(false),
        "False" => Ok(false),
        _ => Err(anyhow!("no valid boolean value: {}", s)),
    }
}

pub fn error_json_response<E: Display>(error: E) -> Response {
    let msg = format!("{}", error);

    let to_j = json!({
        "error": &msg,
    });

    let j = serde_json::to_string(&to_j).unwrap();
    let len = j.len();
    let mut resp = Response::new(Body::from(j));
    *resp.status_mut() = StatusCode::BAD_REQUEST;
    resp.headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    resp.headers_mut()
        .insert(CONTENT_LENGTH, HeaderValue::from(len));

    resp
}

pub fn json_response<J: serde::ser::Serialize>(status: StatusCode, to_j: &J) -> Result<Response> {
    let j = serde_json::to_string(to_j)?;
    let len = j.len();
    let mut resp = Response::new(Body::from(j));
    *resp.status_mut() = status;
    resp.headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    resp.headers_mut()
        .insert(CONTENT_LENGTH, HeaderValue::from(len));

    Ok(resp)
}

pub fn get_or_default(s: String) -> String {
    if s.is_empty() {
        String::from(DEFAULT)
    } else {
        s
    }
}
