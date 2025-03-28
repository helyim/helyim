use std::{
    fmt::Debug,
    net::{AddrParseError, SocketAddr},
    num::ParseIntError,
    path::Path,
};

use nom::{
    IResult,
    branch::alt,
    bytes::complete::take_till,
    character::complete::{alphanumeric1, char, digit1},
    combinator::opt,
    sequence::{pair, tuple},
};

use crate::types::VolumeId;

pub fn parse_url_path(input: &str) -> Result<(VolumeId, &str, &str, &str), ParseError> {
    let (vid, fid, filename, ext) = tuple((
        char('/'),
        parse_vid_fid,
        opt(pair(char('/'), parse_filename)),
    ))(input)
    .map(|(input, (_, (vid, fid), filename))| {
        (vid, fid, filename.map(|(_, filename)| filename), input)
    })
    .map_err(|err| ParseError::Box(Box::new(err.to_owned())))?;
    Ok((vid.parse()?, fid, filename.unwrap_or(""), ext))
}

pub fn parse_vid_fid(input: &str) -> IResult<&str, (&str, &str)> {
    let (input, (vid, _, fid)) =
        tuple((digit1, alt((char('/'), char(','))), alphanumeric1))(input)?;
    Ok((input, (vid, fid)))
}

pub fn parse_filename(input: &str) -> IResult<&str, &str> {
    let (input, filename) = take_till(|c| c == '.')(input)?;
    Ok((input, filename))
}

pub fn parse_host_port(addr: &str) -> Result<(String, u16), ParseIntError> {
    match addr.rfind(':') {
        Some(idx) => {
            let port = addr[idx + 1..].parse::<u16>()?;
            Ok((addr[..idx].to_string(), port))
        }
        None => Ok((addr.to_string(), 80)),
    }
}

pub fn parse_collection_volume_id(name: &str) -> Result<(VolumeId, &str), ParseIntError> {
    match name.find('_') {
        Some(index) => {
            let (collection, vid) = (&name[0..index], &name[index + 1..]);
            Ok((vid.parse()?, collection))
        }
        None => Ok((name.parse()?, "")),
    }
}

pub fn parse_ext(filename: &str) -> String {
    match Path::new(filename).extension() {
        Some(ext) => ext.to_str().unwrap_or("").to_string(),
        None => String::new(),
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ParseError {
    #[error("{0}")]
    Box(Box<dyn std::error::Error + Sync + Send>),
    #[error("ParseInt error: {0}")]
    ParseInt(#[from] ParseIntError),
    #[error("AddrParse error: {0}")]
    AddrParse(#[from] AddrParseError),
}

pub fn parse_int<I: std::str::FromStr>(num: &str) -> Result<I, ParseError>
where
    ParseError: From<<I as std::str::FromStr>::Err>,
{
    Ok(num.parse::<I>()?)
}

pub fn parse_addr(addr: &str) -> Result<SocketAddr, ParseError> {
    Ok(addr.parse()?)
}

#[cfg(test)]
mod tests {
    use crate::parser::{parse_filename, parse_url_path, parse_vid_fid};

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
        assert_eq!(filename, "my_preferred_name");
        assert_eq!(ext, ".jpg");

        let (vid, fid, filename, ext) = parse_url_path("/3/01637037d6/my_preferred_name").unwrap();
        assert_eq!(vid, 3);
        assert_eq!(fid, "01637037d6");
        assert_eq!(filename, "my_preferred_name");
        assert_eq!(ext, "");

        let (vid, fid, filename, ext) = parse_url_path("/3/01637037d6.jpg").unwrap();
        assert_eq!(vid, 3);
        assert_eq!(fid, "01637037d6");
        assert_eq!(filename, "");
        assert_eq!(ext, ".jpg");

        let (vid, fid, filename, ext) = parse_url_path("/30,01637037d6.jpg").unwrap();
        assert_eq!(vid, 30);
        assert_eq!(fid, "01637037d6");
        assert_eq!(filename, "");
        assert_eq!(ext, ".jpg");

        let (vid, fid, filename, ext) = parse_url_path("/300/01637037d6").unwrap();
        assert_eq!(vid, 300);
        assert_eq!(fid, "01637037d6");
        assert_eq!(filename, "");
        assert_eq!(ext, "");

        let (vid, fid, filename, ext) = parse_url_path("/300,01637037d6").unwrap();
        assert_eq!(vid, 300);
        assert_eq!(fid, "01637037d6");
        assert_eq!(filename, "");
        assert_eq!(ext, "");
    }
}
