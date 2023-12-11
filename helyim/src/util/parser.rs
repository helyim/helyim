use nom::{
    branch::alt,
    bytes::complete::take_till,
    character::complete::{alphanumeric1, char, digit1},
    combinator::opt,
    sequence::{pair, tuple},
    IResult,
};

use crate::storage::{VolumeError, VolumeId};

pub fn parse_url_path(input: &str) -> Result<(VolumeId, &str, Option<&str>, &str), VolumeError> {
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

pub fn parse_vid_fid(input: &str) -> IResult<&str, (&str, &str)> {
    let (input, (vid, _, fid)) =
        tuple((digit1, alt((char('/'), char(','))), alphanumeric1))(input)?;
    Ok((input, (vid, fid)))
}

pub fn parse_filename(input: &str) -> IResult<&str, &str> {
    let (input, filename) = take_till(|c| c == '.')(input)?;
    Ok((input, filename))
}

#[cfg(test)]
mod tests {
    use crate::util::parser::{parse_filename, parse_url_path, parse_vid_fid};

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
