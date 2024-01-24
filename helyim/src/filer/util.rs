use axum::http::HeaderValue;

use crate::errors::Result;

#[derive(Debug, PartialEq)]
pub struct HttpRange {
    pub start: u64,
    pub length: u64,
}

impl HttpRange {
    pub fn content_range(&self, size: u64) -> String {
        format!(
            "bytes {}-{}/{}",
            self.start,
            self.start + self.length - 1,
            size
        )
    }
}

pub fn parse_range(header_range: &HeaderValue, size: u64) -> Result<Vec<HttpRange>> {
    let mut http_ranges: Vec<HttpRange> = vec![];

    if let Ok(header_range) = header_range.to_str() {
        if header_range.is_empty() {
            return Ok(http_ranges);
        }

        if header_range.starts_with("bytes=") {
            let byte_range = &header_range[6..];
            let ranges: Vec<&str> = byte_range.split(',').collect();
            ranges.into_iter().for_each(|range| {
                let trim_range = range.replace(" ", "");
                let range_parts: Vec<&str> = trim_range.split('-').collect();

                if range_parts.len() == 2 {
                    if let Ok(start) = range_parts[0].parse::<u64>() {
                        if let Ok(length) = range_parts[1].parse::<u64>() {
                            if start + length > size {
                                // should return err
                                return;
                            }
                            http_ranges.push(HttpRange { start, length });
                        }
                    }
                }
            });
        } else {
            // should return err
            return Ok(http_ranges);
        }
    }

    Ok(http_ranges)
}

pub fn sum_ranges_size(ranges: &Vec<HttpRange>) -> u64 {
    ranges.iter().fold(0, |acc, x| acc + x.length)
}

#[cfg(test)]
mod test {
    use axum::http::HeaderValue;

    use super::{parse_range, HttpRange};
    use crate::errors::Result;

    #[test]
    fn test_range() -> Result<()> {
        let range_header = "bytes=0-499, 600-1000";
        let header_range = HeaderValue::from_str(range_header)?;

        let r = parse_range(&header_range, 10000)?;

        let mut result = Vec::new();
        result.push(HttpRange {
            start: 0,
            length: 499,
        });

        result.push(HttpRange {
            start: 600,
            length: 1000,
        });

        assert_eq!(result, r);

        Ok(())
    }
}
