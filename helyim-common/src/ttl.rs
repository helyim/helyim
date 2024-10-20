use std::fmt::{Display, Formatter};

use nom::{
    branch::alt,
    character::complete::{char as nom_char, digit1},
    combinator::opt,
    sequence::pair,
};
use serde::{Deserialize, Serialize};

#[repr(u8)]
#[derive(Serialize, Deserialize, Copy, Clone, Debug, Default)]
pub enum Unit {
    #[default]
    Empty = 0,
    Minute = 1,
    Hour = 2,
    Day = 3,
    Week = 4,
    Month = 5,
    Year = 6,
}

impl Unit {
    fn from_u8(u: u8) -> Option<Unit> {
        match u {
            0 => Some(Unit::Empty),
            1 => Some(Unit::Minute),
            2 => Some(Unit::Hour),
            3 => Some(Unit::Day),
            4 => Some(Unit::Week),
            5 => Some(Unit::Month),
            6 => Some(Unit::Year),
            _ => None,
        }
    }

    fn new(u: char) -> Unit {
        match u {
            'h' => Unit::Hour,
            'd' => Unit::Day,
            'w' => Unit::Week,
            'M' => Unit::Month,
            'y' => Unit::Year,
            _ => Unit::Minute,
        }
    }
}

impl Display for Unit {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let unit = match *self {
            Unit::Minute => "m",
            Unit::Hour => "h",
            Unit::Day => "d",
            Unit::Week => "w",
            Unit::Month => "M",
            Unit::Year => "y",
            _ => "",
        };
        write!(f, "{}", unit)
    }
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, Default)]
pub struct Ttl {
    pub count: u8,
    pub unit: Unit,
}

impl Ttl {
    pub fn new(s: &str) -> Result<Ttl, TtlError> {
        if s.is_empty() {
            return Ok(Ttl::default());
        }
        let (count, unit) = parse_ttl(s)?;
        Ok(Ttl {
            count: count as u8,
            unit: Unit::new(unit),
        })
    }

    pub fn from_u32(value: u32) -> Result<Ttl, TtlError> {
        let mut buf = [0u8; 2];
        buf[1] = value as u8;
        buf[0] = (value >> 8) as u8;
        Ttl::from_bytes(&buf[..])
    }

    pub fn from_bytes(buf: &[u8]) -> Result<Ttl, TtlError> {
        match Unit::from_u8(buf[1]) {
            Some(unit) => Ok(Ttl {
                count: buf[0],
                unit,
            }),
            None => Err(TtlError::InvalidUnit),
        }
    }

    pub fn as_bytes(&self) -> [u8; 2] {
        let mut buf = [0; 2];
        buf[0] = self.count;
        buf[1] = self.unit as u8;
        buf
    }

    pub fn to_u32(&self) -> u32 {
        if self.count == 0 {
            return 0;
        }
        let mut output = (self.count as u32) << 8;
        output += self.unit as u32;
        output
    }

    pub fn minutes(&self) -> u32 {
        match self.unit {
            Unit::Empty => 0,
            Unit::Minute => self.count as u32,
            Unit::Hour => self.count as u32 * 60,
            Unit::Day => self.count as u32 * 60 * 24,
            Unit::Week => self.count as u32 * 60 * 24 * 7,
            Unit::Month => self.count as u32 * 60 * 24 * 31,
            Unit::Year => self.count as u32 * 60 * 24 * 365,
        }
    }
}

impl Display for Ttl {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.count == 0 {
            write!(f, "")
        } else {
            write!(f, "{}{}", self.count, self.unit)
        }
    }
}

impl From<Ttl> for u32 {
    fn from(value: Ttl) -> Self {
        let mut ret = 0;
        ret += (value.count as u32) << 8;
        ret += value.unit as u32;

        ret
    }
}

fn parse_ttl(input: &str) -> Result<(u32, char), TtlError> {
    let (_, (count, unit)) = pair(
        digit1,
        opt(alt((
            nom_char('m'),
            nom_char('h'),
            nom_char('d'),
            nom_char('w'),
            nom_char('M'),
            nom_char('y'),
        ))),
    )(input)?;
    Ok((count.parse()?, unit.unwrap_or('m')))
}

#[derive(thiserror::Error, Debug)]
pub enum TtlError {
    #[error("Invalid unit")]
    InvalidUnit,
    #[error("Parse integer error: {0}")]
    ParseInt(#[from] std::num::ParseIntError),
}

impl From<nom::Err<nom::error::Error<&str>>> for TtlError {
    fn from(_: nom::Err<nom::error::Error<&str>>) -> Self {
        Self::InvalidUnit
    }
}

#[cfg(test)]
mod tests {
    use crate::ttl::Ttl;

    #[test]
    pub fn test_ttl() {
        let ttl = Ttl::new("").unwrap();
        assert_eq!(ttl.minutes(), 0);

        let ttl = Ttl::new("9").unwrap();
        assert_eq!(ttl.minutes(), 9);

        let ttl = Ttl::new("8m").unwrap();
        assert_eq!(ttl.minutes(), 8);

        let ttl = Ttl::new("5h").unwrap();
        assert_eq!(ttl.minutes(), 300);

        let ttl = Ttl::new("5d").unwrap();
        assert_eq!(ttl.minutes(), 5 * 24 * 60);

        let ttl = Ttl::new("50d").unwrap();
        assert_eq!(ttl.minutes(), 50 * 24 * 60);

        let ttl = Ttl::new("5w").unwrap();
        assert_eq!(ttl.minutes(), 5 * 7 * 24 * 60);

        let ttl = Ttl::new("5M").unwrap();
        assert_eq!(ttl.minutes(), 5 * 60 * 24 * 31);

        let ttl = Ttl::new("5y").unwrap();
        assert_eq!(ttl.minutes(), 5 * 60 * 24 * 365);

        let ttl_bytes = ttl.as_bytes();
        let ttl2 = Ttl::from_bytes(&ttl_bytes[..]).unwrap();
        assert_eq!(ttl.minutes(), ttl2.minutes());

        let ttl3 = Ttl::from_u32(Into::<u32>::into(ttl)).unwrap();
        assert_eq!(ttl.minutes(), ttl3.minutes());
    }
}
