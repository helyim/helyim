use std::fmt::{Display, Formatter};

use bytes::Buf;
use serde::{Deserialize, Serialize};

use crate::errors::{Error, Result};

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

    fn new(u: u8) -> Option<Unit> {
        match char::from(u) {
            'm' => Some(Unit::Minute),
            'h' => Some(Unit::Hour),
            'd' => Some(Unit::Day),
            'w' => Some(Unit::Week),
            'M' => Some(Unit::Month),
            'y' => Some(Unit::Year),
            _ => None,
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

// default: m
// 3m
// 4h
// 5d
// 6w
// 7M
// 8y

impl Ttl {
    pub fn new(s: &str) -> Result<Ttl> {
        if s.is_empty() {
            return Ok(Ttl::default());
        }

        let bytes = s.as_bytes();

        let mut unit = bytes[bytes.len() - 1];
        let mut count_bytes = &bytes[..bytes.len() - 1];

        if unit.is_ascii_digit() {
            unit = b'm';
            count_bytes = bytes;
        }

        if let Some(unit) = Unit::new(unit) {
            let ttl = Ttl {
                count: count_bytes.get_u8(),
                unit,
            };
            return Ok(ttl);
        }

        Err(Error::ParseTtl(String::from(s)))
    }

    pub fn as_bytes(&self) -> [u8; 2] {
        let mut buf = [0; 2];
        buf[0] = self.count;
        buf[1] = self.unit as u8;
        buf
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

impl From<u32> for Ttl {
    fn from(u: u32) -> Self {
        let slice = [(u % 0xff) as u8, ((u >> 8) % 0xff) as u8];
        Ttl::from(&slice[..])
    }
}

impl From<&[u8]> for Ttl {
    fn from(u: &[u8]) -> Self {
        Ttl {
            count: u[0],
            unit: Unit::from_u8(u[1]).unwrap(),
        }
    }
}
