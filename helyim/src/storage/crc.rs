use crc::{Crc, CRC_32_ISCSI};

const CASTAGNOLI: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

pub fn checksum(bytes: &[u8]) -> u32 {
    CASTAGNOLI.checksum(bytes)
}
