pub fn checksum(bytes: &[u8]) -> u32 {
    crc32fast::hash(bytes)
}
