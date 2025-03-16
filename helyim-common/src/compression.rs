use std::io::Read;

use libflate::gzip::Decoder;

pub fn ungzip(data: &[u8]) -> Result<Vec<u8>, std::io::Error> {
    let mut decoded = Vec::new();
    {
        let mut decoder = Decoder::new(data)?;
        decoder.read_to_end(&mut decoded)?;
    }
    Ok(decoded)
}
