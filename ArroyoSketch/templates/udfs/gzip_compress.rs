/*
[dependencies]
flate2 = "1.1.1"
*/

use arroyo_udf_plugin::udf;
use std::io::Write;
use flate2::{Compression, write::GzEncoder};

#[udf]
fn gzip_compress(data: &[u8]) -> Option<Vec<u8>> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());

	encoder.write_all(&data).ok()?;
    encoder.finish().ok()

    //// Handle potential write error
    //if encoder.write_all(&data).is_err() {
    //    return None;
    //}
    //
    //// Handle potential finish error and extract the compressed data
    //match encoder.finish() {
    //    Ok(compressed_data) => Some(compressed_data),
    //    Err(_) => None,
    //}
}
