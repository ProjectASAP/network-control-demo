use sketchlib_rust::{SketchInput, hash128_seeded};

pub(super) fn split_key(key: &str) -> Option<Vec<&str>> {
    let parts: Vec<&str> = key.split(';').filter(|part| !part.is_empty()).collect();
    if parts.is_empty() { None } else { Some(parts) }
}

#[inline(always)]
pub(super) fn hash_key_128(key: &str) -> u128 {
    hash128_seeded(0, &SketchInput::Str(key))
}
