use sketchlib_rust::{SketchInput, hash_it_to_128};

pub(super) fn split_key(key: &str) -> Option<Vec<&str>> {
    let parts: Vec<&str> = key.split(';').filter(|part| !part.is_empty()).collect();
    if parts.is_empty() { None } else { Some(parts) }
}

#[inline(always)]
pub(super) fn hash_key_128(key: &str) -> u128 {
    hash_it_to_128(0, &SketchInput::Str(key))
}
