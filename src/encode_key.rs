use base64::{Engine as _, engine::general_purpose};
use blake2::Blake2bVar;
use blake2::digest::{Update, VariableOutput};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyString};

/// Max raw key size before hashing. Binary keys get base64-encoded (4/3 expansion),
/// so the threshold is 250 * 3 / 4 ≈ 187.
const MAX_KEY_SIZE: usize = 187;

/// Blake2b digest size in bytes. Matches Python's hashlib.blake2b(digest_size=18).
const BLAKE2B_DIGEST_SIZE: usize = 18;

/// A wire-ready encoded key plus metadata.
pub struct EncodedKey {
    /// The key bytes ready to be written to the wire (base64-encoded if binary).
    pub value: Vec<u8>,
    /// Whether the original key was binary (contains non-printable ASCII).
    /// The caller should send the `b` flag to tell the server to decode base64.
    pub is_binary: bool,
}

/// Encode a raw key for the memcache wire protocol:
/// 1. If the key is >= MAX_KEY_SIZE bytes, hash it with blake2b (18-byte digest).
/// 2. If the (possibly hashed) key contains binary bytes, base64-encode it.
/// The resulting wire key is always < 250 bytes (memcache max key length).
/// Returns None if the key is empty.
pub fn encode_key(data: &[u8]) -> Option<EncodedKey> {
    if data.is_empty() {
        return None;
    }

    // Hash long keys to a compact digest
    let mut digest_buf = [0u8; BLAKE2B_DIGEST_SIZE];
    let key: &[u8] = if data.len() >= MAX_KEY_SIZE {
        let mut hasher = Blake2bVar::new(BLAKE2B_DIGEST_SIZE).ok()?;
        hasher.update(data);
        hasher.finalize_variable(&mut digest_buf).ok()?;
        &digest_buf
    } else {
        data
    };

    let is_binary = key.iter().any(|&c| c <= b' ' || c > b'~');

    // Key is guaranteed < MAX_KEY_SIZE (187) bytes at this point (either
    // original short key or 18-byte blake2b digest). Base64 expands by 4/3:
    //   - Worst case: 186 bytes -> ceil(186/3)*4 = 248 < MAX_KEY_LEN (250)
    //   - Hash case:   18 bytes -> ceil(18/3)*4  =  24 < MAX_KEY_LEN (250)
    // So the wire key is always under MAX_KEY_LEN.
    let value = if is_binary {
        general_purpose::STANDARD.encode(key).into_bytes()
    } else {
        key.to_vec()
    };

    Some(EncodedKey { value, is_binary })
}

/// Extract a key from a Python object. Accepts str (UTF-8) or bytes.
pub fn extract_key<'py>(ob: &'py Bound<'py, PyAny>) -> PyResult<&'py [u8]> {
    // Use `cast` instead of `extract` — turning `PyDowncastError` into `PyErr` is costly,
    // and we only care about the success path here.
    if let Ok(s) = ob.cast::<PyString>() {
        Ok(s.to_str()?.as_bytes())
    } else if let Ok(b) = ob.cast::<PyBytes>() {
        Ok(b.as_bytes())
    } else {
        Err(PyValueError::new_err("key must be str or bytes"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_short_ascii_key_passthrough() {
        let ek = encode_key(b"users:profile:12345").unwrap();
        assert_eq!(ek.value, b"users:profile:12345");
        assert!(!ek.is_binary);
    }

    #[test]
    fn test_empty_key_returns_none() {
        assert!(encode_key(b"").is_none());
    }

    #[test]
    fn test_short_binary_key_base64() {
        let ek = encode_key(b"\x00\x01\x02binary\xffkey").unwrap();
        assert!(ek.is_binary);
        // Verify the base64 decodes back to the original
        let decoded = general_purpose::STANDARD.decode(&ek.value).unwrap();
        assert_eq!(decoded, b"\x00\x01\x02binary\xffkey");
    }

    #[test]
    fn test_long_key_gets_hashed_then_base64() {
        let key = vec![b'a'; MAX_KEY_SIZE]; // exactly at threshold
        let ek = encode_key(&key).unwrap();
        // blake2b digest is binary, so it should be base64-encoded
        assert!(ek.is_binary);
        assert_eq!(
            general_purpose::STANDARD.decode(&ek.value).unwrap().len(),
            BLAKE2B_DIGEST_SIZE
        );
    }

    #[test]
    fn test_key_below_threshold_passthrough() {
        let key = vec![b'x'; MAX_KEY_SIZE - 1];
        let ek = encode_key(&key).unwrap();
        assert_eq!(ek.value, key);
        assert!(!ek.is_binary);
    }

    #[test]
    fn test_hash_is_deterministic() {
        let key = vec![b'z'; 200];
        let r1 = encode_key(&key).unwrap();
        let r2 = encode_key(&key).unwrap();
        assert_eq!(r1.value, r2.value);
    }

    #[test]
    fn test_wire_key_under_max_len() {
        // Even a very long key should produce a short wire key after hashing
        let key = vec![b'q'; 10_000];
        let ek = encode_key(&key).unwrap();
        assert!(ek.value.len() < 250);
    }
}
