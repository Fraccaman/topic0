//! Domain value → SQL bind helpers.

use shared::Hash;

/// Length-framed selector blob: `[len][bytes]...`.
pub fn frame_selectors(selectors: &[Hash]) -> Vec<u8> {
    let mut out = Vec::new();
    for s in selectors {
        out.push(s.0.len() as u8);
        out.extend_from_slice(&s.0);
    }
    out
}

/// Inverse of `frame_selectors`.
pub fn unframe_selectors(blob: &[u8]) -> Vec<Hash> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < blob.len() {
        let len = blob[i] as usize;
        i += 1;
        if i + len > blob.len() {
            // Truncated final frame → corruption. debug_assert in debug; release drops the tail.
            debug_assert!(
                false,
                "truncated selector frame: need {len} bytes, have {}",
                blob.len() - i
            );
            break;
        }
        out.push(Hash(blob[i..i + len].to_vec()));
        i += len;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrips_mixed_selector_sizes() {
        // Mixed sizes: 32-byte topic0 + 8-byte discriminator.
        let sels = vec![Hash(vec![1; 32]), Hash(vec![2; 8])];
        assert_eq!(unframe_selectors(&frame_selectors(&sels)), sels);
    }

    #[test]
    fn empty_list_roundtrips() {
        assert!(frame_selectors(&[]).is_empty());
        assert!(unframe_selectors(&[]).is_empty());
    }

    #[test]
    #[should_panic(expected = "truncated selector frame")]
    fn truncated_blob_is_flagged() {
        // len byte says 32 but only 3 bytes follow → debug_assert fires.
        let blob = vec![32u8, 0xAA, 0xBB, 0xCC];
        let _ = unframe_selectors(&blob);
    }
}
