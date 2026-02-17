use anyhow::{Context, Result};
use sha1::{Digest, Sha1};
use std::path::Path;
use tokio::io::AsyncReadExt;

pub async fn hash_file(path: &Path) -> Result<Vec<u8>> {
    let mut file = tokio::fs::File::open(path)
        .await
        .with_context(|| format!("Failed to open file for hashing: {}", path.display()))?;

    let mut hasher = Sha1::new();
    let mut buf = [0u8; 2048];

    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }

    Ok(hasher.finalize().to_vec())
}

pub fn checksum_to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn hash_missing_file_error_context() {
        let result = hash_file(Path::new("/nonexistent/file.bin")).await;
        assert!(result.is_err());
        let msg = format!("{:#}", result.unwrap_err());
        assert!(msg.contains("Failed to open file for hashing"));
    }

    #[tokio::test]
    async fn hash_spans_multiple_chunks() {
        let mut f = NamedTempFile::new().unwrap();
        // Write more than the 2048-byte buffer to exercise the read loop
        let chunk = vec![0xABu8; 4096];
        for _ in 0..10 {
            f.write_all(&chunk).unwrap();
        }

        let hash = hash_file(f.path()).await.unwrap();
        assert_eq!(hash.len(), 20);
    }

    #[test]
    fn checksum_hex_conversion() {
        let bytes = vec![0xab, 0xcd, 0xef, 0x01, 0x23];
        assert_eq!(checksum_to_hex(&bytes), "abcdef0123");
    }

    #[test]
    fn checksum_hex_all_zeros() {
        let bytes = vec![0u8; 20];
        assert_eq!(checksum_to_hex(&bytes), "0000000000000000000000000000000000000000");
    }
}
