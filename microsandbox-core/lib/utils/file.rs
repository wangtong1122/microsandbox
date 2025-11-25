//! Utility functions for working with files.

use std::path::Path;

use oci_spec::image::{DescriptorBuilder, Digest as OciDigest, DigestAlgorithm};
use sha2::{Digest, Sha256, Sha384, Sha512};
use tokio::{fs::File, io::AsyncReadExt};

use crate::{MicrosandboxError, MicrosandboxResult};

//--------------------------------------------------------------------------------------------------
// Functions
//--------------------------------------------------------------------------------------------------

/// Gets the hash of a file.
pub async fn get_file_hash(
    path: &Path,
    algorithm: &DigestAlgorithm,
) -> MicrosandboxResult<Vec<u8>> {
    let mut file = File::open(path).await?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).await?;

    let hash = match algorithm {
        DigestAlgorithm::Sha256 => Sha256::digest(&buffer).to_vec(),
        DigestAlgorithm::Sha384 => Sha384::digest(&buffer).to_vec(),
        DigestAlgorithm::Sha512 => Sha512::digest(&buffer).to_vec(),
        _ => {
            return Err(MicrosandboxError::UnsupportedImageHashAlgorithm(format!(
                "Unsupported algorithm: {}",
                algorithm
            )));
        }
    };

    Ok(hash)
}

/// Reads a file and returns both its digest string (sha256) and size in bytes.
pub async fn digest_and_size_from_path(
    path: &Path,
) -> MicrosandboxResult<(String, u64)> {
    let size = tokio::fs::metadata(path).await?.len();
    let digest = get_file_hash(path, &DigestAlgorithm::Sha256).await?;
    Ok((format!("sha256:{}", hex::encode(digest)), size))
}

/// Build a descriptor for an already-known digest/size pair。
pub fn descriptor_from_digest(
    digest: &str,
    size: u64,
    media_type: &str,
) -> MicrosandboxResult<oci_spec::image::Descriptor> {
    let digest = OciDigest::try_from(digest.to_string())
        .map_err(|e| MicrosandboxError::from(anyhow::Error::new(e)))?;
    Ok(
        DescriptorBuilder::default()
            .media_type(media_type)
            .digest(digest)
            .size(size)
            .build()
            .map_err(|e| MicrosandboxError::from(anyhow::Error::new(e)))?,
    )
}
