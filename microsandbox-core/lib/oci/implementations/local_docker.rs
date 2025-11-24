use std::{
    fs::File,
    io::BufReader,
    path::PathBuf,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream, StreamExt};
use oci_spec::image::{Digest, ImageConfiguration, ImageIndex, ImageManifest};
use serde::Deserialize;
use sqlx::{Pool, Sqlite};
use tar::Archive;
use tempfile::TempDir;
use tokio::fs::{self, OpenOptions};

use crate::{
    management::db,
    oci::{OciRegistryPull, ReferenceSelector},
    utils,
    MicrosandboxError, MicrosandboxResult,
};

/// Use a locally available Docker image (via `docker image save`) as an OCI image source,
/// avoiding any remote registry access.
pub struct LocalDockerRegistry {
    /// Where to place the final layer blobs (same semantics as `DockerRegistry`)
    layer_download_dir: PathBuf,
    /// OCI metadata DB
    oci_db: Pool<Sqlite>,
}

#[derive(Debug, Deserialize)]
struct DockerManifestEntry {
    Config: String,
    Layers: Vec<String>,
    RepoTags: Option<Vec<String>>,
}

impl LocalDockerRegistry {
    pub async fn new(
        layer_download_dir: impl Into<PathBuf>,
        oci_db_path: impl AsRef<std::path::Path>,
    ) -> MicrosandboxResult<Self> {
        Ok(Self {
            layer_download_dir: layer_download_dir.into(),
            oci_db: db::get_or_create_pool(oci_db_path.as_ref(), &db::OCI_DB_MIGRATOR).await?,
        })
    }

    /// Call `docker image save` and expand the resulting tar in a temp dir.
    async fn export_image_to_temp_dir(&self, reference: &str) -> MicrosandboxResult<TempDir> {
        let tmp = TempDir::new()?;
        let tar_path = tmp.path().join("image.tar");

        let status = std::process::Command::new("docker")
            .arg("image")
            .arg("save")
            .arg("-o")
            .arg(&tar_path)
            .arg(reference)
            .status()?;

        if !status.success() {
            return Err(MicrosandboxError::ImageLayerDownloadFailed(format!(
                "failed to export local docker image: {reference}",
            )));
        }

        let file = File::open(&tar_path)?;
        let reader = BufReader::new(file);
        let mut archive = Archive::new(reader);
        archive.unpack(tmp.path())?;

        Ok(tmp)
    }

    fn load_docker_manifest(tmp: &TempDir) -> MicrosandboxResult<Vec<DockerManifestEntry>> {
        let mf_path = tmp.path().join("manifest.json");
        let file = File::open(&mf_path)?;
        let reader = BufReader::new(file);
        let entries: Vec<DockerManifestEntry> = serde_json::from_reader(reader)?;
        if entries.is_empty() {
            return Err(MicrosandboxError::ManifestNotFound);
        }
        Ok(entries)
    }

    fn load_image_config(
        tmp: &TempDir,
        config_name: &str,
    ) -> MicrosandboxResult<ImageConfiguration> {
        let cfg_path = tmp.path().join(config_name);
        let file = File::open(&cfg_path)?;
        let reader = BufReader::new(file);
        let cfg: ImageConfiguration = serde_json::from_reader(reader)?;
        Ok(cfg)
    }
}

#[async_trait]
impl OciRegistryPull for LocalDockerRegistry {
    async fn pull_image(
        &self,
        repository: &str,
        selector: ReferenceSelector,
    ) -> MicrosandboxResult<()> {
        tracing::info!("Pulling image本地的 {}", repository);
        // Compose the Docker CLI reference, which usually matches `<repository>:<tag>`
        let reference = match &selector {
            ReferenceSelector::Tag { tag, .. } => format!("{repository}:{tag}"),
            ReferenceSelector::Digest(digest) => {
                format!("{repository}@{}:{}", digest.algorithm(), digest.digest())
            }
        };

        let tmp = self.export_image_to_temp_dir(&reference).await?;
        let manifest_entries = Self::load_docker_manifest(&tmp)?;
        let entry = &manifest_entries[0];

        // Config
        let config = Self::load_image_config(&tmp, &entry.Config)?;

        // Compute layer digests and sizes, and copy to layer_download_dir
        fs::create_dir_all(&self.layer_download_dir).await?;

        let mut total_size: i64 = 0;

        // Synthetic minimal index/manifest since callers usually care about DB + layers.
        let image_id = db::save_or_update_image(&self.oci_db, &reference, 0).await?;
        let index = ImageIndex::from_reader(std::io::Cursor::new(b"{}" as &[u8]))
            .map_err(|e| MicrosandboxError::from(anyhow::Error::new(e)))?;
        let index_id = db::save_index(&self.oci_db, image_id, &index, None).await?;

        let manifest = ImageManifest::from_reader(std::io::Cursor::new(b"{}" as &[u8]))
            .map_err(|e| MicrosandboxError::from(anyhow::Error::new(e)))?;
        let manifest_id = db::save_manifest(&self.oci_db, image_id, Some(index_id), &manifest).await?;

        db::save_config(&self.oci_db, manifest_id, &config).await?;

        for (idx, layer_rel) in entry.Layers.iter().enumerate() {
            let src_path = tmp.path().join(layer_rel);
            let meta = std::fs::metadata(&src_path)?;
            let size = meta.len();
            total_size += size as i64;

            let hash = utils::get_file_hash(&src_path, &oci_spec::image::DigestAlgorithm::Sha256).await?;
            let hash = hex::encode(hash);
            let digest_str = format!("sha256:{hash}");

            let dest = self.layer_download_dir.join(&digest_str);
            if !dest.exists() {
                let mut src_f = OpenOptions::new().read(true).open(&src_path).await?;
                let mut dst_f = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(&dest)
                    .await?;
                tokio::io::copy(&mut src_f, &mut dst_f).await?;
            }

            // Map to corresponding diff_id if available, otherwise use first.
            let diff_id = config
                .rootfs()
                .diff_ids()
                .get(idx)
                .or_else(|| config.rootfs().diff_ids().get(0))
                .ok_or_else(|| MicrosandboxError::ManifestNotFound)?;

            let layer_id = db::save_or_update_layer(
                &self.oci_db,
                "application/vnd.oci.image.layer.v1.tar+gzip",
                &digest_str,
                size as i64,
                diff_id,
            )
            .await?;

            db::save_manifest_layer(&self.oci_db, manifest_id, layer_id).await?;
        }

        // Update image total size now that we know it.
        db::save_or_update_image(&self.oci_db, &reference, total_size).await?;

        Ok(())
    }

    async fn fetch_index(
        &self,
        _repository: &str,
        _selector: ReferenceSelector,
    ) -> MicrosandboxResult<ImageIndex> {
        Err(MicrosandboxError::ManifestNotFound)
    }

    async fn fetch_manifest(
        &self,
        _repository: &str,
        _digest: &Digest,
    ) -> MicrosandboxResult<ImageManifest> {
        Err(MicrosandboxError::ManifestNotFound)
    }

    async fn fetch_ghcr_manifest(
        &self,
        _repository: &str,
        _reference: &str,
    ) -> MicrosandboxResult<ImageManifest> {
        Err(MicrosandboxError::ManifestNotFound)
    }

    async fn fetch_config(
        &self,
        _repository: &str,
        _digest: &Digest,
    ) -> MicrosandboxResult<ImageConfiguration> {
        Err(MicrosandboxError::ManifestNotFound)
    }

    async fn fetch_image_blob(
        &self,
        _repository: &str,
        _digest: &Digest,
        _range: impl std::ops::RangeBounds<u64> + Send,
    ) -> MicrosandboxResult<BoxStream<'static, MicrosandboxResult<Bytes>>> {
        let s: BoxStream<'static, MicrosandboxResult<Bytes>> = stream::empty().boxed();
        Ok(s)
    }
}
