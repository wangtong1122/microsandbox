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
        tracing::info!("解压本地的{}",tar_path.display());
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
fn normalize_repository(repo: &str) -> String {
    // 已经包含 registry 的就直接返回，例如 `ghcr.io/...` 或 `registry.example.com/...`
    if repo.contains('.') || repo.contains(':') || repo.starts_with("localhost/") {
        repo.to_string()
    } else {
        // 没有 registry 的，统一加上默认的 `docker.io/`
        format!("docker.io/{}", repo)
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
        // Compose the Docker CLI reference, which usually matches `docker.io/<repository>:<tag>`
        let repo = normalize_repository(repository);
        let reference = match &selector {
            ReferenceSelector::Tag { tag, .. } => format!("{}:{}", repo, tag),
            ReferenceSelector::Digest(digest) => {
                format!("{}@{}:{}", repo, digest.algorithm(), digest.digest())
            }
        };

        let tmp = self.export_image_to_temp_dir(&reference).await?;
        tracing::info!("本地的完成本地导出tmp: {}",tmp.path().display());
        let manifest_entries = Self::load_docker_manifest(&tmp)?;
        tracing::info!("读取本地的 manifest_entries 完成 ");
        let entry = &manifest_entries[0];

        // Config
        let config = Self::load_image_config(&tmp, &entry.Config)?;
        tracing::info!("读取config 完成 {}",serde_json::to_string_pretty(&config).unwrap());
        // Compute layer digests and sizes, and copy to layer_download_dir
        fs::create_dir_all(&self.layer_download_dir).await?;

        let mut total_size: i64 = 0;
        // Synthetic minimal index/manifest since callers usually care about DB + layers.
        let image_id = db::save_or_update_image(&self.oci_db, &reference, 0).await?;
        tracing::info!("读取的image_id: {}",image_id);
        // Build a minimal synthetic OCI index and manifest just to satisfy the DB schema.
        let mut index = ImageIndex::default();
        index.set_schema_version(2);
        tracing::info!("读取的index{}",serde_json::to_string_pretty(&index).unwrap());
        let index_id = db::save_index(&self.oci_db, image_id, &index, None).await?;
        tracing::info!("读取的index_id: {}",index_id);

        // Minimal dummy manifest. Use from_reader on a minimal JSON with required fields.
        let manifest_json = br#"{
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": "sha256:0000000000000000000000000000000000000000000000000000000000000000",
                "size": 0
            },
            "layers": []
        }"#;
        let manifest = ImageManifest::from_reader(std::io::Cursor::new(&manifest_json[..]))
            .map_err(|e| MicrosandboxError::from(anyhow::Error::new(e)))?;
        tracing::info!("读取的manifest{}",serde_json::to_string_pretty(&manifest).unwrap());
        let manifest_id = db::save_manifest(&self.oci_db, image_id, Some(index_id), &manifest).await?;
        tracing::info!("读取的manifest_id: {}",manifest_id);
        db::save_config(&self.oci_db, manifest_id, &config).await?;

        for (idx, layer_rel) in entry.Layers.iter().enumerate() {
            tracing::info!("开始处理 layer: {}",layer_rel);
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
