use crate::FileHandle;
use anyhow::{anyhow, Result};
use futures::StreamExt;
use reqwest::Client;
use std::path::Path;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio_util::io::ReaderStream;

pub struct FileClient {
    client: Client,
    base_url: String,
    api_key: String,
}

impl FileClient {
    pub fn new(base_url: String, api_key: String) -> Self {
        let client = Client::builder()
            .danger_accept_invalid_certs(!cfg!(feature = "production"))
            .build()
            .unwrap_or_else(|_| Client::new());
        Self {
            client,
            base_url,
            api_key,
        }
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn api_key(&self) -> &str {
        &self.api_key
    }

    pub async fn upload_file(
        &self,
        path: &Path,
        is_executable: bool,
        is_archive: bool,
    ) -> Result<FileHandle> {
        let file_name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("file")
            .to_string();

        let uuid = uuid::Uuid::new_v4().to_string();
        let s3_key = format!("{}/{}", uuid, file_name);
        let s3_uri = format!("s3://veloce-staging/{}", s3_key);

        let url = format!("{}/s3/veloce-staging/{}", self.base_url, s3_key);

        let file = File::open(path).await?;
        let stream = ReaderStream::new(file);

        let response = self
            .client
            .put(&url)
            .header("X-API-KEY", &self.api_key)
            .body(reqwest::Body::wrap_stream(stream))
            .send()
            .await?;

        if !response.status().is_success() {
            let err = response.text().await?;
            return Err(anyhow!("Upload failed: {}", err));
        }

        Ok(FileHandle {
            file_id: s3_uri,
            original_name: file_name,
            is_executable,
            is_archive,
        })
    }

    pub async fn download_file(&self, file_id: &str, destination: &Path) -> Result<()> {
        if file_id.starts_with("s3://") {
            return self.download_s3_file(file_id, destination).await;
        }

        let url = format!("{}/api/v1/files/{}", self.base_url, file_id);
        let response = self
            .client
            .get(&url)
            .header("X-API-KEY", &self.api_key)
            .send()
            .await?;

        if !response.status().is_success() {
            let err = response.text().await?;
            return Err(anyhow!("Download failed: {}", err));
        }

        let mut file = File::create(destination).await?;
        let mut stream = response.bytes_stream();

        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            file.write_all(&chunk).await?;
        }

        Ok(())
    }

    pub async fn download_file_to_bytes(&self, file_id: &str) -> Result<Vec<u8>> {
        let response = if file_id.starts_with("s3://") {
            let path = file_id.trim_start_matches("s3://");
            let url = format!("{}/s3/{}", self.base_url, path);
            self.client
                .get(&url)
                .header("X-API-KEY", &self.api_key)
                .send()
                .await?
        } else {
            let url = format!("{}/api/v1/files/{}", self.base_url, file_id);
            self.client
                .get(&url)
                .header("X-API-KEY", &self.api_key)
                .send()
                .await?
        };

        if !response.status().is_success() {
            let err = response.text().await?;
            return Err(anyhow!("Download failed: {}", err));
        }

        let bytes = response.bytes().await?;
        Ok(bytes.to_vec())
    }

    pub async fn delete_file(&self, file_id: &str) -> Result<()> {
        if file_id.starts_with("s3://") {
            return self.delete_s3_file(file_id).await;
        }

        let url = format!("{}/api/v1/files/{}", self.base_url, file_id);
        let response = self
            .client
            .delete(&url)
            .header("X-API-KEY", &self.api_key)
            .send()
            .await?;

        if !response.status().is_success() {
            let err = response.text().await?;
            return Err(anyhow!("Deletion failed: {}", err));
        }

        Ok(())
    }

    pub async fn download_s3_file(&self, s3_uri: &str, destination: &Path) -> Result<()> {
        let path = s3_uri.trim_start_matches("s3://");
        let url = format!("{}/s3/{}", self.base_url, path);

        let response = self
            .client
            .get(&url)
            .header("X-API-KEY", &self.api_key)
            .send()
            .await?;

        if !response.status().is_success() {
            let err = response.text().await?;
            return Err(anyhow!("S3 Download failed: {}", err));
        }

        let mut file = File::create(destination).await?;
        let mut stream = response.bytes_stream();

        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            file.write_all(&chunk).await?;
        }

        Ok(())
    }

    pub async fn delete_s3_file(&self, s3_uri: &str) -> Result<()> {
        let path = s3_uri.trim_start_matches("s3://");
        let url = format!("{}/s3/{}", self.base_url, path);

        let response = self
            .client
            .delete(&url)
            .header("X-API-KEY", &self.api_key)
            .send()
            .await?;

        if !response.status().is_success() {
            let err = response.text().await?;
            return Err(anyhow!("S3 Deletion failed: {}", err));
        }

        Ok(())
    }
}
