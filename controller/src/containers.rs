use anyhow::{Context, Result};
use sqlx::any::AnyPoolOptions;
use sqlx::{Any, Pool, Row};
use veloce_common::apptainer::{ContainerAsset, SolverManifest};

pub struct ContainerStore {
    pool: Pool<Any>,
}

impl ContainerStore {
    pub async fn new(db_url: &str) -> Result<Self> {
        sqlx::any::install_default_drivers();

        let conn_url = if db_url.starts_with("sqlite:") && !db_url.starts_with("sqlite://") {
            format!("sqlite://{}", db_url.trim_start_matches("sqlite:"))
        } else {
            db_url.to_string()
        };

        let max_connections = crate::config::sqlite_pool_max_connections(&conn_url);

        let pool = AnyPoolOptions::new()
            .max_connections(max_connections)
            .connect(&conn_url)
            .await
            .context("Failed to connect to containers DB")?;

        let store = Self { pool };
        store.init_schema().await?;
        Ok(store)
    }

    async fn init_schema(&self) -> Result<()> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS veloce_containers (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                image_uri TEXT NOT NULL,
                manifest_json TEXT NOT NULL
            )",
        )
        .execute(&self.pool)
        .await
        .context("Failed to create containers table")?;
        Ok(())
    }

    pub async fn add_container(&self, asset: &ContainerAsset) -> Result<()> {
        let manifest_json = serde_json::to_string(&asset.manifest)?;
        sqlx::query(
            "INSERT INTO veloce_containers (id, name, image_uri, manifest_json)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                image_uri = excluded.image_uri,
                manifest_json = excluded.manifest_json",
        )
        .bind(&asset.id)
        .bind(&asset.name)
        .bind(&asset.image_uri)
        .bind(&manifest_json)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_container(&self, id: &str) -> Result<Option<ContainerAsset>> {
        let row = sqlx::query("SELECT id, name, image_uri, manifest_json FROM veloce_containers WHERE id = $1 OR name = $1 OR image_uri = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;

        if let Some(row) = row {
            let manifest_json: String = row.get("manifest_json");
            let manifest: SolverManifest = serde_json::from_str(&manifest_json)?;
            Ok(Some(ContainerAsset {
                id: row.get("id"),
                name: row.get("name"),
                image_uri: row.get("image_uri"),
                manifest,
            }))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_container_store_crud() {
        let store = ContainerStore::new("sqlite://").await.unwrap();

        let asset = ContainerAsset {
            id: "test-id-123".to_string(),
            name: "Test Solver".to_string(),
            image_uri: "s3://veloce-system-containers/test.sif".to_string(),
            manifest: SolverManifest {
                manifest_version: "1.0".to_string(),
                solver_identity: veloce_common::apptainer::SolverIdentity {
                    vendor: "TestVendor".to_string(),
                    product: "TestProduct".to_string(),
                    version: "1.0".to_string(),
                    capabilities: vec![],
                },
                execution: veloce_common::apptainer::Execution {
                    entrypoint: "/bin/test".to_string(),
                    launch_wrapper: Some("apptainer exec".to_string()),
                    command_template: "{{entrypoint}}".to_string(),
                    environment_vars: Default::default(),
                },
                file_handling: veloce_common::apptainer::FileHandling {
                    working_dir: "/test".to_string(),
                    input_mapping: vec![],
                    output_collection: vec![],
                },
                parameter_mapping: Default::default(),
                vnc_enabled: false,
            },
        };

        // Add
        store.add_container(&asset).await.unwrap();

        // Get
        let fetched = store.get_container("test-id-123").await.unwrap().unwrap();
        assert_eq!(fetched.name, "Test Solver");
        assert_eq!(fetched.manifest.solver_identity.vendor, "TestVendor");

        // Missing get
        let missing = store.get_container("nope").await.unwrap();
        assert!(missing.is_none());
    }
}

impl ContainerStore {
    pub async fn list_containers(&self) -> Result<Vec<ContainerAsset>> {
        let rows = sqlx::query("SELECT id, name, image_uri, manifest_json FROM veloce_containers")
            .fetch_all(&self.pool)
            .await?;

        let mut assets = Vec::new();
        for row in rows {
            let manifest_json: String = row.get("manifest_json");
            let manifest: SolverManifest = serde_json::from_str(&manifest_json)?;
            assets.push(ContainerAsset {
                id: row.get("id"),
                name: row.get("name"),
                image_uri: row.get("image_uri"),
                manifest,
            });
        }
        Ok(assets)
    }

    pub async fn delete_container(&self, id: &str) -> Result<()> {
        let result = sqlx::query("DELETE FROM veloce_containers WHERE id = $1 OR name = $1")
            .bind(id)
            .execute(&self.pool)
            .await?;
        if result.rows_affected() == 0 {
            anyhow::bail!("Container not found");
        }
        Ok(())
    }
}
