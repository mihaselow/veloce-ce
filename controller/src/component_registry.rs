use anyhow::{Context, Result};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use sha2::{Digest, Sha256};
use sqlx::{Any, Pool, Row};
use std::time::{SystemTime, UNIX_EPOCH};
use subtle::ConstantTimeEq;
use veloce_common::auth::{ComponentType, RegisteredComponent};

pub struct ComponentRegistry {
    pool: Pool<Any>,
}

impl ComponentRegistry {
    pub async fn new(pool: Pool<Any>) -> Result<Self> {
        let registry = Self { pool };
        registry.init_schema().await?;
        Ok(registry)
    }

    async fn init_schema(&self) -> Result<()> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS veloce_component_registry (
                component_id VARCHAR(255) PRIMARY KEY,
                component_type VARCHAR(50) NOT NULL,
                token_hash VARCHAR(255) NOT NULL,
                roles TEXT NOT NULL,
                created_at BIGINT NOT NULL,
                last_seen_at BIGINT,
                revoked INTEGER NOT NULL DEFAULT 0
            );",
        )
        .execute(&self.pool)
        .await
        .context("Failed to create veloce_component_registry table")?;
        Ok(())
    }

    /// Generates and issues a new registration token for a component.
    /// Returns the plaintext token (only returned once to the caller).
    pub async fn issue_token(
        &self,
        component_id: &str,
        component_type: ComponentType,
        roles: &[String],
    ) -> Result<String> {
        let plaintext_token = format!(
            "veloce_tok_{}_{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        );
        let token_hash = hash_token(&plaintext_token);

        let roles_str = roles.join(",");
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;

        sqlx::query(
            "INSERT INTO veloce_component_registry (component_id, component_type, token_hash, roles, created_at, last_seen_at, revoked)
             VALUES ($1, $2, $3, $4, $5, NULL, 0)
             ON CONFLICT(component_id) DO UPDATE SET
                token_hash = excluded.token_hash,
                roles = excluded.roles,
                created_at = excluded.created_at,
                last_seen_at = NULL,
                revoked = 0"
        )
        .bind(component_id)
        .bind(component_type.to_string())
        .bind(&token_hash)
        .bind(&roles_str)
        .bind(now)
        .execute(&self.pool)
        .await
        .context("Failed to insert/update token in registry")?;

        Ok(plaintext_token)
    }

    /// Validates a presented token for a given component.
    /// Returns the RegisteredComponent if valid and active.
    /// Performs constant-time comparison of the token hash.
    pub async fn validate(
        &self,
        component_id: &str,
        component_type: ComponentType,
        presented_token: &str,
    ) -> Result<Option<RegisteredComponent>> {
        let row = sqlx::query(
            "SELECT component_id, component_type, token_hash, roles, created_at, last_seen_at, revoked
             FROM veloce_component_registry
             WHERE component_id = $1"
        )
        .bind(component_id)
        .fetch_optional(&self.pool)
        .await
        .context("Failed to query component registry")?;

        let row = match row {
            Some(r) => r,
            None => return Ok(None),
        };

        let db_type_str: String = row.get("component_type");
        let db_type = match db_type_str.parse::<ComponentType>() {
            Ok(t) => t,
            Err(_) => return Ok(None),
        };

        if db_type != component_type {
            return Ok(None);
        }

        let revoked_val: i32 = row.get("revoked");
        let revoked = revoked_val != 0;
        if revoked {
            return Ok(None);
        }

        let db_hash: String = row.get("token_hash");
        let presented_hash = hash_token(presented_token);

        // Constant-time verification of the hashes
        let db_hash_bytes = db_hash.as_bytes();
        let presented_hash_bytes = presented_hash.as_bytes();

        if db_hash_bytes.ct_eq(presented_hash_bytes).unwrap_u8() == 1 {
            // Update last_seen_at
            let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
            let _ = sqlx::query(
                "UPDATE veloce_component_registry SET last_seen_at = $1 WHERE component_id = $2",
            )
            .bind(now)
            .bind(component_id)
            .execute(&self.pool)
            .await;

            let roles_str: String = row.get("roles");
            let roles = if roles_str.is_empty() {
                Vec::new()
            } else {
                roles_str.split(',').map(|s| s.to_string()).collect()
            };

            let created_at: i64 = row.get("created_at");
            let last_seen_at: Option<i64> = row.get("last_seen_at");

            Ok(Some(RegisteredComponent {
                component_id: component_id.to_string(),
                component_type,
                roles,
                created_at: created_at as u64,
                last_seen_at: last_seen_at.map(|t| t as u64),
                revoked,
            }))
        } else {
            Ok(None)
        }
    }

    /// Validates a presented token without knowing the component_id in advance.
    /// Performs constant-time comparison of the token hash.
    pub async fn validate_token(
        &self,
        presented_token: &str,
    ) -> Result<Option<RegisteredComponent>> {
        let presented_hash = hash_token(presented_token);

        let row = sqlx::query(
            "SELECT component_id, component_type, token_hash, roles, created_at, last_seen_at, revoked
             FROM veloce_component_registry
             WHERE token_hash = $1"
        )
        .bind(&presented_hash)
        .fetch_optional(&self.pool)
        .await
        .context("Failed to query component registry by token hash")?;

        let row = match row {
            Some(r) => r,
            None => return Ok(None),
        };

        let revoked_val: i32 = row.get("revoked");
        let revoked = revoked_val != 0;
        if revoked {
            return Ok(None);
        }

        let db_hash: String = row.get("token_hash");

        // Constant-time verification of the hashes
        let db_hash_bytes = db_hash.as_bytes();
        let presented_hash_bytes = presented_hash.as_bytes();

        if db_hash_bytes.ct_eq(presented_hash_bytes).unwrap_u8() == 1 {
            let component_id: String = row.get("component_id");
            // Update last_seen_at
            let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
            let _ = sqlx::query(
                "UPDATE veloce_component_registry SET last_seen_at = $1 WHERE component_id = $2",
            )
            .bind(now)
            .bind(&component_id)
            .execute(&self.pool)
            .await;

            let db_type_str: String = row.get("component_type");
            let component_type = db_type_str.parse::<ComponentType>()?;

            let roles_str: String = row.get("roles");
            let roles = if roles_str.is_empty() {
                Vec::new()
            } else {
                roles_str.split(',').map(|s| s.to_string()).collect()
            };

            let created_at: i64 = row.get("created_at");
            let last_seen_at: Option<i64> = row.get("last_seen_at");

            Ok(Some(RegisteredComponent {
                component_id,
                component_type,
                roles,
                created_at: created_at as u64,
                last_seen_at: last_seen_at.map(|t| t as u64),
                revoked,
            }))
        } else {
            Ok(None)
        }
    }

    /// Revokes a component's registration.
    pub async fn revoke(&self, component_id: &str) -> Result<()> {
        let result =
            sqlx::query("UPDATE veloce_component_registry SET revoked = 1 WHERE component_id = $1")
                .bind(component_id)
                .execute(&self.pool)
                .await
                .context("Failed to revoke component in registry")?;

        if result.rows_affected() == 0 {
            anyhow::bail!("Component not found in registry");
        }
        Ok(())
    }

    /// Rotates a component's token. Returns the new plaintext token.
    pub async fn rotate(&self, component_id: &str) -> Result<String> {
        let row = sqlx::query(
            "SELECT component_type, roles FROM veloce_component_registry WHERE component_id = $1",
        )
        .bind(component_id)
        .fetch_optional(&self.pool)
        .await
        .context("Failed to query component for rotation")?;

        let row = match row {
            Some(r) => r,
            None => anyhow::bail!("Component not found in registry"),
        };

        let db_type_str: String = row.get("component_type");
        let component_type = db_type_str.parse::<ComponentType>()?;
        let roles_str: String = row.get("roles");
        let roles = if roles_str.is_empty() {
            Vec::new()
        } else {
            roles_str.split(',').map(|s| s.to_string()).collect()
        };

        self.issue_token(component_id, component_type, &roles).await
    }

    /// Lists all registered components.
    pub async fn list_components(&self) -> Result<Vec<RegisteredComponent>> {
        let rows = sqlx::query(
            "SELECT component_id, component_type, roles, created_at, last_seen_at, revoked
             FROM veloce_component_registry",
        )
        .fetch_all(&self.pool)
        .await
        .context("Failed to list components")?;

        let mut list = Vec::new();
        for row in rows {
            let db_type_str: String = row.get("component_type");
            let component_type = match db_type_str.parse::<ComponentType>() {
                Ok(t) => t,
                Err(_) => continue,
            };
            let roles_str: String = row.get("roles");
            let roles = if roles_str.is_empty() {
                Vec::new()
            } else {
                roles_str.split(',').map(|s| s.to_string()).collect()
            };
            let created_at: i64 = row.get("created_at");
            let last_seen_at: Option<i64> = row.get("last_seen_at");
            let revoked_val: i32 = row.get("revoked");
            let revoked = revoked_val != 0;

            list.push(RegisteredComponent {
                component_id: row.get("component_id"),
                component_type,
                roles,
                created_at: created_at as u64,
                last_seen_at: last_seen_at.map(|t| t as u64),
                revoked,
            });
        }
        Ok(list)
    }
}

pub fn hash_token(token: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(token.as_bytes());
    BASE64.encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::any::AnyPoolOptions;

    #[tokio::test]
    async fn test_component_registry_logic() {
        sqlx::any::install_default_drivers();
        let pool = AnyPoolOptions::new()
            .max_connections(1)
            .connect("sqlite://")
            .await
            .unwrap();

        let registry = ComponentRegistry::new(pool).await.unwrap();

        // 1. Issue a token
        let component_id = "test-worker-1";
        let token = registry
            .issue_token(component_id, ComponentType::Worker, &["worker".to_string()])
            .await
            .unwrap();
        assert!(token.starts_with("veloce_tok_"));

        // 2. Validate token
        let component = registry
            .validate(component_id, ComponentType::Worker, &token)
            .await
            .unwrap();
        assert!(component.is_some());
        let component = component.unwrap();
        assert_eq!(component.component_id, component_id);
        assert_eq!(component.component_type, ComponentType::Worker);
        assert_eq!(component.roles, vec!["worker".to_string()]);
        assert!(!component.revoked);

        // 3. Validate with wrong token
        let invalid = registry
            .validate(component_id, ComponentType::Worker, "wrong_token")
            .await
            .unwrap();
        assert!(invalid.is_none());

        // 4. Validate with wrong type
        let invalid_type = registry
            .validate(component_id, ComponentType::Client, &token)
            .await
            .unwrap();
        assert!(invalid_type.is_none());

        // 5. Rotate token
        let new_token = registry.rotate(component_id).await.unwrap();
        assert_ne!(token, new_token);

        // Old token should be invalid now
        let old_valid = registry
            .validate(component_id, ComponentType::Worker, &token)
            .await
            .unwrap();
        assert!(old_valid.is_none());

        // New token should be valid
        let new_valid = registry
            .validate(component_id, ComponentType::Worker, &new_token)
            .await
            .unwrap();
        assert!(new_valid.is_some());

        // 6. List components
        let list = registry.list_components().await.unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].component_id, component_id);

        // 7. Revoke component
        registry.revoke(component_id).await.unwrap();
        let after_revoke = registry
            .validate(component_id, ComponentType::Worker, &new_token)
            .await
            .unwrap();
        assert!(after_revoke.is_none());

        let list_after = registry.list_components().await.unwrap();
        assert_eq!(list_after.len(), 1);
        assert!(list_after[0].revoked);
    }
}
