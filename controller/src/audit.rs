use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sqlx::{Any, AssertSqlSafe, Pool, Row};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEvent {
    pub id: Option<i64>,
    pub ts: u64,
    pub event_type: String,
    pub principal: String,
    pub principal_kind: String, // "oidc" | "component" | "api_key" | "system"
    pub component_id: Option<String>,
    pub target: Option<String>,
    pub details: Option<String>,
    pub deny_reason: Option<String>,
    pub source_ip: Option<String>,
}

pub struct AuditLog {
    pool: Pool<Any>,
}

impl AuditLog {
    pub async fn new(pool: Pool<Any>) -> Result<Self> {
        let log = Self { pool };
        log.init_schema().await?;
        Ok(log)
    }

    async fn init_schema(&self) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS veloce_audit_events (
                id BIGSERIAL PRIMARY KEY,
                ts BIGINT NOT NULL,
                event_type VARCHAR(100) NOT NULL,
                principal VARCHAR(255) NOT NULL,
                principal_kind VARCHAR(32) NOT NULL,
                component_id VARCHAR(255),
                target VARCHAR(255),
                details TEXT,
                deny_reason TEXT,
                source_ip VARCHAR(64)
            );
            "#,
        )
        .execute(&self.pool)
        .await
        .context("Failed to create veloce_audit_events table")?;

        // Helpful indexes for common queries
        let _ = sqlx::query("CREATE INDEX IF NOT EXISTS idx_audit_ts ON veloce_audit_events (ts);")
            .execute(&self.pool)
            .await;
        let _ = sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_audit_type ON veloce_audit_events (event_type);",
        )
        .execute(&self.pool)
        .await;
        let _ = sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_audit_principal ON veloce_audit_events (principal);",
        )
        .execute(&self.pool)
        .await;

        Ok(())
    }

    /// Append a structured audit event.
    pub async fn log(&self, event: &AuditEvent) -> Result<()> {
        let ts = if event.ts == 0 {
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64
        } else {
            event.ts as i64
        };

        sqlx::query(
            r#"
            INSERT INTO veloce_audit_events
                (ts, event_type, principal, principal_kind, component_id, target, details, deny_reason, source_ip)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            "#
        )
        .bind(ts)
        .bind(&event.event_type)
        .bind(&event.principal)
        .bind(&event.principal_kind)
        .bind(&event.component_id)
        .bind(&event.target)
        .bind(&event.details)
        .bind(&event.deny_reason)
        .bind(&event.source_ip)
        .execute(&self.pool)
        .await
        .context("Failed to insert audit event")?;

        Ok(())
    }

    /// Query recent audit events with optional filters.
    /// Simple implementation for P1-6.1 (no full-text, no complex AND/OR yet).
    pub async fn list(
        &self,
        event_type: Option<&str>,
        principal: Option<&str>,
        limit: Option<i64>,
    ) -> Result<Vec<AuditEvent>> {
        let limit = limit.unwrap_or(100).min(1000);

        let mut query = String::from(
            "SELECT id, ts, event_type, principal, principal_kind, component_id, target, details, deny_reason, source_ip \
             FROM veloce_audit_events WHERE 1=1"
        );

        if event_type.is_some() {
            query.push_str(" AND event_type = $1");
        }
        if principal.is_some() {
            let param = if event_type.is_some() { "$2" } else { "$1" };
            query.push_str(&format!(" AND principal = {}", param));
        }
        query.push_str(" ORDER BY ts DESC LIMIT ");
        query.push_str(&limit.to_string());

        let sql = AssertSqlSafe(query);
        let rows = if let (Some(et), Some(pr)) = (event_type, principal) {
            sqlx::query(sql)
                .bind(et)
                .bind(pr)
                .fetch_all(&self.pool)
                .await?
        } else if let Some(et) = event_type {
            sqlx::query(sql).bind(et).fetch_all(&self.pool).await?
        } else if let Some(pr) = principal {
            sqlx::query(sql).bind(pr).fetch_all(&self.pool).await?
        } else {
            sqlx::query(sql).fetch_all(&self.pool).await?
        };

        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            events.push(AuditEvent {
                id: row.get("id"),
                ts: row.get::<i64, _>("ts") as u64,
                event_type: row.get("event_type"),
                principal: row.get("principal"),
                principal_kind: row.get("principal_kind"),
                component_id: row.get("component_id"),
                target: row.get("target"),
                details: row.get("details"),
                deny_reason: row.get("deny_reason"),
                source_ip: row.get("source_ip"),
            });
        }

        Ok(events)
    }
}

// Convenience constructors for common events (keeps call sites clean)
impl AuditEvent {
    pub fn component_register_success(
        component_id: &str,
        component_type: &str,
        principal: &str,
    ) -> Self {
        Self {
            id: None,
            ts: 0,
            event_type: "component.register.success".to_string(),
            principal: principal.to_string(),
            principal_kind: if component_id.contains('@') {
                "oidc".to_string()
            } else {
                "component".to_string()
            },
            component_id: Some(component_id.to_string()),
            target: Some(format!("type={}", component_type)),
            details: None,
            deny_reason: None,
            source_ip: None,
        }
    }

    pub fn component_register_denied(
        component_id: &str,
        component_type: &str,
        reason: &str,
        principal: &str,
    ) -> Self {
        Self {
            id: None,
            ts: 0,
            event_type: "component.register.denied".to_string(),
            principal: principal.to_string(),
            principal_kind: if component_id.contains('@') {
                "oidc".to_string()
            } else {
                "component".to_string()
            },
            component_id: Some(component_id.to_string()),
            target: Some(format!("type={}", component_type)),
            details: None,
            deny_reason: Some(reason.to_string()),
            source_ip: None,
        }
    }

    pub fn component_token_action(action: &str, component_id: &str, principal: &str) -> Self {
        Self {
            id: None,
            ts: 0,
            event_type: format!("component.token.{}", action),
            principal: principal.to_string(),
            principal_kind: "oidc".to_string(), // admin actions come via REST
            component_id: Some(component_id.to_string()),
            target: Some(component_id.to_string()),
            details: None,
            deny_reason: None,
            source_ip: None,
        }
    }

    pub fn noise_rpc_denied(
        principal: &str,
        component_id: Option<&str>,
        rpc: &str,
        reason: &str,
    ) -> Self {
        Self {
            id: None,
            ts: 0,
            event_type: "noise.rpc.denied".to_string(),
            principal: principal.to_string(),
            principal_kind: "component".to_string(),
            component_id: component_id.map(|s| s.to_string()),
            target: Some(rpc.to_string()),
            details: None,
            deny_reason: Some(reason.to_string()),
            source_ip: None,
        }
    }

    pub fn auth_login(success: bool, principal: &str, kind: &str) -> Self {
        Self {
            id: None,
            ts: 0,
            event_type: if success {
                "auth.login.success".to_string()
            } else {
                "auth.login.failure".to_string()
            },
            principal: principal.to_string(),
            principal_kind: kind.to_string(),
            component_id: None,
            target: None,
            details: None,
            deny_reason: None,
            source_ip: None,
        }
    }

    pub fn auth_denied(principal: &str, kind: &str, reason: &str) -> Self {
        Self {
            id: None,
            ts: 0,
            event_type: "auth.denied".to_string(),
            principal: principal.to_string(),
            principal_kind: kind.to_string(),
            component_id: None,
            target: None,
            details: None,
            deny_reason: Some(reason.to_string()),
            source_ip: None,
        }
    }

    pub fn job_mutation(action: &str, job_id: u64, principal: &str) -> Self {
        Self {
            id: None,
            ts: 0,
            event_type: format!("job.{}", action),
            principal: principal.to_string(),
            principal_kind: "oidc".to_string(),
            component_id: None,
            target: Some(job_id.to_string()),
            details: None,
            deny_reason: None,
            source_ip: None,
        }
    }

    pub fn job_launch_denied(principal: &str, reason: &str) -> Self {
        Self {
            id: None,
            ts: 0,
            event_type: "job.launch.denied".to_string(),
            principal: principal.to_string(),
            principal_kind: "api".to_string(),
            component_id: None,
            target: None,
            details: None,
            deny_reason: Some(reason.to_string()),
            source_ip: None,
        }
    }

    pub fn job_system_profile_launch(job_id: u64, principal: &str, profile: &str) -> Self {
        Self {
            id: None,
            ts: 0,
            event_type: "job.launch.system_profile".to_string(),
            principal: principal.to_string(),
            principal_kind: "api".to_string(),
            component_id: None,
            target: Some(job_id.to_string()),
            details: Some(serde_json::json!({ "job_profile": profile }).to_string()),
            deny_reason: None,
            source_ip: None,
        }
    }

    pub fn job_launch_user_not_found(job_id: u64, user_id: &str) -> Self {
        Self {
            id: None,
            ts: 0,
            event_type: "job.launch.user_not_found".to_string(),
            principal: user_id.to_string(),
            principal_kind: "worker".to_string(),
            component_id: None,
            target: Some(job_id.to_string()),
            details: None,
            deny_reason: Some(format!("user '{}' not found on worker", user_id)),
            source_ip: None,
        }
    }

    pub fn system_restart(principal: &str) -> Self {
        Self {
            id: None,
            ts: 0,
            event_type: "system.restart".to_string(),
            principal: principal.to_string(),
            principal_kind: "oidc".to_string(),
            component_id: None,
            target: None,
            details: None,
            deny_reason: None,
            source_ip: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::any::AnyPoolOptions;

    async fn make_test_log() -> AuditLog {
        sqlx::any::install_default_drivers();
        let pool = AnyPoolOptions::new()
            .max_connections(1)
            .connect("sqlite://")
            .await
            .expect("failed to create in-memory sqlite pool for audit test");
        AuditLog::new(pool).await.expect("failed to init audit log")
    }

    #[tokio::test]
    async fn test_audit_persistence_roundtrip_and_queries() {
        let log = make_test_log().await;

        // Insert a variety of events, including a denial with reason
        let e1 = AuditEvent::component_register_success("worker-42", "worker", "worker-42");
        let e2 = AuditEvent::component_register_denied(
            "bad-client",
            "client",
            "invalid_or_revoked_token",
            "bad-client",
        );
        let e3 = AuditEvent::component_token_action("issued", "mcp-ha-1", "admin@veloce.local");
        let e4 = AuditEvent::auth_login(true, "alice@company.com", "oidc");
        let e5 = AuditEvent::auth_denied("bob@company.com", "oidc", "insufficient_role");
        let e6 = AuditEvent::job_mutation("submit", 123, "alice@company.com");
        let e7 = AuditEvent::noise_rpc_denied(
            "mcp-ha-1",
            Some("mcp-ha-1"),
            "SubmitJob",
            "forbidden_by_rbac",
        );
        let e8 = AuditEvent::system_restart("admin@veloce.local");

        for ev in [&e1, &e2, &e3, &e4, &e5, &e6, &e7, &e8] {
            log.log(ev).await.expect("log failed");
        }

        // Query all (default limit 100)
        let all = log.list(None, None, None).await.expect("list failed");
        assert!(
            all.len() >= 8,
            "expected at least 8 events, got {}",
            all.len()
        );
        // Newest first
        for i in 1..all.len() {
            assert!(
                all[i - 1].ts >= all[i].ts,
                "events not in descending ts order"
            );
        }

        // Filter by event_type
        let denials = log
            .list(Some("component.register.denied"), None, Some(10))
            .await
            .expect("filter by type failed");
        assert_eq!(denials.len(), 1);
        assert_eq!(denials[0].event_type, "component.register.denied");
        assert_eq!(
            denials[0].deny_reason.as_deref(),
            Some("invalid_or_revoked_token")
        );
        assert_eq!(denials[0].component_id.as_deref(), Some("bad-client"));

        // Filter by principal
        let alice_events = log
            .list(None, Some("alice@company.com"), Some(10))
            .await
            .expect("filter by principal failed");
        assert!(alice_events.len() >= 2);
        assert!(alice_events
            .iter()
            .any(|e| e.event_type == "auth.login.success"));
        assert!(alice_events.iter().any(|e| e.event_type == "job.submit"));

        // Limit works
        let limited = log
            .list(None, None, Some(3))
            .await
            .expect("limited query failed");
        assert_eq!(limited.len(), 3);

        // Verify deny reason round-trips for a different event
        let rbac_denial = log
            .list(Some("noise.rpc.denied"), None, Some(1))
            .await
            .expect("noise denial query failed");
        assert_eq!(rbac_denial.len(), 1);
        assert_eq!(
            rbac_denial[0].deny_reason.as_deref(),
            Some("forbidden_by_rbac")
        );
        assert_eq!(rbac_denial[0].target.as_deref(), Some("SubmitJob"));
    }
}
