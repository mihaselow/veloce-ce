use axum::{
    extract::{Request, State},
    http::{Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Extension,
};
use dashmap::DashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::warn;

use crate::auth::AuthenticatedPrincipal;
use crate::SharedContext;

/// Simple per-minute fixed-window rate limiter.
/// Keyed by principal (user_id or API key id) + request class.
pub struct ApiRateLimiter {
    buckets: DashMap<String, RateBucket>,
    read_limit: u32,
    mutation_limit: u32,
    admin_limit: u32,
}

struct RateBucket {
    window_start: AtomicU64, // unix minutes
    count: AtomicU32,
}

impl ApiRateLimiter {
    pub fn new(read: u32, mutation: u32, admin: u32) -> Self {
        Self {
            buckets: DashMap::new(),
            read_limit: read,
            mutation_limit: mutation,
            admin_limit: admin,
        }
    }

    fn classify(method: &Method, path: &str, principal: &AuthenticatedPrincipal) -> RequestClass {
        if path.starts_with("/api/v1/admin")
            || principal.is_admin_or_operator() && path.contains("/admin")
        {
            return RequestClass::Admin;
        }
        match *method {
            Method::POST | Method::PUT | Method::DELETE | Method::PATCH => RequestClass::Mutation,
            _ => RequestClass::Read,
        }
    }

    fn limit_for(&self, class: RequestClass) -> u32 {
        match class {
            RequestClass::Admin => self.admin_limit,
            RequestClass::Mutation => self.mutation_limit,
            RequestClass::Read => self.read_limit,
        }
    }

    /// Returns Ok(()) if allowed, or Err(retry_after_seconds) if limited.
    pub fn check(
        &self,
        principal: &str,
        method: &Method,
        path: &str,
        p: &AuthenticatedPrincipal,
    ) -> Result<(), u64> {
        let class = Self::classify(method, path, p);
        let limit = self.limit_for(class);
        if limit == 0 {
            return Ok(());
        }

        let key = format!("{}:{}", principal, class as u8);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let now_min = now / 60;

        let bucket = self.buckets.entry(key).or_insert_with(|| RateBucket {
            window_start: AtomicU64::new(now_min),
            count: AtomicU32::new(0),
        });

        let bucket = bucket.value();

        // Reset window if needed (lock-free best effort)
        let current_window = bucket.window_start.load(Ordering::Relaxed);
        if current_window != now_min {
            // Try to reset; races are okay, we may under-count slightly.
            let _ = bucket.window_start.compare_exchange(
                current_window,
                now_min,
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
            bucket.count.store(0, Ordering::Relaxed);
        }

        let prev = bucket.count.fetch_add(1, Ordering::Relaxed);
        if prev >= limit {
            // Compute seconds until next window
            let secs_into_min = now % 60;
            let retry_after = 60 - secs_into_min;
            // Roll back the count we just added to avoid permanent overcount in this window
            bucket.count.fetch_sub(1, Ordering::Relaxed);
            return Err(retry_after);
        }

        Ok(())
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[repr(u8)]
enum RequestClass {
    Read = 0,
    Mutation = 1,
    Admin = 2,
}

pub fn rate_limit_enabled() -> bool {
    std::env::var("VELOCE_RATE_LIMIT_ENABLED")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false)
}

pub async fn rate_limit_middleware(
    State(ctx): State<SharedContext>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    req: Request,
    next: Next,
) -> Response {
    if !rate_limit_enabled() {
        return next.run(req).await;
    }

    if let Some(limiter) = &ctx.rate_limiter {
        let method = req.method().clone();
        let path = req.uri().path().to_string();
        let key = principal.user_id.clone();

        match limiter.check(&key, &method, &path, &principal) {
            Ok(()) => {}
            Err(retry_after) => {
                // Sampled audit (every time for now; production can sample)
                let audit_event = crate::audit::AuditEvent {
                    id: None,
                    ts: 0,
                    event_type: "rate_limit.exceeded".to_string(),
                    principal: key.clone(),
                    principal_kind: match principal.auth_method {
                        crate::auth::AuthMethod::Jwt => "oidc",
                        crate::auth::AuthMethod::ApiKey => "api_key",
                        crate::auth::AuthMethod::WsTicket => "ws_ticket",
                    }
                    .to_string(),
                    component_id: None,
                    target: Some(path.clone()),
                    details: Some(format!("method={}", method)),
                    deny_reason: Some("rate limit exceeded".to_string()),
                    source_ip: None,
                };
                // Fire and forget; don't block response on audit
                let audit = ctx.audit.clone();
                tokio::spawn(async move {
                    let _ = audit.log(&audit_event).await;
                });

                warn!(
                    principal = %key,
                    path = %path,
                    method = %method,
                    "Rate limit exceeded"
                );

                return (
                    StatusCode::TOO_MANY_REQUESTS,
                    [("Retry-After", retry_after.to_string())],
                    "Too Many Requests",
                )
                    .into_response();
            }
        }
    }

    next.run(req).await
}

/// Create a rate limiter with Phase 2 recommended defaults (or env overrides).
pub fn new_default_rate_limiter() -> Arc<ApiRateLimiter> {
    let read = std::env::var("VELOCE_RATE_LIMIT_READ_PER_MIN")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(600);
    let mutation = std::env::var("VELOCE_RATE_LIMIT_MUTATION_PER_MIN")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(60);
    let admin = std::env::var("VELOCE_RATE_LIMIT_ADMIN_PER_MIN")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30);

    Arc::new(ApiRateLimiter::new(read, mutation, admin))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Method;

    fn test_principal(user: &str, roles: Vec<&str>) -> crate::auth::AuthenticatedPrincipal {
        crate::auth::AuthenticatedPrincipal {
            user_id: user.to_string(),
            roles: roles.into_iter().map(|s| s.to_string()).collect(),
            auth_method: crate::auth::AuthMethod::ApiKey,
        }
    }

    #[test]
    fn basic_mutation_limit() {
        let limiter = ApiRateLimiter::new(1000, 2, 1000);

        let p = test_principal("u1", vec!["submitter"]);

        assert!(limiter
            .check("u1", &Method::POST, "/api/v1/jobs", &p)
            .is_ok());
        assert!(limiter
            .check("u1", &Method::POST, "/api/v1/jobs", &p)
            .is_ok());
        assert!(limiter
            .check("u1", &Method::POST, "/api/v1/jobs", &p)
            .is_err());
    }

    #[test]
    fn separate_buckets_per_class() {
        let limiter = ApiRateLimiter::new(1, 1, 1);
        let p = test_principal("u2", vec![]);

        assert!(limiter
            .check("u2", &Method::GET, "/api/v1/jobs", &p)
            .is_ok());
        assert!(limiter
            .check("u2", &Method::GET, "/api/v1/jobs", &p)
            .is_err());

        assert!(limiter
            .check("u2", &Method::POST, "/api/v1/jobs", &p)
            .is_ok());
    }

    #[test]
    fn admin_limit() {
        let limiter = ApiRateLimiter::new(100, 100, 1);
        let p = test_principal("admin", vec!["admin"]);

        assert!(limiter
            .check("admin", &Method::POST, "/api/v1/admin/foo", &p)
            .is_ok());
        assert!(limiter
            .check("admin", &Method::POST, "/api/v1/admin/foo", &p)
            .is_err());
    }
}
