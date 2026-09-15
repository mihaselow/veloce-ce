use axum::{
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
};

use crate::config::AppState;

pub(crate) fn s3_api_key_authorized(req: &Request, api_key: &str) -> bool {
    req.headers()
        .get("X-API-KEY")
        .and_then(|h| h.to_str().ok())
        .is_some_and(|k| k == api_key)
}

/// Dev-only legacy auth: substring match of api_key in Authorization or presigned query.
/// Disabled when `VELOCE_ALLOW_INSECURE` is not set.
pub(crate) fn s3_legacy_auth_authorized(req: &Request, api_key: &str) -> bool {
    if let Some(auth) = req
        .headers()
        .get("Authorization")
        .and_then(|h| h.to_str().ok())
    {
        if auth.starts_with("AWS4-HMAC-SHA256") && auth.contains(api_key) {
            return true;
        }
    }
    if let Some(query) = req.uri().query() {
        if query.contains("X-Amz-Signature=") && query.contains(api_key) {
            return true;
        }
    }
    false
}

pub(crate) fn s3_request_authorized(req: &Request, api_key: &str, allow_insecure: bool) -> bool {
    if s3_api_key_authorized(req, api_key) {
        return true;
    }
    allow_insecure && s3_legacy_auth_authorized(req, api_key)
}
pub(crate) async fn auth_middleware(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Response {
    let auth_header = req
        .headers()
        .get("X-API-KEY")
        .and_then(|header| header.to_str().ok());

    match auth_header {
        Some(key) if key == state.api_key => next.run(req).await,
        _ => (StatusCode::UNAUTHORIZED, "Invalid or missing API key").into_response(),
    }
}

pub(crate) async fn s3_auth_middleware(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    if s3_request_authorized(&req, &state.api_key, state.allow_insecure) {
        Ok(next.run(req).await)
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}
