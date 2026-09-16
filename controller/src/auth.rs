use crate::SharedContext;
use axum::{
    extract::{Query, State},
    http::{header, HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use jsonwebtoken::jwk::{Jwk, JwkSet};
use jsonwebtoken::{Algorithm, DecodingKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

fn generate_pkce_pair() -> (String, String) {
    let u1 = uuid::Uuid::new_v4();
    let u2 = uuid::Uuid::new_v4();
    let mut bytes = [0u8; 32];
    bytes[..16].copy_from_slice(u1.as_bytes());
    bytes[16..].copy_from_slice(u2.as_bytes());
    let verifier = URL_SAFE_NO_PAD.encode(bytes);
    let mut hasher = Sha256::new();
    hasher.update(verifier.as_bytes());
    let challenge = URL_SAFE_NO_PAD.encode(hasher.finalize());
    (verifier, challenge)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum AuthMethod {
    Jwt,
    ApiKey,
    WsTicket,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthenticatedPrincipal {
    pub user_id: String,
    pub roles: Vec<String>,
    pub auth_method: AuthMethod,
}

impl AuthenticatedPrincipal {
    pub fn is_admin_or_operator(&self) -> bool {
        self.roles.iter().any(|r| r == "admin" || r == "operator")
    }

    pub fn resolve_user_id(&self, payload_user_id: &str) -> Result<String, String> {
        if self.is_admin_or_operator() {
            if payload_user_id.is_empty() {
                Ok(self.user_id.clone())
            } else {
                Ok(payload_user_id.to_string())
            }
        } else {
            if payload_user_id.is_empty() {
                Ok(self.user_id.clone())
            } else if payload_user_id != self.user_id {
                Err(format!(
                    "Forbidden: cannot override user ID '{}' with '{}'",
                    self.user_id, payload_user_id
                ))
            } else {
                Ok(self.user_id.clone())
            }
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct StaticApiKey {
    pub id: String,
    pub key_hash: String,
    pub roles: Vec<String>,
    pub linked_component_id: Option<String>,
}

pub struct JwksCache {
    keys: dashmap::DashMap<String, DecodingKey>,
}

impl JwksCache {
    pub fn new() -> Self {
        Self {
            keys: dashmap::DashMap::new(),
        }
    }

    pub async fn get_key(&self, kid: &str, app_url: &str) -> Option<DecodingKey> {
        if let Some(key) = self.keys.get(kid) {
            return Some(key.clone());
        }

        // Fetch JWKS from provider
        let url = format!("{}/.well-known/jwks.json", app_url);
        let client = reqwest::Client::builder()
            .danger_accept_invalid_certs(!cfg!(feature = "production"))
            .build()
            .unwrap_or_default();

        match client.get(&url).send().await {
            Ok(resp) => {
                if let Ok(jwks) = resp.json::<JwkSet>().await {
                    for jwk in jwks.keys {
                        if let Some(ref kid_val) = jwk.common.key_id {
                            if let Ok(decoding_key) = decoding_key_from_jwk(&jwk) {
                                self.keys.insert(kid_val.clone(), decoding_key);
                            }
                        }
                    }
                }
            }
            Err(e) => {
                log::error!("Failed to fetch JWKS from OIDC provider at {}: {}", url, e);
            }
        }

        self.keys.get(kid).map(|k| k.clone())
    }
}

fn decoding_key_from_jwk(jwk: &Jwk) -> Result<DecodingKey, jsonwebtoken::errors::Error> {
    DecodingKey::from_jwk(jwk)
}

struct OidcConfig {
    client_id: String,
    client_secret: String,
    app_url: String,
    tenant_id: Option<String>,
}

impl OidcConfig {
    fn from_env() -> Option<Self> {
        let client_id = std::env::var("FUSIONAUTH_CLIENT_ID").ok()?;
        let client_secret = std::env::var("FUSIONAUTH_CLIENT_SECRET").ok()?;
        let app_url = std::env::var("FUSIONAUTH_APP_URL")
            .unwrap_or_else(|_| "http://localhost:9011".to_string());
        let tenant_id = std::env::var("FUSIONAUTH_TENANT_ID").ok();

        Some(Self {
            client_id,
            client_secret,
            app_url,
            tenant_id,
        })
    }
}

fn request_origin(headers: &HeaderMap) -> (String, String) {
    let proto = headers
        .get("X-Forwarded-Proto")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("http")
        .to_string();
    let host = headers
        .get("X-Forwarded-Host")
        .or_else(|| headers.get("Host"))
        .and_then(|h| h.to_str().ok())
        .unwrap_or("localhost:82")
        .to_string();
    (proto, host)
}

/// Browser-reachable FusionAuth base URL for authorize/logout redirects.
/// Set `FUSIONAUTH_PUBLIC_URL=auto` (default) to derive `http(s)://<request-host>:9011`
/// from `X-Forwarded-Host` / `Host` so LAN clients are not sent to localhost.
fn fusionauth_browser_url(headers: &HeaderMap) -> String {
    match std::env::var("FUSIONAUTH_PUBLIC_URL") {
        Ok(url) if !url.is_empty() && url != "auto" => url,
        _ => {
            let (proto, host) = request_origin(headers);
            let hostname = host.split(':').next().unwrap_or(host.as_str());
            let port =
                std::env::var("FUSIONAUTH_OAUTH_PORT").unwrap_or_else(|_| "9011".to_string());
            format!("{}://{}:{}", proto, hostname, port)
        }
    }
}

pub fn get_cookie(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(header::COOKIE)
        .and_then(|val| val.to_str().ok())
        .and_then(|s| {
            s.split(';')
                .map(|p| p.trim())
                .find(|p| p.starts_with(&format!("{}=", name)))
                .map(|p| p[name.len() + 1..].to_string())
        })
}

pub async fn verify_jwt(
    ctx: &SharedContext,
    token: &str,
    app_url: &str,
) -> Result<AuthenticatedPrincipal, String> {
    let header = match jsonwebtoken::decode_header(token) {
        Ok(h) => h,
        Err(e) => return Err(format!("Invalid token header: {}", e)),
    };

    let kid = match header.kid {
        Some(ref k) => k,
        None => return Err("Missing key ID (kid) in token header".to_string()),
    };

    let decoding_key = match ctx.jwks_cache.get_key(kid, app_url).await {
        Some(key) => key,
        None => return Err(format!("Key ID {} not found in JWKS", kid)),
    };

    let mut validation = jsonwebtoken::Validation::new(Algorithm::RS256);
    validation.validate_exp = true;

    if let Ok(aud) = std::env::var("FUSIONAUTH_CLIENT_ID") {
        validation.set_audience(&[aud]);
    } else {
        validation.validate_aud = false;
    }
    validation.leeway = 60;

    #[derive(Deserialize)]
    struct CustomClaims {
        sub: String,
        roles: Option<Vec<String>>,
    }

    match jsonwebtoken::decode::<CustomClaims>(token, &decoding_key, &validation) {
        Ok(token_data) => {
            let roles = token_data.claims.roles.unwrap_or_default();
            Ok(AuthenticatedPrincipal {
                user_id: token_data.claims.sub,
                roles,
                auth_method: AuthMethod::Jwt,
            })
        }
        Err(e) => Err(format!("JWT validation failed: {}", e)),
    }
}

pub async fn login_handler(headers: HeaderMap) -> impl IntoResponse {
    let oidc = match OidcConfig::from_env() {
        Some(c) => c,
        None => {
            return (StatusCode::INTERNAL_SERVER_ERROR, "OIDC is not configured").into_response()
        }
    };

    let state = uuid::Uuid::new_v4().to_string();

    let (proto, host) = request_origin(&headers);
    let redirect_uri = format!("{}://{}/auth/callback", proto, host);
    let (code_verifier, code_challenge) = generate_pkce_pair();
    let fusionauth_url = fusionauth_browser_url(&headers);

    let mut auth_url = format!(
        "{}/oauth2/authorize?client_id={}&response_type=code&redirect_uri={}&state={}&code_challenge={}&code_challenge_method=S256",
        fusionauth_url,
        oidc.client_id,
        urlencoding::encode(&redirect_uri),
        state,
        urlencoding::encode(&code_challenge),
    );

    if let Some(ref tenant_id) = oidc.tenant_id {
        auth_url.push_str(&format!("&tenantId={}", tenant_id));
    }

    let secure_flag = if proto == "https" { "Secure;" } else { "" };
    let state_cookie = format!(
        "veloce_oidc_state={}; HttpOnly; Path=/; SameSite=Lax; {} Max-Age=300",
        state, secure_flag
    );
    let verifier_cookie = format!(
        "veloce_oidc_verifier={}; HttpOnly; Path=/; SameSite=Lax; {} Max-Age=300",
        code_verifier, secure_flag
    );
    let mut response_headers = HeaderMap::new();
    response_headers.append(header::SET_COOKIE, state_cookie.parse().unwrap());
    response_headers.append(header::SET_COOKIE, verifier_cookie.parse().unwrap());
    response_headers.insert(header::LOCATION, auth_url.parse().unwrap());

    (StatusCode::FOUND, response_headers).into_response()
}

pub async fn callback_handler(
    State(ctx): State<SharedContext>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let oidc = match OidcConfig::from_env() {
        Some(c) => c,
        None => {
            return (StatusCode::INTERNAL_SERVER_ERROR, "OIDC is not configured").into_response()
        }
    };

    let code = match params.get("code") {
        Some(c) => c,
        None => return (StatusCode::BAD_REQUEST, "Missing authorization code").into_response(),
    };

    let state = match params.get("state") {
        Some(s) => s,
        None => return (StatusCode::BAD_REQUEST, "Missing state parameter").into_response(),
    };

    // Verify state against cookie
    let cookie_state = get_cookie(&headers, "veloce_oidc_state");
    if cookie_state.as_deref() != Some(state) {
        return (StatusCode::BAD_REQUEST, "State mismatch").into_response();
    }

    let code_verifier = match get_cookie(&headers, "veloce_oidc_verifier") {
        Some(v) => v,
        None => return (StatusCode::BAD_REQUEST, "Missing PKCE verifier").into_response(),
    };

    // Determine redirect URI
    let (proto, host) = request_origin(&headers);
    let redirect_uri = format!("{}://{}/auth/callback", proto, host);

    // Exchange code for token
    let token_url = format!("{}/oauth2/token", oidc.app_url);
    let mut form_data = HashMap::new();
    form_data.insert("grant_type", "authorization_code");
    form_data.insert("code", code);
    form_data.insert("client_id", &oidc.client_id);
    form_data.insert("client_secret", &oidc.client_secret);
    form_data.insert("redirect_uri", &redirect_uri);
    form_data.insert("code_verifier", &code_verifier);

    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(!cfg!(feature = "production"))
        .build()
        .unwrap();

    let resp = match client.post(&token_url).form(&form_data).send().await {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to reach OIDC token endpoint: {}", e),
            )
                .into_response()
        }
    };

    if !resp.status().is_success() {
        let err_text = resp.text().await.unwrap_or_default();
        // P1-6.1 audit
        let _ = ctx
            .audit
            .log(&crate::audit::AuditEvent::auth_login(
                false, "unknown", "oidc",
            ))
            .await;
        return (
            StatusCode::BAD_REQUEST,
            format!("Token exchange failed: {}", err_text),
        )
            .into_response();
    }

    #[derive(Deserialize)]
    struct TokenResponse {
        access_token: String,
        refresh_token: Option<String>,
    }

    let token_resp: TokenResponse = match resp.json().await {
        Ok(t) => t,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to parse token response: {}", e),
            )
                .into_response()
        }
    };

    // Validate the token
    let principal = match verify_jwt(&ctx, &token_resp.access_token, &oidc.app_url).await {
        Ok(p) => p,
        Err(e) => {
            // P1-6.1 audit
            let _ = ctx
                .audit
                .log(&crate::audit::AuditEvent::auth_login(
                    false, "unknown", "oidc",
                ))
                .await;
            return (
                StatusCode::UNAUTHORIZED,
                format!("Invalid session token: {}", e),
            )
                .into_response();
        }
    };

    // P1-6.1 audit successful OIDC login
    let _ = ctx
        .audit
        .log(&crate::audit::AuditEvent::auth_login(
            true,
            &principal.user_id,
            "oidc",
        ))
        .await;

    // Set cookies
    let mut response_headers = HeaderMap::new();

    let secure_flag = if proto == "https" { "Secure;" } else { "" };
    let session_cookie = format!(
        "veloce_session_token={}; HttpOnly; Path=/; SameSite=Lax; {} Max-Age=3600",
        token_resp.access_token, secure_flag
    );
    response_headers.append(header::SET_COOKIE, session_cookie.parse().unwrap());

    if let Some(refresh_token) = token_resp.refresh_token {
        let refresh_cookie = format!(
            "veloce_refresh_token={}; HttpOnly; Path=/; SameSite=Lax; {} Max-Age=2592000",
            refresh_token, secure_flag
        );
        response_headers.append(header::SET_COOKIE, refresh_cookie.parse().unwrap());
    }

    // Clear state cookie
    response_headers.append(
        header::SET_COOKIE,
        "veloce_oidc_state=; HttpOnly; Path=/; Max-Age=0"
            .parse()
            .unwrap(),
    );
    response_headers.append(
        header::SET_COOKIE,
        "veloce_oidc_verifier=; HttpOnly; Path=/; Max-Age=0"
            .parse()
            .unwrap(),
    );

    // Redirect to web root
    response_headers.insert(header::LOCATION, "/".parse().unwrap());
    (StatusCode::FOUND, response_headers).into_response()
}

pub async fn logout_handler(headers: HeaderMap) -> impl IntoResponse {
    let oidc = match OidcConfig::from_env() {
        Some(c) => c,
        None => {
            return (StatusCode::INTERNAL_SERVER_ERROR, "OIDC is not configured").into_response()
        }
    };

    let mut response_headers = HeaderMap::new();

    // Clear cookies
    response_headers.append(
        header::SET_COOKIE,
        "veloce_session_token=; HttpOnly; Path=/; Max-Age=0"
            .parse()
            .unwrap(),
    );
    response_headers.append(
        header::SET_COOKIE,
        "veloce_refresh_token=; HttpOnly; Path=/; Max-Age=0"
            .parse()
            .unwrap(),
    );

    let (proto, host) = request_origin(&headers);
    let post_logout_redirect_uri = format!("{}://{}/", proto, host);
    let fusionauth_url = fusionauth_browser_url(&headers);
    let logout_url = format!(
        "{}/oauth2/logout?client_id={}&post_logout_redirect_uri={}",
        fusionauth_url,
        oidc.client_id,
        urlencoding::encode(&post_logout_redirect_uri),
    );

    response_headers.insert(header::LOCATION, logout_url.parse().unwrap());
    (StatusCode::FOUND, response_headers).into_response()
}

pub async fn session_handler(
    State(ctx): State<SharedContext>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let oidc = match OidcConfig::from_env() {
        Some(c) => c,
        None => {
            return Json(serde_json::json!({
                "authenticated": false,
                "error": "OIDC is not configured"
            }))
            .into_response();
        }
    };

    let session_token = match get_cookie(&headers, "veloce_session_token") {
        Some(t) => t,
        None => {
            return Json(serde_json::json!({
                "authenticated": false
            }))
            .into_response();
        }
    };

    match verify_jwt(&ctx, &session_token, &oidc.app_url).await {
        Ok(principal) => Json(serde_json::json!({
            "authenticated": true,
            "user_id": principal.user_id,
            "roles": principal.roles
        }))
        .into_response(),
        Err(_) => Json(serde_json::json!({
            "authenticated": false
        }))
        .into_response(),
    }
}

pub async fn refresh_handler(headers: HeaderMap) -> impl IntoResponse {
    let oidc = match OidcConfig::from_env() {
        Some(c) => c,
        None => {
            return (StatusCode::INTERNAL_SERVER_ERROR, "OIDC is not configured").into_response()
        }
    };

    let refresh_token = match get_cookie(&headers, "veloce_refresh_token") {
        Some(t) => t,
        None => return (StatusCode::BAD_REQUEST, "Missing refresh token").into_response(),
    };

    let token_url = format!("{}/oauth2/token", oidc.app_url);
    let mut form_data = HashMap::new();
    form_data.insert("grant_type", "refresh_token");
    form_data.insert("refresh_token", &refresh_token);
    form_data.insert("client_id", &oidc.client_id);
    form_data.insert("client_secret", &oidc.client_secret);

    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(!cfg!(feature = "production"))
        .build()
        .unwrap();

    let resp = match client.post(&token_url).form(&form_data).send().await {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to reach OIDC token endpoint: {}", e),
            )
                .into_response()
        }
    };

    if !resp.status().is_success() {
        let err_text = resp.text().await.unwrap_or_default();
        return (
            StatusCode::BAD_REQUEST,
            format!("Token refresh failed: {}", err_text),
        )
            .into_response();
    }

    #[derive(Deserialize)]
    struct TokenRefreshResponse {
        access_token: String,
        refresh_token: Option<String>,
    }

    let token_resp: TokenRefreshResponse = match resp.json().await {
        Ok(t) => t,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to parse token response: {}", e),
            )
                .into_response()
        }
    };

    let proto = headers
        .get("X-Forwarded-Proto")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("http");

    let mut response_headers = HeaderMap::new();
    let secure_flag = if proto == "https" { "Secure;" } else { "" };

    let session_cookie = format!(
        "veloce_session_token={}; HttpOnly; Path=/; SameSite=Lax; {} Max-Age=3600",
        token_resp.access_token, secure_flag
    );
    response_headers.append(header::SET_COOKIE, session_cookie.parse().unwrap());

    if let Some(new_refresh) = token_resp.refresh_token {
        let refresh_cookie = format!(
            "veloce_refresh_token={}; HttpOnly; Path=/; SameSite=Lax; {} Max-Age=2592000",
            new_refresh, secure_flag
        );
        response_headers.append(header::SET_COOKIE, refresh_cookie.parse().unwrap());
    }

    (
        StatusCode::OK,
        response_headers,
        Json(serde_json::json!({ "success": true })),
    )
        .into_response()
}

pub fn routes(_ctx: SharedContext) -> Router<SharedContext> {
    Router::new()
        .route("/auth/login", get(login_handler))
        .route("/auth/callback", get(callback_handler))
        .route("/auth/logout", get(logout_handler))
        .route("/auth/session", get(session_handler))
        .route("/auth/refresh", post(refresh_handler))
}
