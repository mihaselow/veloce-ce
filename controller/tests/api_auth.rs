use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..")
}

fn controller_binary() -> PathBuf {
    if let Some(path) = option_env!("CARGO_BIN_EXE_veloce_controller") {
        PathBuf::from(path)
    } else {
        workspace_root().join("target/debug/veloce-controller")
    }
}

#[derive(serde::Serialize)]
struct Claims {
    sub: String,
    roles: Vec<String>,
    exp: u64,
    iss: String,
    aud: String,
}

async fn mock_jwks() -> impl axum::response::IntoResponse {
    Json(serde_json::json!({
        "keys": [
            {
                "kty": "RSA",
                "use": "sig",
                "alg": "RS256",
                "kid": "mock-key-id",
                "n": "ze44DMkfbnzQqjg0q5IY_VJRXJLUwjaMxsThS9sEULShLhwJ7UKUwP0qEIau2Q4d0N8_2sLn6M3EhalEmnd62nHhaoTgs3jzx1Kt3J8XTrsUKBXXMa9aH1LuqocAMalQ_OnNTGSO1OQt1gG_aQ2jeGaDLMsVWejT6nmK1mC8FEwkJJg8uYtZEI91Ic3HSfUDnjzrwxwROSLbB4nLcpvnqUGCs7rLtC9bhKU2_pkOykl7mDgrPWb8uDW17xMvEI8moG2dIfov12jrp53BzbuKHwtfpkOgIoqTxdd4RjM1qC_tG_xe6UlG5KD68cwrsd3E3KPQRbnNJVYv07xKp7FvvQ",
                "e": "AQAB"
            }
        ]
    }))
}

async fn mock_token(State(encoding_key): State<EncodingKey>) -> impl axum::response::IntoResponse {
    let claims = Claims {
        sub: "test-user-123".to_string(),
        roles: vec!["admin".to_string(), "operator".to_string()],
        exp: (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600),
        iss: "http://127.0.0.1:8989".to_string(),
        aud: "veloce-web".to_string(),
    };

    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some("mock-key-id".to_string());

    let token = jsonwebtoken::encode(&header, &claims, &encoding_key).unwrap();

    Json(serde_json::json!({
        "access_token": token,
        "refresh_token": "mock-refresh-token-123"
    }))
}

async fn mock_logout() -> impl axum::response::IntoResponse {
    StatusCode::OK
}

async fn start_mock_oidc_server() -> tokio::task::JoinHandle<()> {
    let private_key_pem = r#"-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEAze44DMkfbnzQqjg0q5IY/VJRXJLUwjaMxsThS9sEULShLhwJ
7UKUwP0qEIau2Q4d0N8/2sLn6M3EhalEmnd62nHhaoTgs3jzx1Kt3J8XTrsUKBXX
Ma9aH1LuqocAMalQ/OnNTGSO1OQt1gG/aQ2jeGaDLMsVWejT6nmK1mC8FEwkJJg8
uYtZEI91Ic3HSfUDnjzrwxwROSLbB4nLcpvnqUGCs7rLtC9bhKU2/pkOykl7mDgr
PWb8uDW17xMvEI8moG2dIfov12jrp53BzbuKHwtfpkOgIoqTxdd4RjM1qC/tG/xe
6UlG5KD68cwrsd3E3KPQRbnNJVYv07xKp7FvvQIDAQABAoIBAAL4KWS9za85K4UY
1GGY9LVKZ5PvJhQ61yLSmfEPEmvbfut8SgRazmxN+jpMxt6oXnOxlGkiIFfyB6Bp
xWx4xpO5yqdPjTHpT5KTNaCVxq9C8VJ2pii4P5NuDbT1x2Hv8BQFhwlP9eNJ+wM3
+TuZj77fs4qEzyUBv3SFFiRrNqsQO1+p49lpmIRHkwHi24gHssmhDZPIsgJkWL7u
w9Ip/NP1eU8MLU9acs743tFThaYyPcsOR8ba2fEq/C0UkEdXWnILFs9ozRtg9WEm
alh++TXyd/241QzxCHixfWAwvY047AsRLuKYTpxIvUkw6levTal1/iYN8CB7HFDX
3nOPFt0CgYEA8iQOhFZYQPfAk+aTs3LWT2bKQBxcvg0NfqQpT0Rmk07rK62cHAgK
SErUi+ne3DbmxB6UZEJuWbpr3KvsDlgzmv4nqk2s69GboYU80vICkpaGtjJOUEg+
xyeu9yNXKi5DdNpWUYZD9n8gt6Cn/hUMAaa41OmpNsdTkrBAc2o+jscCgYEA2beY
NbtkasFIb5mJVoMUMDMoHgPc6K9fjM2zt8wxz1sRdX6J3ZTptEwNaB2TMdbehUc7
hr2T6sZGGS1HzzSHfSs0G9744Ehs6jTa+XKG7hLDhv8/mZlB7133Lpk2DadOdqJM
ZcKYoLGHbD3aWVXwjRg2Zc9xyomPiWAdfOoi2VsCgYEAwqByvrI8a7P4KalLDRD/
64B+jnt9nBEXyLQgtCMRo9PqOQhpkypvQV5Ma02HIVBLulWuBsxSsHKkYhIaQglp
KWqh7URT+pRXWMOkeRWnNbYh/259/g+jziY6f1D7rd7Tv6gDe7HFDOtwG8jZXuQB
643bwN8zcOFUbnKWy24ZbF8CgYAu+v0vaxaKKtc0rc8DChoLJJ7dizvaQi2+No03
diqxchdcYUfitsWPkHG8K9WdhZ5S6EIiGzqWCN8Lg8fhIJa0HeSKtxzBWR+XknxG
I76WFRp4QRA6VuXxfzddqNYPMDEwTGlr9Af3dReh9d7uNCtKZxUl9xO4/uIoZMM4
N1X5zQKBgQDJZ7DntqafZ6ik+KaI6qWIBfMHEoPDejjiCClfJLTuW8z1Vifg1ZCs
BsOrxN9skLeIhKClKYNALJsN7V13K6LriowpV8s0Q8+V0aUm8TisQq5YmnGxM2U/
3JrINkyteVyTPnU5ct0ecqQfq2kWdeDTS8HBmH4uc1X+2WkwiUx8gw==
-----END RSA PRIVATE KEY-----"#;

    let encoding_key = EncodingKey::from_rsa_pem(private_key_pem.as_bytes()).unwrap();

    let app = Router::new()
        .route("/.well-known/jwks.json", get(mock_jwks))
        .route("/oauth2/token", post(mock_token))
        .route("/oauth2/logout", get(mock_logout))
        .with_state(encoding_key);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:8989")
        .await
        .unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    })
}

struct TestServer {
    child: Child,
}

impl TestServer {
    fn start() -> Self {
        let log_file = std::fs::File::create("../controller_test_run.log").unwrap();
        let child = Command::new(controller_binary())
            .current_dir(workspace_root())
            .args(&[
                "--api-port",
                "8888",
                "--api-key",
                "test-api-key",
                "--bind",
                "127.0.0.1:9099",
                "--cert-path",
                "certs/cert.pem",
                "--key-path",
                "certs/key.pem",
            ])
            .env("VELOCE_ALLOW_INSECURE", "true")
            .env("VELOCE_CONTAINER_DB_URL", "sqlite://")
            .env("FUSIONAUTH_CLIENT_ID", "veloce-web")
            .env("FUSIONAUTH_CLIENT_SECRET", "test-secret")
            .env("FUSIONAUTH_APP_URL", "http://127.0.0.1:8989")
            .env("FUSIONAUTH_PUBLIC_URL", "http://127.0.0.1:8989")
            .stdout(log_file.try_clone().unwrap())
            .stderr(log_file)
            .spawn()
            .expect("Failed to start veloce-controller");

        let mut retries = 0;
        loop {
            if TcpStream::connect("127.0.0.1:8888").is_ok() {
                break;
            }
            std::thread::sleep(Duration::from_millis(200));
            retries += 1;
            if retries > 100 {
                panic!("Controller failed to start");
            }
        }

        Self { child }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait(); // Clean up zombied child
        let _ = std::fs::remove_file("../controller_test_run.log");
    }
}

#[tokio::test]
async fn test_api_auth_flows() {
    let _mock_oidc = start_mock_oidc_server().await;
    let _server = TestServer::start();

    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();

    let client_no_redirect = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    // 1. Verify 401 response without API key or session cookie
    let resp = client
        .get("https://127.0.0.1:8888/api/v1/jobs")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);

    // 2. Verify 200 response with valid X-API-KEY
    let resp = client
        .get("https://127.0.0.1:8888/api/v1/jobs")
        .header("X-API-KEY", "test-api-key")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // 3. Test OIDC BFF Login and Callback Flows
    let login_resp = client_no_redirect
        .get("https://127.0.0.1:8888/auth/login")
        .send()
        .await
        .unwrap();
    assert_eq!(login_resp.status(), reqwest::StatusCode::FOUND);
    let redirect_loc = login_resp
        .headers()
        .get("Location")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(redirect_loc.contains("/oauth2/authorize"));
    assert!(redirect_loc.contains("client_id=veloce-web"));
    assert!(redirect_loc.contains("code_challenge="));
    assert!(redirect_loc.contains("code_challenge_method=S256"));

    // Extract state cookie and state query param
    let login_cookies: Vec<String> = login_resp
        .headers()
        .get_all("Set-Cookie")
        .iter()
        .map(|h| {
            h.to_str()
                .unwrap()
                .split(';')
                .next()
                .unwrap()
                .trim()
                .to_string()
        })
        .collect();
    let state_cookie = login_cookies
        .iter()
        .find(|s| s.starts_with("veloce_oidc_state="))
        .cloned()
        .unwrap();
    let verifier_cookie = login_cookies
        .iter()
        .find(|s| s.starts_with("veloce_oidc_verifier="))
        .cloned()
        .unwrap();

    let state_query = redirect_loc
        .split('&')
        .find(|p| p.starts_with("state=") || p.contains("state="))
        .and_then(|p| p.split('=').nth(1))
        .unwrap();

    // Invoke callback
    let callback_resp = client_no_redirect
        .get(&format!(
            "https://127.0.0.1:8888/auth/callback?code=mock-code&state={}",
            state_query
        ))
        .header("Cookie", format!("{}; {}", state_cookie, verifier_cookie))
        .send()
        .await
        .unwrap();

    assert_eq!(callback_resp.status(), reqwest::StatusCode::FOUND);

    let cookies: Vec<String> = callback_resp
        .headers()
        .get_all("Set-Cookie")
        .iter()
        .map(|h| {
            h.to_str()
                .unwrap()
                .split(';')
                .next()
                .unwrap()
                .trim()
                .to_string()
        })
        .collect();

    let session_cookie = cookies
        .iter()
        .find(|c| c.starts_with("veloce_session_token="))
        .cloned()
        .expect("Missing veloce_session_token cookie");

    let refresh_cookie = cookies
        .iter()
        .find(|c| c.starts_with("veloce_refresh_token="))
        .cloned()
        .expect("Missing veloce_refresh_token cookie");

    // 4. Verify OIDC Session Route
    let session_resp = client
        .get("https://127.0.0.1:8888/auth/session")
        .header("Cookie", &session_cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(session_resp.status(), reqwest::StatusCode::OK);
    let session_json: serde_json::Value = session_resp.json().await.unwrap();
    assert_eq!(session_json["authenticated"], true);
    assert_eq!(session_json["user_id"], "test-user-123");
    assert_eq!(
        session_json["roles"],
        serde_json::json!(["admin", "operator"])
    );

    // 5. Verify access to protected endpoint using OIDC session cookie
    let api_cookie_resp = client
        .get("https://127.0.0.1:8888/api/v1/jobs")
        .header("Cookie", &session_cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(api_cookie_resp.status(), reqwest::StatusCode::OK);

    // 6. Verify access to protected endpoint using Bearer Token
    let token_val = &session_cookie["veloce_session_token=".len()..];
    let api_bearer_resp = client
        .get("https://127.0.0.1:8888/api/v1/jobs")
        .header("Authorization", format!("Bearer {}", token_val))
        .send()
        .await
        .unwrap();
    assert_eq!(api_bearer_resp.status(), reqwest::StatusCode::OK);

    // 7. Verify OIDC Session Refresh Route
    let refresh_resp = client
        .post("https://127.0.0.1:8888/auth/refresh")
        .header("Cookie", &refresh_cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(refresh_resp.status(), reqwest::StatusCode::OK);
    let refresh_cookies: Vec<String> = refresh_resp
        .headers()
        .get_all("Set-Cookie")
        .iter()
        .map(|h| {
            h.to_str()
                .unwrap()
                .split(';')
                .next()
                .unwrap()
                .trim()
                .to_string()
        })
        .collect();

    let refresh_json: serde_json::Value = refresh_resp.json().await.unwrap();
    assert_eq!(refresh_json["success"], true);

    let new_session_cookie = refresh_cookies
        .iter()
        .find(|c| c.starts_with("veloce_session_token="))
        .cloned()
        .expect("Missing refreshed session token cookie");

    // Verify the refreshed session works
    let api_new_cookie_resp = client
        .get("https://127.0.0.1:8888/api/v1/jobs")
        .header("Cookie", &new_session_cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(api_new_cookie_resp.status(), reqwest::StatusCode::OK);

    // 8. Verify WS ticket creation using session cookie authentication
    #[derive(serde::Serialize)]
    struct TicketReq {
        scope: String,
        job_id: Option<u64>,
    }

    #[derive(serde::Deserialize)]
    #[allow(dead_code)]
    struct TicketResp {
        ticket: String,
        expires_at: u64,
    }

    let payload = TicketReq {
        scope: "events".to_string(),
        job_id: None,
    };

    let resp = client
        .post("https://127.0.0.1:8888/api/v1/ws/ticket")
        .header("Cookie", &session_cookie)
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    let ticket_resp: TicketResp = resp.json().await.unwrap();
    let ticket = ticket_resp.ticket;

    // 9. Verify WS ticket authentication works
    let resp = client
        .get(&format!(
            "https://127.0.0.1:8888/api/v1/ws/events?ticket={}",
            ticket
        ))
        .send()
        .await
        .unwrap();
    assert_ne!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);

    // 10. Verify Logout Route clears session cookies and redirects with post_logout URI
    let logout_resp = client_no_redirect
        .get("https://127.0.0.1:8888/auth/logout")
        .header("X-Forwarded-Proto", "http")
        .header("X-Forwarded-Host", "localhost:82")
        .send()
        .await
        .unwrap();
    assert_eq!(logout_resp.status(), reqwest::StatusCode::FOUND);

    let location = logout_resp
        .headers()
        .get("Location")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(location.contains("post_logout_redirect_uri="));
    assert!(location.contains("http%3A%2F%2Flocalhost%3A82%2F"));

    let logout_cookies: Vec<String> = logout_resp
        .headers()
        .get_all("Set-Cookie")
        .iter()
        .map(|h| {
            h.to_str()
                .unwrap()
                .split(';')
                .next()
                .unwrap()
                .trim()
                .to_string()
        })
        .collect();

    assert!(logout_cookies.iter().any(|c| c == "veloce_session_token="));
    assert!(logout_cookies.iter().any(|c| c == "veloce_refresh_token="));
}

fn hash_token(token: &str) -> String {
    use base64::engine::general_purpose::STANDARD as BASE64;
    use base64::Engine;
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(token.as_bytes());
    BASE64.encode(hasher.finalize())
}

struct CustomTestServer {
    child: Child,
    port: u16,
}

impl CustomTestServer {
    fn start(
        port: u16,
        bind_port: u16,
        api_key: &str,
        secret: &str,
        static_keys_json: Option<&str>,
        allow_insecure: bool,
    ) -> Self {
        let log_file =
            std::fs::File::create(format!("../controller_test_run_{}.log", port)).unwrap();
        let mut cmd = Command::new(controller_binary());
        cmd.current_dir(workspace_root()).args(&[
            "--api-port",
            &port.to_string(),
            "--api-key",
            api_key,
            "--bind",
            &format!("127.0.0.1:{}", bind_port),
            "--cert-path",
            "certs/cert.pem",
            "--key-path",
            "certs/key.pem",
        ]);

        if allow_insecure {
            cmd.env("VELOCE_ALLOW_INSECURE", "true");
        } else {
            cmd.env("VELOCE_ALLOW_INSECURE", "false");
        }
        cmd.env("VELOCE_SECRET", secret);
        if let Some(json) = static_keys_json {
            cmd.env("VELOCE_API_KEYS", json);
        }
        cmd.env("VELOCE_CONTAINER_DB_URL", "sqlite://");
        cmd.env("FUSIONAUTH_CLIENT_ID", "veloce-web");
        cmd.env("FUSIONAUTH_CLIENT_SECRET", "test-secret");
        cmd.env("FUSIONAUTH_APP_URL", "http://127.0.0.1:8989");
        cmd.env("FUSIONAUTH_PUBLIC_URL", "http://127.0.0.1:8989");

        let child = cmd
            .stdout(log_file.try_clone().unwrap())
            .stderr(log_file)
            .spawn()
            .expect("Failed to start custom veloce-controller");

        Self { child, port }
    }

    fn wait_ready(&self) {
        let mut retries = 0;
        loop {
            if TcpStream::connect(format!("127.0.0.1:{}", self.port)).is_ok() {
                break;
            }
            std::thread::sleep(Duration::from_millis(200));
            retries += 1;
            if retries > 100 {
                panic!("Custom Controller failed to start");
            }
        }
    }
}

impl Drop for CustomTestServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait(); // Clean up zombied child
                                   // let _ = std::fs::remove_file(format!("../controller_test_run_{}.log", self.port));
    }
}

#[tokio::test]
async fn test_static_api_keys_and_secret_deprecation() {
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();

    let static_key_plaintext = "veloce_tok_test_key_abc_123";
    let static_key_hash = hash_token(static_key_plaintext);

    let api_keys_json = format!(
        "[{{\"id\":\"mcp-ha-key\",\"key_hash\":\"{}\",\"roles\":[\"operator\",\"viewer\"],\"linked_component_id\":\"mcp-ha\"}}]",
        static_key_hash
    );

    // 1. Run in hardened mode (VELOCE_ALLOW_INSECURE = false)
    {
        let server = CustomTestServer::start(
            8889,
            9098,
            "test-global-api-key",
            "my-cluster-secret",
            Some(&api_keys_json),
            false,
        );
        server.wait_ready();

        // A. Verify static api key allows access
        let resp = client
            .get("https://127.0.0.1:8889/api/v1/jobs")
            .header("X-API-KEY", static_key_plaintext)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK);

        // B. Verify cluster secret is denied as X-API-KEY in hardened mode
        let resp = client
            .get("https://127.0.0.1:8889/api/v1/jobs")
            .header("X-API-KEY", "my-cluster-secret")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);
    }

    // 2. Run in insecure mode (VELOCE_ALLOW_INSECURE = true)
    {
        // To allow the cluster secret as the api_key in validate_production_config, we start it with api_key == cluster_secret
        let server = CustomTestServer::start(
            8890,
            9097,
            "my-cluster-secret",
            "my-cluster-secret",
            None,
            true,
        );
        server.wait_ready();

        // Verify cluster secret is accepted when allowed
        let resp = client
            .get("https://127.0.0.1:8890/api/v1/jobs")
            .header("X-API-KEY", "my-cluster-secret")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
    }
}

#[tokio::test]
async fn test_validate_production_config_exit() {
    let log_file = std::fs::File::create("../controller_test_run_exit.log").unwrap();
    let mut child = Command::new(controller_binary())
        .current_dir(workspace_root())
        .args(&[
            "--api-port",
            "8891",
            "--api-key",
            "same-secret-key",
            "--bind",
            "127.0.0.1:9096",
            "--cert-path",
            "certs/cert.pem",
            "--key-path",
            "certs/key.pem",
        ])
        .env("VELOCE_ALLOW_INSECURE", "false")
        .env("VELOCE_SECRET", "same-secret-key")
        .env("VELOCE_CONTAINER_DB_URL", "sqlite://")
        .stdout(log_file.try_clone().unwrap())
        .stderr(log_file)
        .spawn()
        .expect("Failed to start veloce-controller");

    let mut exited = false;
    for _ in 0..50 {
        if let Ok(Some(status)) = child.try_wait() {
            exited = true;
            assert_eq!(status.code(), Some(1));
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(exited, "Process should have exited with code 1");
    let _ = std::fs::remove_file("../controller_test_run_exit.log");
}
