use std::net::TcpStream as StdTcpStream;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::Duration;

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
use axum::{
    extract::{Multipart, Path},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};

struct TestMockFileserver {
    port: u16,
    _handle: tokio::task::JoinHandle<()>,
}

async fn mock_upload(headers: HeaderMap, mut multipart: Multipart) -> impl IntoResponse {
    let api_key = headers.get("X-API-KEY").and_then(|h| h.to_str().ok());
    if api_key != Some("mock-fileserver-key") {
        return (StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
    }

    let mut received_file = false;
    while let Ok(Some(field)) = multipart.next_field().await {
        let name = field.name().unwrap_or("file").to_string();
        if name == "file" {
            let file_name = field.file_name().unwrap_or("unknown.txt").to_string();
            let content_type = field
                .content_type()
                .unwrap_or("application/octet-stream")
                .to_string();
            let bytes = field.bytes().await.unwrap();
            if file_name == "test.txt" && content_type == "text/plain" && bytes == "hello world" {
                received_file = true;
            }
        }
    }

    if received_file {
        (
            StatusCode::OK,
            Json(serde_json::json!({
                "file_id": "test-file-123",
                "filename": "test.txt"
            })),
        )
            .into_response()
    } else {
        (StatusCode::BAD_REQUEST, "Missing or invalid file").into_response()
    }
}

async fn mock_download(headers: HeaderMap, Path(file_id): Path<String>) -> impl IntoResponse {
    let api_key = headers.get("X-API-KEY").and_then(|h| h.to_str().ok());
    if api_key != Some("mock-fileserver-key") {
        return (StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
    }
    if file_id == "test-file-123" {
        axum::response::Response::builder()
            .header("content-type", "text/plain")
            .body(axum::body::Body::from("hello world"))
            .unwrap()
            .into_response()
    } else {
        (StatusCode::NOT_FOUND, "Not Found").into_response()
    }
}

async fn mock_delete(headers: HeaderMap, Path(file_id): Path<String>) -> impl IntoResponse {
    let api_key = headers.get("X-API-KEY").and_then(|h| h.to_str().ok());
    if api_key != Some("mock-fileserver-key") {
        return (StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
    }
    if file_id == "test-file-123" {
        StatusCode::NO_CONTENT.into_response()
    } else {
        StatusCode::NOT_FOUND.into_response()
    }
}

impl TestMockFileserver {
    async fn start() -> Self {
        let app = Router::new()
            .route("/api/v1/files", post(mock_upload))
            .route("/api/v1/files/:id", get(mock_download).delete(mock_delete));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let _handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        Self { port, _handle }
    }
}

struct TestController {
    child: Child,
}

impl TestController {
    fn start(fileserver_url: &str) -> Self {
        let log_file =
            std::fs::File::create("../controller_fileserver_proxy_test_run.log").unwrap();
        let child = Command::new(controller_binary())
            .current_dir(workspace_root())
            .args([
                "--api-port",
                "8899",
                "--api-key",
                "test-api-key",
                "--bind",
                "127.0.0.1:9199",
                "--cert-path",
                "certs/cert.pem",
                "--key-path",
                "certs/key.pem",
            ])
            .env("VELOCE_ALLOW_INSECURE", "true")
            .env("VELOCE_CONTAINER_DB_URL", "sqlite://")
            .env("VELOCE_REQUIRE_CLIENT_TOKENS", "true")
            .env("VELOCE_SECRET", "test_secret")
            .env("VELOCE_FILESERVER_URL", fileserver_url)
            .env("VELOCE_FILESERVER_KEY", "mock-fileserver-key")
            .stdout(log_file.try_clone().unwrap())
            .stderr(log_file)
            .spawn()
            .expect("Failed to start veloce-controller");

        let mut retries = 0;
        loop {
            if StdTcpStream::connect("127.0.0.1:8899").is_ok() {
                break;
            }
            std::thread::sleep(Duration::from_millis(200));
            retries += 1;
            if retries > 300 {
                panic!("Controller failed to start after 60 seconds");
            }
        }

        Self { child }
    }
}

impl Drop for TestController {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_file("../controller_fileserver_proxy_test_run.log");
    }
}

#[derive(serde::Serialize)]
struct IssueComponentTokenRequest {
    pub component_id: String,
    pub component_type: String,
    pub roles: Vec<String>,
}

#[derive(serde::Deserialize)]
struct IssueComponentTokenResponse {
    pub token: String,
}

async fn issue_token(client: &reqwest::Client, component_id: &str, roles: Vec<&str>) -> String {
    let payload = IssueComponentTokenRequest {
        component_id: component_id.to_string(),
        component_type: "client".to_string(),
        roles: roles.into_iter().map(|s| s.to_string()).collect(),
    };

    let resp = client
        .post("https://127.0.0.1:8899/api/v1/admin/components")
        .header("X-API-KEY", "test-api-key")
        .json(&payload)
        .send()
        .await
        .unwrap();

    let status = resp.status();
    let body_text = resp.text().await.unwrap();
    assert_eq!(
        status,
        reqwest::StatusCode::CREATED,
        "Failed to issue token. Status: {}. Response: {}",
        status,
        body_text
    );
    let body: IssueComponentTokenResponse = serde_json::from_str(&body_text).unwrap();
    body.token
}

#[tokio::test]
async fn test_fileserver_proxy_integration() {
    // 1. Start mock fileserver
    let mock_fileserver = TestMockFileserver::start().await;
    let fileserver_url = format!("http://127.0.0.1:{}", mock_fileserver.port);

    // 2. Start controller
    let _controller = TestController::start(&fileserver_url);

    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();

    // 3. Issue roles tokens
    let admin_token = issue_token(&client, "admin_client", vec!["admin"]).await;
    let submitter_token = issue_token(&client, "submitter_client", vec!["submitter"]).await;
    let viewer_token = issue_token(&client, "viewer_client", vec!["viewer"]).await;

    // --- UPLOAD TEST ---
    // Test 1: Upload without token should fail (401 Unauthorized)
    let form = reqwest::multipart::Form::new().text("file", "hello world");
    let resp = client
        .post("https://127.0.0.1:8899/api/v1/files")
        .multipart(form)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);

    // Test 2: Upload with viewer token should fail (403 Forbidden)
    let form = reqwest::multipart::Form::new().part(
        "file",
        reqwest::multipart::Part::bytes("hello world".as_bytes())
            .file_name("test.txt")
            .mime_str("text/plain")
            .unwrap(),
    );
    let resp = client
        .post("https://127.0.0.1:8899/api/v1/files")
        .header("X-API-KEY", &viewer_token)
        .multipart(form)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::FORBIDDEN);

    // Test 3: Upload with submitter token should succeed
    let form = reqwest::multipart::Form::new().part(
        "file",
        reqwest::multipart::Part::bytes("hello world".as_bytes())
            .file_name("test.txt")
            .mime_str("text/plain")
            .unwrap(),
    );
    let resp = client
        .post("https://127.0.0.1:8899/api/v1/files")
        .header("X-API-KEY", &submitter_token)
        .multipart(form)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let upload_res: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        upload_res.get("file_id").unwrap().as_str().unwrap(),
        "test-file-123"
    );

    // --- DOWNLOAD TEST ---
    // Test 4: Download without token should fail (401 Unauthorized)
    let resp = client
        .get("https://127.0.0.1:8899/api/v1/files/test-file-123")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);

    // Test 5: Download with viewer token should succeed
    let resp = client
        .get("https://127.0.0.1:8899/api/v1/files/test-file-123")
        .header("X-API-KEY", &viewer_token)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap(),
        "text/plain"
    );
    let content = resp.text().await.unwrap();
    assert_eq!(content, "hello world");

    // Test 6: Download with non-existent file id should return 404
    let resp = client
        .get("https://127.0.0.1:8899/api/v1/files/non-existent-id")
        .header("X-API-KEY", &viewer_token)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    // --- DELETE TEST ---
    // Test 7: Delete with viewer token should fail (403 Forbidden)
    let resp = client
        .delete("https://127.0.0.1:8899/api/v1/files/test-file-123")
        .header("X-API-KEY", &viewer_token)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::FORBIDDEN);

    // Test 8: Delete with admin token should succeed
    let resp = client
        .delete("https://127.0.0.1:8899/api/v1/files/test-file-123")
        .header("X-API-KEY", &admin_token)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NO_CONTENT);

    // Test 9: Delete with non-existent file id should return 404
    let resp = client
        .delete("https://127.0.0.1:8899/api/v1/files/non-existent-id")
        .header("X-API-KEY", &admin_token)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_sqlx_connect() {
    sqlx::any::install_default_drivers();

    let formats = vec![
        ("sqlite:rbac_test_format1.db", "rbac_test_format1.db"),
        ("sqlite://rbac_test_format2.db", "rbac_test_format2.db"),
        (
            "sqlite:///Users/michaelhaselow/_devel/veloce/rbac_test_format3.db",
            "rbac_test_format3.db",
        ),
        ("sqlite:./rbac_test_format4.db", "rbac_test_format4.db"),
        ("sqlite://./rbac_test_format5.db", "rbac_test_format5.db"),
    ];

    for (url, filename) in formats {
        let _ = std::fs::remove_file(filename);
        let _ = std::fs::remove_file(format!("controller/{}", filename));

        // Pre-create the file
        if let Ok(mut f) = std::fs::File::create(format!("controller/{}", filename)) {
            use std::io::Write;
            let _ = f.write_all(&[]);
        }

        let pool = sqlx::any::AnyPoolOptions::new()
            .max_connections(1)
            .connect(url)
            .await;

        let pool_ok = pool.is_ok();
        let err_msg = if let Err(e) = pool {
            format!("{:?}", e)
        } else {
            "None".to_string()
        };

        let file_exists = std::fs::metadata(filename).is_ok()
            || std::fs::metadata(format!("controller/{}", filename)).is_ok();
        println!(
            "URL: {} -> Result: {}, Error: {}, File exists: {}",
            url, pool_ok, err_msg, file_exists
        );

        let _ = std::fs::remove_file(filename);
        let _ = std::fs::remove_file(format!("controller/{}", filename));
    }
}
