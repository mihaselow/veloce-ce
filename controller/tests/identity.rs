use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use futures::{SinkExt, StreamExt};
use reqwest::StatusCode;
use sha2::{Digest, Sha256};
use std::net::TcpStream as StdTcpStream;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use veloce_common::{Message, MessageCodec};

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

fn hash_token(token: &str) -> String {
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
    ) -> Self {
        let log_file =
            std::fs::File::create(format!("../controller_identity_test_run_{}.log", port)).unwrap();
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

        cmd.env("VELOCE_ALLOW_INSECURE", "true");
        cmd.env("VELOCE_SECRET", secret);
        if let Some(json) = static_keys_json {
            cmd.env("VELOCE_API_KEYS", json);
        }
        cmd.env("VELOCE_CONTAINER_DB_URL", "sqlite://");
        cmd.env("VELOCE_REQUIRE_CLIENT_TOKENS", "true");
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
            if StdTcpStream::connect(format!("127.0.0.1:{}", self.port)).is_ok() {
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
        let _ = std::fs::remove_file(format!("../controller_identity_test_run_{}.log", self.port));
    }
}

async fn issue_token(
    client: &reqwest::Client,
    port: u16,
    component_id: &str,
    roles: Vec<&str>,
    admin_key: &str,
) -> String {
    let payload = serde_json::json!({
        "component_id": component_id.to_string(),
        "component_type": "client".to_string(),
        "roles": roles.into_iter().map(|s| s.to_string()).collect::<Vec<_>>(),
    });

    let resp = client
        .post(&format!(
            "https://127.0.0.1:{}/api/v1/admin/components",
            port
        ))
        .header("X-API-KEY", admin_key)
        .json(&payload)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    #[derive(serde::Deserialize)]
    struct IssueResponse {
        token: String,
    }
    let body: IssueResponse = resp.json().await.unwrap();
    body.token
}

#[tokio::test]
async fn test_rest_and_noise_principal_binding() {
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();

    let user_plaintext = "veloce_tok_user_123";
    let admin_plaintext = "veloce_tok_admin_123";
    let operator_plaintext = "veloce_tok_operator_123";

    let api_keys_json = format!(
        "[{{\"id\":\"user-key\",\"key_hash\":\"{}\",\"roles\":[\"submitter\"],\"linked_component_id\":\"user1\"}},\
          {{\"id\":\"admin-key\",\"key_hash\":\"{}\",\"roles\":[\"admin\"],\"linked_component_id\":\"admin1\"}},\
          {{\"id\":\"operator-key\",\"key_hash\":\"{}\",\"roles\":[\"operator\"],\"linked_component_id\":\"operator1\"}}]",
        hash_token(user_plaintext),
        hash_token(admin_plaintext),
        hash_token(operator_plaintext)
    );

    let server = CustomTestServer::start(
        8893,
        9094,
        "test-master-api-key",
        "test_secret",
        Some(&api_keys_json),
    );
    server.wait_ready();

    // ----------------------------------------------------
    // REST Verification Tests
    // ----------------------------------------------------

    // 1. Submit job with mismatching user_id as a regular user -> expect 403 Forbidden
    let payload_spoof = serde_json::json!({
        "binary": "solve",
        "args": [],
        "req_nodes": 1,
        "req_cores": 1,
        "req_memory": 1024,
        "walltime": 3600,
        "priority": 10,
        "user_id": "user2", // Mismatch with authenticated user1
        "working_directory": "/tmp",
    });

    let resp = client
        .post("https://127.0.0.1:8893/api/v1/jobs")
        .header("X-API-KEY", user_plaintext)
        .json(&payload_spoof)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    let body = resp.text().await.unwrap();
    assert!(body.contains("Forbidden: cannot override user ID"));

    // 2. Submit job with matching user_id as a regular user -> expect 201 Created
    let payload_matching = serde_json::json!({
        "binary": "solve",
        "args": [],
        "req_nodes": 1,
        "req_cores": 1,
        "req_memory": 1024,
        "walltime": 3600,
        "priority": 10,
        "user_id": "user1",
        "working_directory": "/tmp",
    });

    let resp = client
        .post("https://127.0.0.1:8893/api/v1/jobs")
        .header("X-API-KEY", user_plaintext)
        .json(&payload_matching)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    #[derive(serde::Deserialize)]
    struct SubmitResp {
        job_id: u64,
    }
    let body: SubmitResp = resp.json().await.unwrap();
    let first_job_id = body.job_id;

    // Verify job owner is indeed user1
    let resp = client
        .get(&format!(
            "https://127.0.0.1:8893/api/v1/jobs/{}",
            first_job_id
        ))
        .header("X-API-KEY", user_plaintext)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let job_info: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(job_info["user_id"], "user1");

    // 3. Submit job with empty user_id as a regular user -> expect 201 Created, auto-populated to user1
    let payload_empty = serde_json::json!({
        "binary": "solve",
        "args": [],
        "req_nodes": 1,
        "req_cores": 1,
        "req_memory": 1024,
        "walltime": 3600,
        "priority": 10,
        "user_id": "",
        "working_directory": "/tmp",
    });

    let resp = client
        .post("https://127.0.0.1:8893/api/v1/jobs")
        .header("X-API-KEY", user_plaintext)
        .json(&payload_empty)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let body: SubmitResp = resp.json().await.unwrap();
    let second_job_id = body.job_id;

    // Verify job owner resolved to user1
    let resp = client
        .get(&format!(
            "https://127.0.0.1:8893/api/v1/jobs/{}",
            second_job_id
        ))
        .header("X-API-KEY", user_plaintext)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let job_info: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(job_info["user_id"], "user1");

    // 4. Submit job with mismatching user_id as admin -> expect 201 Created (allowed override)
    let payload_admin_override = serde_json::json!({
        "binary": "solve",
        "args": [],
        "req_nodes": 1,
        "req_cores": 1,
        "req_memory": 1024,
        "walltime": 3600,
        "priority": 10,
        "user_id": "other_user",
        "working_directory": "/tmp",
    });

    let resp = client
        .post("https://127.0.0.1:8893/api/v1/jobs")
        .header("X-API-KEY", admin_plaintext)
        .json(&payload_admin_override)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let body: SubmitResp = resp.json().await.unwrap();
    let admin_job_id = body.job_id;

    // Verify job owner is other_user
    let resp = client
        .get(&format!(
            "https://127.0.0.1:8893/api/v1/jobs/{}",
            admin_job_id
        ))
        .header("X-API-KEY", admin_plaintext)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let job_info: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(job_info["user_id"], "other_user");

    // ----------------------------------------------------
    // Noise RPC Verification Tests
    // ----------------------------------------------------

    // Issue component tokens to allow Noise RPC connections with roles
    let noise_user_token = issue_token(
        &client,
        8893,
        "noise_user1",
        vec!["submitter"],
        "test-master-api-key",
    )
    .await;
    let noise_admin_token = issue_token(
        &client,
        8893,
        "noise_admin1",
        vec!["admin"],
        "test-master-api-key",
    )
    .await;

    // Helper to connect and authenticate via Noise RPC
    let connect_and_auth = |client_id: &'static str, token: String| async move {
        let stream = TcpStream::connect("127.0.0.1:9094").await.unwrap();
        let noise_stream = veloce_common::noise::upgrade_initiator(stream, "test_secret")
            .await
            .unwrap();
        let mut framed = Framed::new(noise_stream, MessageCodec::new());

        let hello = Message::HelloClient {
            client_id: client_id.to_string(),
            registration_token: Some(token),
        };
        framed.send(hello).await.unwrap();

        match framed.next().await {
            Some(Ok(Message::Ack)) => Ok(framed),
            Some(Ok(Message::Error(e))) => Err(e),
            other => Err(format!("Unexpected handshake response: {:?}", other)),
        }
    };

    let msg_template = Message::Submit {
        job_name: None,
        job_comment: None,
        binary: "solve".to_string(),
        args: vec![],
        req_nodes: 1,
        req_cores: 1,
        req_memory: 1024,
        walltime: 3600,
        priority: 10,
        user_id: "".to_string(),
        working_directory: "/tmp".to_string(),
        array_indices: None,
        inputs: vec![],
        gres_req: std::collections::BTreeMap::new(),
        env_vars: vec![],
        wait_for_licenses: false,
        estimated_walltime: None,
        priority_offset: None,
        dependencies: None,
        dependency_specs: None,
        qos: veloce_common::QosLevel::Production,
        image_uri: None,
        vnc_enabled: false,
        inherit_host_env: false,
        env_allowlist: None,
        job_profile: None,
    };

    // Case A: Submit job via Noise with mismatching user_id as regular user (noise_user1) -> expect Message::Error
    let mut f_user = connect_and_auth("noise_user1", noise_user_token.clone())
        .await
        .unwrap();
    let mut msg_spoof = msg_template.clone();
    if let Message::Submit {
        ref mut user_id, ..
    } = msg_spoof
    {
        *user_id = "other_user".to_string();
    }
    f_user.send(msg_spoof).await.unwrap();
    let resp = f_user.next().await.unwrap().unwrap();
    match resp {
        Message::Error(e) => assert!(e.contains("Forbidden: cannot override user ID")),
        other => panic!("Expected Message::Error for spoofing, got: {:?}", other),
    }

    // Case B: Submit job via Noise with empty user_id as regular user (noise_user1) -> expect Message::JobId
    let mut msg_empty = msg_template.clone();
    if let Message::Submit {
        ref mut user_id, ..
    } = msg_empty
    {
        *user_id = "".to_string();
    }
    f_user.send(msg_empty).await.unwrap();
    let resp = f_user.next().await.unwrap().unwrap();
    let noise_job_id = match resp {
        Message::JobId { job_id } => job_id,
        other => panic!("Expected Message::JobId, got: {:?}", other),
    };

    // Verify job owner resolved to noise_user1
    let resp = client
        .get(&format!(
            "https://127.0.0.1:8893/api/v1/jobs/{}",
            noise_job_id
        ))
        .header("X-API-KEY", admin_plaintext)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let job_info: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(job_info["user_id"], "noise_user1");

    // Case C: Submit job via Noise with mismatching user_id as admin (noise_admin1) -> expect Message::JobId (allowed override)
    let mut f_admin = connect_and_auth("noise_admin1", noise_admin_token)
        .await
        .unwrap();
    let mut msg_admin_override = msg_template.clone();
    if let Message::Submit {
        ref mut user_id, ..
    } = msg_admin_override
    {
        *user_id = "some_other_user".to_string();
    }
    f_admin.send(msg_admin_override).await.unwrap();
    let resp = f_admin.next().await.unwrap().unwrap();
    let admin_noise_job_id = match resp {
        Message::JobId { job_id } => job_id,
        other => panic!("Expected Message::JobId, got: {:?}", other),
    };

    // Verify job owner is indeed some_other_user
    let resp = client
        .get(&format!(
            "https://127.0.0.1:8893/api/v1/jobs/{}",
            admin_noise_job_id
        ))
        .header("X-API-KEY", admin_plaintext)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let job_info: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(job_info["user_id"], "some_other_user");
}
