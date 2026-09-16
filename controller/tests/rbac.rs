use std::net::TcpStream as StdTcpStream;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::Duration;

#[path = "helpers/test_certs.rs"]
mod test_certs;

fn workspace_root() -> PathBuf {
    test_certs::workspace_root()
}

fn controller_binary() -> PathBuf {
    if let Some(path) = option_env!("CARGO_BIN_EXE_veloce_controller") {
        PathBuf::from(path)
    } else {
        workspace_root().join("target/debug/veloce-controller")
    }
}
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use veloce_common::{Message, MessageCodec};

struct TestServer {
    child: Child,
}

impl TestServer {
    fn start() -> Self {
        test_certs::ensure_test_certs();
        let log_file = std::fs::File::create("../controller_rbac_test_run.log").unwrap();
        let child = Command::new(controller_binary())
            .current_dir(workspace_root())
            .args([
                "--api-port",
                "8887",
                "--api-key",
                "test-api-key",
                "--bind",
                "127.0.0.1:9098",
                "--cert-path",
                "certs/cert.pem",
                "--key-path",
                "certs/key.pem",
            ])
            .env("VELOCE_ALLOW_INSECURE", "true")
            .env("VELOCE_CONTAINER_DB_URL", "sqlite://")
            .env("VELOCE_REQUIRE_CLIENT_TOKENS", "true")
            .env("VELOCE_SECRET", "test_secret")
            .stdout(log_file.try_clone().unwrap())
            .stderr(log_file)
            .spawn()
            .expect("Failed to start veloce-controller");

        let mut retries = 0;
        loop {
            if StdTcpStream::connect("127.0.0.1:8887").is_ok() {
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
                                   // let _ = std::fs::remove_file("../controller_rbac_test_run.log");
    }
}

#[derive(serde::Serialize)]
struct IssueComponentTokenRequest {
    pub component_id: String,
    pub component_type: String,
    pub roles: Vec<String>,
}

#[derive(serde::Deserialize)]
#[allow(dead_code)]
struct IssueComponentTokenResponse {
    pub component_id: String,
    pub token: String,
}

async fn issue_token(client: &reqwest::Client, component_id: &str, roles: Vec<&str>) -> String {
    let payload = IssueComponentTokenRequest {
        component_id: component_id.to_string(),
        component_type: "client".to_string(),
        roles: roles.into_iter().map(|s| s.to_string()).collect(),
    };

    let resp = client
        .post("https://127.0.0.1:8887/api/v1/admin/components")
        .header("X-API-KEY", "test-api-key")
        .json(&payload)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    let body: IssueComponentTokenResponse = resp.json().await.unwrap();
    body.token
}

#[tokio::test]
async fn test_noise_rpc_rbac_integration() {
    let _server = TestServer::start();

    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();

    // 1. Issue tokens for clients with different roles
    let submitter_token = issue_token(&client, "client_submitter", vec!["submitter"]).await;
    let operator_token = issue_token(&client, "client_operator", vec!["operator"]).await;
    let viewer_token = issue_token(&client, "client_viewer", vec!["viewer"]).await;

    // 2. Helper to connect and authenticate via Noise RPC
    let connect_and_auth = |client_id: &'static str, token: String| async move {
        let stream = TcpStream::connect("127.0.0.1:9098").await.unwrap();
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

    // Message templates
    let msg_submit = Message::Submit {
        job_name: None,
        job_comment: None,
        binary: "solve".to_string(),
        args: vec![],
        req_nodes: 1,
        req_cores: 1,
        req_memory: 1024,
        walltime: 3600,
        priority: 10,
        user_id: "user1".to_string(),
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

    let msg_reservation = Message::CreateReservation {
        nodes: std::collections::HashSet::new(),
        start_time: 0,
        end_time: 100,
        owner: "operator".to_string(),
    };

    let msg_syslogs = Message::GetSystemLogs {
        request_id: 1,
        component_id: "worker1".to_string(),
        log_source: "syslog".to_string(),
        lines: 100,
    };

    // --- Submitter Test ---
    let mut f_submitter = connect_and_auth("client_submitter", submitter_token.clone())
        .await
        .unwrap();
    // Submitter can submit (should not be unauthorized, might be other error or OK)
    f_submitter.send(msg_submit.clone()).await.unwrap();
    let resp = f_submitter.next().await.unwrap().unwrap();
    if let Message::Error(ref e) = resp {
        assert!(
            !e.contains("Permission denied: unauthorized operation"),
            "Submitter should not be unauthorized"
        );
    }
    // Submitter cannot create reservation
    f_submitter.send(msg_reservation.clone()).await.unwrap();
    let resp = f_submitter.next().await.unwrap().unwrap();
    match resp {
        Message::Error(e) => assert!(e.contains("Permission denied: unauthorized operation")),
        other => panic!("Expected permission denied error, got: {:?}", other),
    }

    // --- Operator Test ---
    let mut f_operator = connect_and_auth("client_operator", operator_token.clone())
        .await
        .unwrap();
    // Operator cannot submit
    f_operator.send(msg_submit.clone()).await.unwrap();
    let resp = f_operator.next().await.unwrap().unwrap();
    match resp {
        Message::Error(e) => assert!(e.contains("Permission denied: unauthorized operation")),
        other => panic!("Expected permission denied error, got: {:?}", other),
    }
    // Operator can create reservation (or get a validation error but not unauthorized)
    f_operator.send(msg_reservation.clone()).await.unwrap();
    let resp = f_operator.next().await.unwrap().unwrap();
    if let Message::Error(ref e) = resp {
        assert!(
            !e.contains("Permission denied: unauthorized operation"),
            "Operator should not be unauthorized"
        );
    }
    // Operator can get system logs
    f_operator.send(msg_syslogs.clone()).await.unwrap();
    let resp = f_operator.next().await.unwrap().unwrap();
    if let Message::Error(ref e) = resp {
        assert!(
            !e.contains("Permission denied: unauthorized operation"),
            "Operator should not be unauthorized"
        );
    }

    // --- Viewer Test ---
    let mut f_viewer = connect_and_auth("client_viewer", viewer_token.clone())
        .await
        .unwrap();
    // Viewer cannot submit
    f_viewer.send(msg_submit.clone()).await.unwrap();
    let resp = f_viewer.next().await.unwrap().unwrap();
    match resp {
        Message::Error(e) => assert!(e.contains("Permission denied: unauthorized operation")),
        other => panic!("Expected permission denied error, got: {:?}", other),
    }
    // Viewer can list jobs
    f_viewer
        .send(Message::ListJobs { state_filter: None })
        .await
        .unwrap();
    let resp = f_viewer.next().await.unwrap().unwrap();
    match resp {
        Message::JobList(list) => {
            assert!(
                list.is_empty(),
                "Viewer should only see their own jobs, found: {:?}",
                list
            );
        }
        other => panic!("Expected JobList response, got: {:?}", other),
    }

    // ==========================================
    // REST API RBAC Integration Tests
    // ==========================================

    // Submitter REST Test
    // 1. Submitter can submit a job
    let submit_payload = serde_json::json!({
        "binary": "solve",
        "args": [],
        "req_nodes": 1,
        "req_cores": 1,
        "req_memory": 1024,
        "walltime": 3600,
        "priority": 10,
        "user_id": "client_submitter",
        "working_directory": "/tmp",
        "qos": "Production"
    });

    let resp = client
        .post("https://127.0.0.1:8887/api/v1/jobs")
        .header("X-API-KEY", &submitter_token)
        .json(&submit_payload)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    let submit_res: serde_json::Value = resp.json().await.unwrap();
    let job_id = submit_res.get("job_id").unwrap().as_u64().unwrap();

    // 2. Viewer cannot submit a job (returns FORBIDDEN)
    let resp = client
        .post("https://127.0.0.1:8887/api/v1/jobs")
        .header("X-API-KEY", &viewer_token)
        .json(&submit_payload)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::FORBIDDEN);

    // 3. Submitter can cancel their own job
    let resp = client
        .delete(format!("https://127.0.0.1:8887/api/v1/jobs/{}", job_id))
        .header("X-API-KEY", &submitter_token)
        .send()
        .await
        .unwrap();
    assert!(
        resp.status() == reqwest::StatusCode::OK || resp.status() == reqwest::StatusCode::FORBIDDEN
    );

    // Let's create another job by the submitter to test ownership restrictions
    let resp = client
        .post("https://127.0.0.1:8887/api/v1/jobs")
        .header("X-API-KEY", &submitter_token)
        .json(&submit_payload)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    let submit_res2: serde_json::Value = resp.json().await.unwrap();
    let job_id2 = submit_res2.get("job_id").unwrap().as_u64().unwrap();

    // 4. Viewer cannot cancel submitter's job (returns FORBIDDEN)
    let resp = client
        .delete(format!("https://127.0.0.1:8887/api/v1/jobs/{}", job_id2))
        .header("X-API-KEY", &viewer_token)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::FORBIDDEN);

    // 5. Viewer cannot view submitter's job details (returns FORBIDDEN)
    let resp = client
        .get(format!("https://127.0.0.1:8887/api/v1/jobs/{}", job_id2))
        .header("X-API-KEY", &viewer_token)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::FORBIDDEN);

    // 6. Submitter can view their own job details
    let resp = client
        .get(format!("https://127.0.0.1:8887/api/v1/jobs/{}", job_id2))
        .header("X-API-KEY", &submitter_token)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // 7. Viewer only lists their own jobs (which is none, so they get empty list)
    let resp = client
        .get("https://127.0.0.1:8887/api/v1/jobs")
        .header("X-API-KEY", &viewer_token)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let jobs_list: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert!(jobs_list
        .iter()
        .all(|j| j.get("user_id").unwrap().as_str().unwrap() == "client_viewer"));

    // 8. Submitter can list jobs (sees all, including job_id2)
    let resp = client
        .get("https://127.0.0.1:8887/api/v1/jobs")
        .header("X-API-KEY", &submitter_token)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let jobs_list2: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert!(jobs_list2
        .iter()
        .any(|j| j.get("id").unwrap().as_u64().unwrap() == job_id2));

    // 9. Submitter / Viewer cannot create reservations (returns FORBIDDEN)
    let reservation_payload = serde_json::json!({
        "nodes": [],
        "start_time": 0,
        "end_time": 100,
        "owner": "operator"
    });
    let resp = client
        .post("https://127.0.0.1:8887/api/v1/reservations")
        .header("X-API-KEY", &submitter_token)
        .json(&reservation_payload)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::FORBIDDEN);

    let resp = client
        .post("https://127.0.0.1:8887/api/v1/reservations")
        .header("X-API-KEY", &viewer_token)
        .json(&reservation_payload)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::FORBIDDEN);

    // 10. Submitter / Viewer cannot delete reservations (returns FORBIDDEN)
    let resp = client
        .delete("https://127.0.0.1:8887/api/v1/reservations/res_id")
        .header("X-API-KEY", &submitter_token)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::FORBIDDEN);

    // 11. Submitter / Viewer / Operator cannot call system restart (admin only)
    let resp = client
        .post("https://127.0.0.1:8887/api/v1/system/restart")
        .header("X-API-KEY", &operator_token)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::FORBIDDEN);

    let resp = client
        .post("https://127.0.0.1:8887/api/v1/system/restart")
        .header("X-API-KEY", &submitter_token)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::FORBIDDEN);

    // ==========================================
    // Noise RPC Job Ownership Checks
    // ==========================================

    // 12. Viewer cannot cancel submitter's job
    f_viewer
        .send(Message::CancelJob { job_id: job_id2 })
        .await
        .unwrap();
    let resp = f_viewer.next().await.unwrap().unwrap();
    match resp {
        Message::Error(e) => assert!(e.contains("Permission denied: unauthorized operation")),
        other => panic!("Expected permission denied error, got: {:?}", other),
    }

    // 13. Viewer cannot get logs of submitter's job
    f_viewer
        .send(Message::GetLogs {
            request_id: 100,
            job_id: job_id2,
            log_type: veloce_common::LogType::Stdout,
            offset: 0,
            length: None,
            working_directory: None,
            rank: None,
        })
        .await
        .unwrap();
    let resp = f_viewer.next().await.unwrap().unwrap();
    match resp {
        Message::Error(e) => assert!(e.contains("Permission denied: unauthorized operation")),
        other => panic!("Expected permission denied error, got: {:?}", other),
    }

    // 14. Viewer cannot get job events of submitter's job
    f_viewer
        .send(Message::GetJobEvents { job_id: job_id2 })
        .await
        .unwrap();
    let resp = f_viewer.next().await.unwrap().unwrap();
    match resp {
        Message::Error(e) => assert!(e.contains("Permission denied: unauthorized operation")),
        other => panic!("Expected permission denied error, got: {:?}", other),
    }

    // 15. Viewer cannot get efficiency stats
    f_viewer
        .send(Message::GetEfficiencyStats {
            solver_name: Some("solve".to_string()),
            binary_name: None,
        })
        .await
        .unwrap();
    let resp = f_viewer.next().await.unwrap().unwrap();
    match resp {
        Message::Error(e) => assert!(e.contains("Permission denied: unauthorized operation")),
        other => panic!("Expected permission denied error, got: {:?}", other),
    }

    // 16. Viewer cannot get metrics
    f_viewer
        .send(Message::GetMetrics {
            start_time: None,
            end_time: None,
            nodes: None,
            aggregate: false,
        })
        .await
        .unwrap();
    let resp = f_viewer.next().await.unwrap().unwrap();
    match resp {
        Message::Error(e) => assert!(e.contains("Permission denied: unauthorized operation")),
        other => panic!("Expected permission denied error, got: {:?}", other),
    }
}
