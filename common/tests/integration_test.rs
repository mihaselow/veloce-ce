use futures::{SinkExt, StreamExt};
use std::collections::{BTreeMap, HashMap};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_util::codec::Framed;
use veloce_common::{Message, MessageCodec, Resources};

#[tokio::test]
async fn test_handshake_integration() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let (tx_ctrl, mut rx_ctrl) = mpsc::channel(1);

    tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut framed = Framed::new(socket, MessageCodec::new());

        if let Some(Ok(msg)) = framed.next().await {
            if let Message::HelloWorker { worker_id, .. } = msg {
                tx_ctrl.send(worker_id).await.unwrap();
            }
        }
    });

    tokio::spawn(async move {
        let socket = TcpStream::connect(addr).await.unwrap();
        let mut framed = Framed::new(socket, MessageCodec::new());

        let resources = Resources {
            cpu_cores: 4,
            total_memory: 1024,
            free_memory: 512,
            cpu_usage: 0.0,
            cpu_model: "TestCPU".to_string(),
            arch: "x86_64".to_string(),
            host_name: "test-host".to_string(),
            kernel_version: "0.0.0".to_string(),
            os_name: "Linux".to_string(),
            os_version: "1.0".to_string(),
            load_avg: [0.0, 0.0, 0.0],
            disk_total: 0,
            disk_free: 0,
            uptime: 0,
            boot_time: 0,
            process_count: 0,
            swap_total: 0,
            swap_free: 0,
            version: "v1".to_string(),
            gres: BTreeMap::new(),
            cgroup_enabled: false,
        };

        framed
            .send(Message::HelloWorker {
                worker_id: "worker-1".to_string(),
                hostname: "localhost".to_string(),
                resources,
                features: Some(0),
                registration_token: None,
            })
            .await
            .unwrap();
    });

    let received_worker_id = rx_ctrl.recv().await.unwrap();
    assert_eq!(received_worker_id, "worker-1");
}

#[tokio::test]
async fn test_job_protocol_flow() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let (tx, mut rx) = mpsc::channel(10);

    tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut framed = Framed::new(socket, MessageCodec::new());

        let _ = framed.next().await;

        let run_msg = Message::RunJob {
            job_id: 100,
            container_asset: None,
            binary: "/bin/echo".into(),
            args: vec!["hello".into()],
            node_list: vec!["127.0.0.1".into()],
            assigned_cores: Some(vec![0]),
            req_memory: 100,
            working_directory: "/tmp".into(),
            user_id: "test".into(),
            walltime: 0,
            submission_time: 0,
            array_id: None,
            array_task_id: None,
            inputs: Vec::new(),
            allocated_gres: HashMap::new(),
            gres_req: BTreeMap::new(),
            secret: "secret".into(),
            controller_url: "url".into(),
            env_vars: Vec::new(),
            wait_for_licenses: false,
            estimated_walltime: None,
            priority_offset: None,
            dependencies: None,
            dependency_specs: None,
            qos: veloce_common::QosLevel::Production,
            vnc_enabled: false,
            inherit_host_env: false,
            env_allowlist: None,
            job_profile: None,
        };
        framed.send(run_msg).await.unwrap();

        if let Some(Ok(msg)) = framed.next().await {
            tx.send(msg).await.unwrap();
        }

        if let Some(Ok(msg)) = framed.next().await {
            tx.send(msg).await.unwrap();
        }
    });

    tokio::spawn(async move {
        let socket = TcpStream::connect(addr).await.unwrap();
        let mut framed = Framed::new(socket, MessageCodec::new());

        let resources = Resources {
            cpu_cores: 4,
            total_memory: 1024,
            free_memory: 512,
            cpu_usage: 0.0,
            cpu_model: "TestCPU".to_string(),
            arch: "x86_64".to_string(),
            host_name: "test-host".to_string(),
            kernel_version: "0.0.0".to_string(),
            os_name: "Linux".to_string(),
            os_version: "1.0".to_string(),
            load_avg: [0.0, 0.0, 0.0],
            disk_total: 0,
            disk_free: 0,
            uptime: 0,
            boot_time: 0,
            process_count: 0,
            swap_total: 0,
            swap_free: 0,
            version: "v1".to_string(),
            gres: BTreeMap::new(),
            cgroup_enabled: false,
        };
        framed
            .send(Message::HelloWorker {
                worker_id: "worker-1".into(),
                hostname: "localhost".into(),
                resources,
                features: Some(0),
                registration_token: None,
            })
            .await
            .unwrap();

        if let Some(Ok(Message::RunJob { job_id, .. })) = framed.next().await {
            framed.send(Message::JobStarted { job_id }).await.unwrap();
            framed
                .send(Message::JobDone {
                    job_id,
                    exit_code: 0,
                })
                .await
                .unwrap();
        }
    });

    let msg1 = rx.recv().await.unwrap();
    match msg1 {
        Message::JobStarted { job_id } => assert_eq!(job_id, 100),
        _ => panic!("Expected JobStarted"),
    }

    let msg2 = rx.recv().await.unwrap();
    match msg2 {
        Message::JobDone { job_id, exit_code } => {
            assert_eq!(job_id, 100);
            assert_eq!(exit_code, 0);
        }
        _ => panic!("Expected JobDone"),
    }
}
