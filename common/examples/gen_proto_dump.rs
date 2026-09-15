use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::Write;
use std::path::Path;
use veloce_common::*;

fn write_golden<T: serde::Serialize>(name: &str, data: &T) {
    let bytes = bincode::serialize(data).expect("Serialization failed");
    let path = Path::new("ports/go/tests/golden").join(format!("{}.bin", name));
    let mut file = File::create(&path).expect("Failed to create file");
    file.write_all(&bytes).expect("Failed to write bytes");
    println!("Wrote {}", path.display());
}

fn main() {
    // 1. Resources
    let resources = Resources {
        cpu_cores: 4,
        total_memory: 1024 * 1024 * 1024,
        free_memory: 512 * 1024 * 1024,
        cpu_usage: 12.5,
        cpu_model: "Intel Core i7".to_string(),
        arch: "x86_64".to_string(),
        os_name: "Linux".to_string(),
        os_version: "5.15.0".to_string(),
        kernel_version: "#1 SMP".to_string(),
        host_name: "worker-1".to_string(),
        load_avg: [0.1, 0.5, 0.9],
        disk_total: 100 * 1024 * 1024 * 1024,
        disk_free: 50 * 1024 * 1024 * 1024,
        uptime: 3600,
        boot_time: 1234567800,
        process_count: 100,
        swap_total: 8 * 1024 * 1024 * 1024,
        swap_free: 4 * 1024 * 1024 * 1024,
        version: "v1.0.0".to_string(),
        gres: BTreeMap::new(),
        cgroup_enabled: true,
    };
    write_golden("Resources", &resources);

    // 2. JobStatus Variants
    write_golden("JobStatus_Pending", &JobStatus::Pending);
    write_golden("JobStatus_Running", &JobStatus::Running);
    write_golden("JobStatus_Completed", &JobStatus::Completed(0));
    write_golden("JobStatus_Completed_Neg", &JobStatus::Completed(-1));
    write_golden(
        "JobStatus_Failed",
        &JobStatus::Failed("OOM Killed".to_string()),
    );
    write_golden("JobStatus_Killed", &JobStatus::Killed);

    // 3. Message::HelloClient
    write_golden(
        "Message_HelloClient",
        &Message::HelloClient {
            client_id: "client-uuid-123".to_string(),
            registration_token: Some("client-token-abc".to_string()),
        },
    );

    // 4. Message::HelloWorker
    write_golden(
        "Message_HelloWorker",
        &Message::HelloWorker {
            worker_id: "worker-uuid-123".to_string(),
            hostname: "worker-1".to_string(),
            resources: resources.clone(),
            features: Some(0),
            registration_token: None,
        },
    );

    // 5. Message::Submit (Complex Vec<String>, Options)
    write_golden(
        "Message_Submit",
        &Message::Submit {
            job_name: Some("sleep-demo".to_string()),
            job_comment: Some("golden protocol sample".to_string()),
            binary: "/bin/sleep".to_string(),
            args: vec!["10".to_string(), "--verbose".to_string()],
            req_nodes: 2,
            req_cores: 4,
            req_memory: 2048,
            walltime: 3600,
            priority: 10,
            user_id: "alice".to_string(),
            working_directory: "/home/alice".to_string(),
            array_indices: None,
            inputs: Vec::new(),
            gres_req: BTreeMap::new(),
            env_vars: Vec::new(),
            wait_for_licenses: false,
            estimated_walltime: None,
            priority_offset: None,
            dependencies: None,
            dependency_specs: None,
            qos: QosLevel::Production,
            image_uri: None,
            vnc_enabled: false,
            inherit_host_env: false,
            env_allowlist: None,
            job_profile: None,
        },
    );

    // 6. Message::RunJob (Option<Vec<usize>>, u64)
    write_golden(
        "Message_RunJob",
        &Message::RunJob {
            job_id: 1001,
            binary: "mpirun".to_string(),
            args: vec![],
            node_list: vec!["10.0.0.1".to_string(), "10.0.0.2".to_string()],
            assigned_cores: Some(vec![0, 2, 4]),
            req_memory: 1024,
            working_directory: "/tmp".to_string(),
            user_id: "bob".to_string(),
            walltime: 0,
            submission_time: 1234567890,
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
            qos: QosLevel::Production,
            container_asset: None,
            vnc_enabled: false,
            inherit_host_env: false,
            env_allowlist: None,
            job_profile: None,
        },
    );

    // 7. Message::RunJob (None assigned_cores)
    write_golden(
        "Message_RunJob_NoCores",
        &Message::RunJob {
            job_id: 1002,
            binary: "simple".to_string(),
            args: vec![],
            node_list: vec!["localhost".to_string()],
            assigned_cores: None,
            req_memory: 0,
            working_directory: "/".to_string(),
            user_id: "root".to_string(),
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
            qos: QosLevel::Production,
            container_asset: None,
            vnc_enabled: false,
            inherit_host_env: false,
            env_allowlist: None,
            job_profile: None,
        },
    );

    // 8. Message with Payload (JobOutput)
    write_golden(
        "Message_JobOutput",
        &Message::JobOutput {
            job_id: 500,
            data: vec![0xDE, 0xAD, 0xBE, 0xEF],
        },
    );
}
