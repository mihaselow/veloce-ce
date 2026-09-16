use std::collections::{BTreeMap, HashMap};
use veloce_common::{JobInfo, JobStatus};

#[test]
fn test_job_info_serialization_with_reason() {
    let job = JobInfo {
        id: 1,
        job_name: Some("test-job".into()),
        job_comment: None,
        container_asset: None,
        binary: "test".into(),
        args: vec![],
        status: JobStatus::Pending,
        req_nodes: 1,
        req_cores: 1,
        req_memory: 100,
        assigned_workers: vec![],
        walltime: 0,
        start_time: None,
        priority: 0,
        user_id: "user".into(),
        working_directory: "/".into(),
        queued_time: 100,
        current_cpu_usage: 0.0,
        current_memory_usage: 0,
        is_idle: false,
        idle_duration: 0,
        end_time: None,
        reason: Some("Waiting for resources".to_string()),
        array_id: None,
        array_task_id: None,
        inputs: Vec::new(),
        cgroup_active: false,
        gres_req: BTreeMap::new(),
        allocated_cores: HashMap::new(),
        allocated_gres: HashMap::new(),
        env_vars: Vec::new(),
        mpi_stats: None,
        secret: "secret".into(),
        stdout_file_id: None,
        stderr_file_id: None,
        workdir_file_id: None,
        worker_log_files: Default::default(),
        output_artifacts: Vec::new(),
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
        interactive_port: None,
    };

    let encoded = bincode::serialize(&job).unwrap();
    let decoded: JobInfo = bincode::deserialize(&encoded).unwrap();

    assert_eq!(decoded.reason, Some("Waiting for resources".to_string()));
    assert_eq!(decoded.job_name, Some("test-job".to_string()));
}
