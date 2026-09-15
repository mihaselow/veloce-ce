use bytes::BytesMut;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::io;
use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};

use crate::apptainer;
use crate::peer::PeerMessage;
use crate::types::{
    EfficiencyStat, FederationClusterCapacity, FederationClusterStatus, FederationJobInfo,
    FederationLogRequest, FederationOutputContentRequest, FederationSubmitJobRequest,
    FederationSubmitStepRequest, FederationTemplateInfo, FileHandle, HistoryFilter, JobEvent,
    JobInfo, JobOutputArtifact, JobStateFilter, JobStats, JobUsage, LogType, NodeMetrics, QosLevel,
    Reservation, Resources, StepInfo, SubmitDag, WorkerInfo,
};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum Message {
    // --- Handshake ---
    HelloWorker {
        worker_id: String,
        hostname: String,
        resources: Resources,
        #[serde(default)]
        features: Option<u64>,
        #[serde(default)]
        registration_token: Option<String>,
    },
    HelloClient {
        client_id: String,
        #[serde(default)]
        registration_token: Option<String>,
    },
    HelloPeer {
        controller_id: String,
        #[serde(default)]
        features: Option<u64>,
        #[serde(default)]
        registration_token: Option<String>,
    },
    HelloFederationPeer {
        cluster_id: String,
        #[serde(default)]
        token: Option<String>,
    },

    // --- Worker -> Controller ---
    Heartbeat {
        resources: Resources,
        job_stats: Vec<JobStats>,
    },
    WorkerDraining {
        worker_id: String,
    },
    JobStarted {
        job_id: u64,
    },
    JobInteractivePort {
        job_id: u64,
        port: u16,
    },
    JobOutput {
        job_id: u64,
        data: Vec<u8>,
    },
    JobDone {
        job_id: u64,
        exit_code: i32,
    },
    JobError {
        job_id: u64,
        error: String,
    },
    JobKilled {
        job_id: u64,
    },
    LogData {
        request_id: u64,
        job_id: u64,
        content: Vec<u8>,
    },
    JobOutputFiles {
        request_id: u64,
        job_id: u64,
        artifacts: Vec<JobOutputArtifact>,
    },
    JobOutputFileChunk {
        request_id: u64,
        job_id: u64,
        path: String,
        content: Vec<u8>,
        size: Option<u64>,
    },
    ReportJobUsage(JobUsage),
    ReportMetrics(NodeMetrics),
    Pulse(NodeMetrics),
    LogStream {
        labels: HashMap<String, String>,
        line: String,
        timestamp: u64,
    },
    StepStarted {
        parent_job_id: u64,
        step_id: u32,
    },
    StepDone {
        parent_job_id: u64,
        step_id: u32,
        exit_code: i32,
    },
    StepError {
        parent_job_id: u64,
        step_id: u32,
        error: String,
    },
    StepOutput {
        parent_job_id: u64,
        step_id: u32,
        is_stderr: bool,
        data: Vec<u8>,
    },
    StepPMIPut {
        parent_job_id: u64,
        step_id: u32,
        rank: u32,
        key: String,
        value: String,
    },
    StepPMIGet {
        parent_job_id: u64,
        step_id: u32,
        rank: u32,
        key: String,
        request_id: u64,
    },
    StepPMIBarrierEnter {
        parent_job_id: u64,
        step_id: u32,
        rank: u32,
    },

    // --- Controller -> Worker ---
    RunJob {
        job_id: u64,
        binary: String,
        args: Vec<String>,
        node_list: Vec<String>,
        assigned_cores: Option<Vec<usize>>,
        req_memory: u64,
        working_directory: String,
        user_id: String,
        walltime: u64,
        submission_time: u64,
        array_id: Option<u64>,
        array_task_id: Option<u32>,
        inputs: Vec<FileHandle>,
        allocated_gres: HashMap<String, Vec<u32>>,
        gres_req: BTreeMap<String, u64>,
        secret: String,
        controller_url: String,
        env_vars: Vec<(String, String)>,
        #[serde(default)]
        wait_for_licenses: bool,
        #[serde(default)]
        estimated_walltime: Option<u64>,
        #[serde(default)]
        priority_offset: Option<i32>,
        #[serde(default)]
        dependencies: Option<Vec<u64>>,
        #[serde(default)]
        dependency_specs: Option<Vec<String>>,
        #[serde(default)]
        qos: QosLevel,
        #[serde(default)]
        container_asset: Option<apptainer::ContainerAsset>,
        #[serde(default)]
        vnc_enabled: bool,
        #[serde(default)]
        inherit_host_env: bool,
        #[serde(default)]
        env_allowlist: Option<Vec<String>>,
        #[serde(default)]
        job_profile: Option<String>,
    },
    TerminateJob {
        job_id: u64,
    },
    PreemptJob {
        job_id: u64,
    },
    GetLogs {
        request_id: u64,
        job_id: u64,
        log_type: LogType,
        offset: u64,
        length: Option<u64>,
        working_directory: Option<String>,
        #[serde(default)]
        rank: Option<usize>,
    },
    ListJobOutputFiles {
        request_id: u64,
        job_id: u64,
        working_directory: Option<String>,
        #[serde(default)]
        container_asset: Option<apptainer::ContainerAsset>,
    },
    GetJobOutputFileChunk {
        request_id: u64,
        job_id: u64,
        path: String,
        offset: u64,
        length: Option<u64>,
        working_directory: Option<String>,
        #[serde(default)]
        container_asset: Option<apptainer::ContainerAsset>,
    },
    RunStep {
        parent_job_id: u64,
        step_id: u32,
        binary: String,
        args: Vec<String>,
        node_list: Vec<String>,
        assigned_cores: Option<Vec<usize>>,
        working_directory: String,
        user_id: String,
        assigned_ranks: Vec<u32>,
        total_ranks: u32,
        inputs: Vec<FileHandle>,
        allocated_gres: HashMap<String, Vec<u32>>,
        gres_req: BTreeMap<String, u64>,
        secret: String,
        controller_url: String,
        env_vars: Vec<(String, String)>,
    },
    StepPMIGetResponse {
        parent_job_id: u64,
        step_id: u32,
        rank: u32,
        request_id: u64,
        key: String,
        value: Option<String>,
    },
    StepPMIBarrierRelease {
        parent_job_id: u64,
        step_id: u32,
    },
    SetPulseInterval {
        seconds: f32,
    },
    PeerAction(PeerMessage),

    // --- HA Messages ---
    LeaderRedirect {
        leader_addr: String,
    },

    // --- Client -> Controller ---
    SubmitDag(SubmitDag),
    Submit {
        #[serde(default)]
        job_name: Option<String>,
        #[serde(default)]
        job_comment: Option<String>,
        binary: String,
        args: Vec<String>,
        req_nodes: usize,
        req_cores: u32,
        req_memory: u64,
        walltime: u64,
        priority: u32,
        user_id: String,
        working_directory: String,
        array_indices: Option<Vec<u32>>,
        inputs: Vec<FileHandle>,
        gres_req: BTreeMap<String, u64>,
        env_vars: Vec<(String, String)>,
        #[serde(default)]
        wait_for_licenses: bool,
        #[serde(default)]
        estimated_walltime: Option<u64>,
        #[serde(default)]
        priority_offset: Option<i32>,
        #[serde(default)]
        dependencies: Option<Vec<u64>>,
        #[serde(default)]
        dependency_specs: Option<Vec<String>>,
        #[serde(default)]
        qos: QosLevel,
        #[serde(default)]
        image_uri: Option<String>,
        #[serde(default)]
        vnc_enabled: bool,
        #[serde(default)]
        inherit_host_env: bool,
        #[serde(default)]
        env_allowlist: Option<Vec<String>>,
        #[serde(default)]
        job_profile: Option<String>,
    },
    ListWorkers,
    ListJobs {
        state_filter: Option<JobStateFilter>,
    },
    CancelJob {
        job_id: u64,
    },
    Restart {
        component: String,
        target_id: Option<String>,
        delay_ms: u64,
        reason: String,
    },
    GetJobHistory {
        filter: HistoryFilter,
    },
    GetMetrics {
        start_time: Option<u64>,
        end_time: Option<u64>,
        nodes: Option<Vec<String>>,
        aggregate: bool,
    },
    // Client reuses GetLogs message
    SubmitStep {
        parent_job_id: u64,
        binary: String,
        args: Vec<String>,
        req_nodes: usize,
        req_cores: u32,
        ntasks: u32,
        inputs: Vec<FileHandle>,
        gres_req: BTreeMap<String, u64>,
        env_vars: Vec<(String, String)>,
    },
    GetComponentLogs {
        request_id: u64,
        component_id: String,
        lines: usize,
    },
    GetSystemLogs {
        request_id: u64,
        component_id: String,
        log_source: String, // "dmesg", "syslog", "journal"
        lines: usize,
    },
    GetEfficiencyStats {
        solver_name: Option<String>,
        binary_name: Option<String>,
    },
    GetJobEvents {
        job_id: u64,
    },
    GetJobSteps {
        job_id: u64,
    },
    StageProject {
        source_url: String,
        target_path: String,
    },

    // --- Response Variants ---
    EfficiencyStats(Vec<EfficiencyStat>),
    JobEvents(Vec<JobEvent>),
    StagingStarted {
        task_id: String,
    },
    ComponentLogs {
        request_id: u64,
        component_id: String,
        hostname: String,
        content: String,
    },
    SystemLogs {
        request_id: u64,
        component_id: String,
        hostname: String,
        content: String,
    },

    // --- Original Logic Continue ---
    JobSummary(Box<JobInfo>),

    // --- Controller -> Client ---
    JobId {
        job_id: u64,
    },
    ArrayJobSubmitted {
        base_job_id: u64,
        task_count: usize,
    },
    DagSubmitted {
        base_job_id: u64,
        task_count: usize,
    },
    WorkerList(Vec<WorkerInfo>),
    JobList(Vec<JobInfo>),
    FederationGetCluster,
    FederationCluster(FederationClusterStatus),
    FederationGetCapacity,
    FederationCapacity(FederationClusterCapacity),
    FederationListJobs,
    FederationJobList(Vec<FederationJobInfo>),
    FederationGetJob {
        job_id: u64,
    },
    FederationJob(Option<FederationJobInfo>),
    FederationListTemplates,
    FederationTemplateList(Vec<FederationTemplateInfo>),
    FederationSubmitJob(FederationSubmitJobRequest),
    FederationJobSubmitted {
        job_id: u64,
    },
    FederationCancelJob {
        job_id: u64,
    },
    FederationJobCanceled,
    FederationGetLogs(FederationLogRequest),
    FederationLogData {
        content: String,
    },
    FederationListOutputs {
        job_id: u64,
    },
    FederationOutputList(Vec<JobOutputArtifact>),
    FederationGetOutputContent(FederationOutputContentRequest),
    FederationOutputContent {
        content: Vec<u8>,
    },
    FederationListSteps {
        job_id: u64,
    },
    FederationStepList(Vec<StepInfo>),
    FederationSubmitStep {
        job_id: u64,
        step: FederationSubmitStepRequest,
    },
    FederationStepSubmitted {
        step_id: u32,
    },
    FederationError {
        message: String,
    },
    JobHistoryResponse(Vec<JobUsage>),
    MetricsData(Vec<NodeMetrics>),
    JobSteps(Vec<StepInfo>),
    StepId {
        step_id: u32,
    },
    CreateReservation {
        nodes: std::collections::HashSet<String>,
        start_time: u64,
        end_time: u64,
        owner: String,
    },
    ListReservations,
    DeleteReservation {
        id: String,
    },
    ReservationCreated {
        id: String,
    },
    ReservationList(Vec<Reservation>),
    StartTerminalSession {
        job_id: u64,
        session_id: u64,
    },
    StopTerminalSession {
        session_id: u64,
    },
    TerminalInput {
        session_id: u64,
        data: Vec<u8>,
    },
    TerminalOutput {
        session_id: u64,
        data: Vec<u8>,
    },
    TerminalResize {
        session_id: u64,
        rows: u16,
        cols: u16,
    },
    TerminalClosed {
        session_id: u64,
        reason: String,
    },
    StartVncSession {
        job_id: u64,
        session_id: u64,
    },
    StopVncSession {
        session_id: u64,
    },
    VncInput {
        session_id: u64,
        data: Vec<u8>,
    },
    VncOutput {
        session_id: u64,
        data: Vec<u8>,
    },
    VncClosed {
        session_id: u64,
        reason: String,
    },
    Ack,
    Error(String),
}

pub struct MessageCodec {
    frame_codec: LengthDelimitedCodec,
}

impl Default for MessageCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl MessageCodec {
    pub fn new() -> Self {
        Self {
            frame_codec: LengthDelimitedCodec::new(),
        }
    }
}

impl Encoder<Message> for MessageCodec {
    type Error = io::Error;

    fn encode(&mut self, item: Message, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let data =
            bincode::serialize(&item).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let data_buf = BytesMut::from(data.as_slice());
        self.frame_codec.encode(data_buf.into(), dst)
    }
}

impl Decoder for MessageCodec {
    type Item = Message;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match self.frame_codec.decode(src)? {
            Some(frame) => {
                if frame.len() == 4 && frame[..] == [1, 0, 0, 0] {
                    return Ok(Some(Message::HelloClient {
                        client_id: "legacy_client".to_string(),
                        registration_token: None,
                    }));
                }
                let message: Message = bincode::deserialize(&frame)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                Ok(Some(message))
            }
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_codec_roundtrip() {
        let mut codec = MessageCodec::new();
        let mut buffer = BytesMut::new();

        let original_msg = Message::HelloClient {
            client_id: "test-client".to_string(),
            registration_token: Some("test-token".to_string()),
        };

        // Encode
        codec.encode(original_msg.clone(), &mut buffer).unwrap();

        // Decode
        let decoded_msg = codec.decode(&mut buffer).unwrap().unwrap();

        // Verify
        match (original_msg, decoded_msg) {
            (
                Message::HelloClient {
                    client_id: id1,
                    registration_token: tok1,
                },
                Message::HelloClient {
                    client_id: id2,
                    registration_token: tok2,
                },
            ) => {
                assert_eq!(id1, id2);
                assert_eq!(tok1, tok2);
            }
            _ => panic!("Messages do not match"),
        }
    }

    #[test]
    fn test_legacy_hello_client_decoding() {
        let mut codec = MessageCodec::new();
        let mut buf = BytesMut::from(&[0, 0, 0, 4, 1, 0, 0, 0][..]);
        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        match decoded {
            Message::HelloClient {
                client_id,
                registration_token,
            } => {
                assert_eq!(client_id, "legacy_client");
                assert!(registration_token.is_none());
            }
            _ => panic!("Expected HelloClient"),
        }
    }

    #[test]
    fn test_resources_serialization() {
        use crate::types::Resources;

        let res = Resources {
            cpu_cores: 4,
            total_memory: 1024,
            free_memory: 512,
            cpu_usage: 10.5,
            cpu_model: "TestCPU".to_string(),
            arch: "x86_64".to_string(),
            os_name: "TestOS".to_string(),
            os_version: "1.0".to_string(),
            kernel_version: "5.15".to_string(),
            host_name: "test-host".to_string(),
            load_avg: [0.1, 0.2, 0.3],
            disk_total: 1000,
            disk_free: 500,
            uptime: 0,
            boot_time: 0,
            process_count: 0,
            swap_total: 0,
            swap_free: 0,
            version: "v1".into(),
            gres: BTreeMap::new(),
            cgroup_enabled: false,
        };

        let encoded = bincode::serialize(&res).unwrap();
        let decoded: Resources = bincode::deserialize(&encoded).unwrap();

        assert_eq!(res.cpu_cores, decoded.cpu_cores);
        assert_eq!(res.total_memory, decoded.total_memory);
        assert_eq!(res.cpu_model, decoded.cpu_model);
        assert_eq!(res.os_name, decoded.os_name);
    }

    #[test]
    fn test_job_status_serialization() {
        use crate::types::JobStatus;

        let statuses = vec![
            JobStatus::Pending,
            JobStatus::Running,
            JobStatus::Completed(0),
            JobStatus::Completed(-1),
            JobStatus::Failed("Error".to_string()),
            JobStatus::Killed,
        ];

        for status in statuses {
            let encoded = bincode::serialize(&status).unwrap();
            let decoded: JobStatus = bincode::deserialize(&encoded).unwrap();
            assert_eq!(status, decoded);
        }
    }

    #[test]
    fn test_complex_message_serialization() {
        use crate::types::QosLevel;

        let msg = Message::RunJob {
            job_id: 12345,
            binary: "/bin/bash".into(),
            args: vec!["-c".into(), "echo hello".into()],
            node_list: vec!["10.0.0.1".into(), "10.0.0.2".into()],
            assigned_cores: Some(vec![0, 1, 2, 3]),
            req_memory: 2048,
            working_directory: "/home/user".into(),
            user_id: "user1".into(),
            walltime: 3600,
            submission_time: 100000,
            array_id: None,
            array_task_id: None,
            inputs: Vec::new(),
            allocated_gres: HashMap::new(),
            gres_req: BTreeMap::new(),
            secret: "test_secret".into(),
            controller_url: "https://localhost:8080".into(),
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
        };

        let encoded = bincode::serialize(&msg).unwrap();
        let decoded: Message = bincode::deserialize(&encoded).unwrap();

        match decoded {
            Message::RunJob {
                job_id,
                binary,
                assigned_cores,
                ..
            } => {
                assert_eq!(job_id, 12345);
                assert_eq!(binary, "/bin/bash");
                assert_eq!(assigned_cores, Some(vec![0, 1, 2, 3]));
            }
            _ => panic!("Wrong message variant"),
        }
    }
}
