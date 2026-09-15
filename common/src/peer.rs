use serde::{Deserialize, Serialize};

use crate::message::Message;
use crate::types::{JobInfo, JobStats, PersistedState, Resources};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum PeerMessage {
    Heartbeat {
        term: u64,
        leader_id: String,
    },
    StateSync(PersistedState),
    StateUpdate(StateDelta),
    RegisterWorker {
        worker_id: String,
        hostname: String,
        resources: Resources,
        features: Option<u64>,
        #[serde(default)]
        ip_address: Option<String>,
    },
    DeregisterWorker {
        worker_id: String,
    },
    ForwardToWorker {
        worker_id: String,
        msg: Box<Message>,
    },
    ForwardFromWorker {
        worker_id: String,
        msg: Box<Message>,
    },
    BatchWorkerHeartbeats {
        updates: Vec<(String, Resources, Vec<JobStats>)>,
    },
    Restart {
        delay_ms: u64,
        reason: String,
    },
    GetLogs {
        request_id: u64,
        msg: Box<Message>,
    },
    LogsResponse {
        request_id: u64,
        msg: Box<Message>,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StateDelta {
    pub jobs: Vec<JobInfo>,
    pub queue: Vec<u64>,
}
