//! API module: reservations.rs

use crate::SharedContext;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

#[derive(serde::Deserialize)]
pub struct CreateReservationRequest {
    pub nodes: std::collections::HashSet<String>,
    pub start_time: u64,
    pub end_time: u64,
    pub owner: String,
}

pub fn check_overlap(
    reservations: &std::collections::HashMap<String, veloce_common::Reservation>,
    nodes: &std::collections::HashSet<String>,
    start_time: u64,
    end_time: u64,
    exclude_id: Option<&str>,
) -> bool {
    for (id, res) in reservations {
        if let Some(ex_id) = exclude_id {
            if id == ex_id {
                continue;
            }
        }
        let overlap = start_time < res.end_time && res.start_time < end_time;
        if overlap {
            if nodes.iter().any(|node| res.nodes.contains(node)) {
                return true;
            }
        }
    }
    false
}

pub(super) async fn api_create_reservation(
    State(ctx): State<SharedContext>,
    Extension(principal): Extension<crate::auth::AuthenticatedPrincipal>,
    Json(mut payload): Json<CreateReservationRequest>,
) -> impl IntoResponse {
    if !principal
        .roles
        .iter()
        .any(|r| r == "admin" || r == "operator")
    {
        return (StatusCode::FORBIDDEN, "Forbidden: insufficient role").into_response();
    }
    let resolved_owner = match principal.resolve_user_id(&payload.owner) {
        Ok(uid) => uid,
        Err(e) => return (StatusCode::FORBIDDEN, e).into_response(),
    };
    payload.owner = resolved_owner;

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    if payload.start_time >= payload.end_time {
        return (
            StatusCode::BAD_REQUEST,
            "Start time must be before end time",
        )
            .into_response();
    }
    if payload.end_time <= now {
        return (StatusCode::BAD_REQUEST, "End time must be in the future").into_response();
    }
    if payload.nodes.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            "Reservation must include at least one node",
        )
            .into_response();
    }

    let mut state = ctx.state.lock().await;

    // Verify nodes exist
    for node in &payload.nodes {
        if !state.workers.contains_key(node) {
            return (
                StatusCode::BAD_REQUEST,
                format!("Node {} does not exist", node),
            )
                .into_response();
        }
    }

    // Check overlap
    if check_overlap(
        &state.reservations,
        &payload.nodes,
        payload.start_time,
        payload.end_time,
        None,
    ) {
        return (
            StatusCode::CONFLICT,
            "Requested nodes are already reserved during this time window",
        )
            .into_response();
    }

    let reservation_id = Uuid::new_v4().to_string();
    let res = veloce_common::Reservation {
        id: reservation_id.clone(),
        nodes: payload.nodes.clone(),
        start_time: payload.start_time,
        end_time: payload.end_time,
        owner: payload.owner.clone(),
    };

    state.reservations.insert(reservation_id.clone(), res);
    crate::save_state(&state);

    state.scheduler_notify.notify_one();

    (
        StatusCode::CREATED,
        Json(serde_json::json!({ "reservation_id": reservation_id })),
    )
        .into_response()
}

pub(super) async fn api_list_reservations(
    State(ctx): State<SharedContext>,
    Extension(principal): Extension<crate::auth::AuthenticatedPrincipal>,
) -> Response {
    if !principal
        .roles
        .iter()
        .any(|r| r == "admin" || r == "operator")
    {
        return (StatusCode::FORBIDDEN, "Forbidden: insufficient role").into_response();
    }
    let state = ctx.state.lock().await;
    let list: Vec<veloce_common::Reservation> = state.reservations.values().cloned().collect();
    Json(list).into_response()
}

pub(super) async fn api_delete_reservation(
    State(ctx): State<SharedContext>,
    Extension(principal): Extension<crate::auth::AuthenticatedPrincipal>,
    Path(id): Path<String>,
) -> Response {
    if !principal
        .roles
        .iter()
        .any(|r| r == "admin" || r == "operator")
    {
        return (StatusCode::FORBIDDEN, "Forbidden: insufficient role").into_response();
    }
    let mut state = ctx.state.lock().await;
    if state.reservations.remove(&id).is_some() {
        crate::save_state(&state);
        state.scheduler_notify.notify_one();
        StatusCode::OK.into_response()
    } else {
        StatusCode::NOT_FOUND.into_response()
    }
}
