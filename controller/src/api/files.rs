//! API module: files.rs

use crate::SharedContext;
use axum::{
    extract::{Multipart, Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};

pub(super) async fn api_upload_file(
    State(ctx): State<SharedContext>,
    Extension(principal): Extension<crate::auth::AuthenticatedPrincipal>,
    mut multipart: Multipart,
) -> impl IntoResponse {
    if !principal
        .roles
        .iter()
        .any(|r| r == "admin" || r == "operator" || r == "submitter")
    {
        return (StatusCode::FORBIDDEN, "Forbidden: insufficient role").into_response();
    }

    let config = &ctx.config;
    if let (Some(url), Some(key)) = (&config.fileserver_url, &config.fileserver_api_key) {
        let mut form = reqwest::multipart::Form::new();

        while let Ok(Some(field)) = multipart.next_field().await {
            let name = field.name().unwrap_or("file").to_string();
            let file_name = field.file_name().map(|n| n.to_string());
            let content_type = field.content_type().map(|c| c.to_string());

            match field.bytes().await {
                Ok(bytes) => {
                    let mut part = reqwest::multipart::Part::bytes(bytes.to_vec());
                    if let Some(fn_str) = file_name {
                        part = part.file_name(fn_str);
                    }
                    if let Some(ct_str) = content_type {
                        part = part.mime_str(&ct_str).unwrap();
                    }
                    form = form.part(name, part);
                }
                Err(e) => {
                    return (
                        StatusCode::BAD_REQUEST,
                        format!("Failed to read multipart data: {}", e),
                    )
                        .into_response();
                }
            }
        }

        let client = &ctx.proxy_client;
        let upload_url = format!("{}/api/v1/files", url);

        match client
            .post(&upload_url)
            .header("X-API-KEY", key)
            .multipart(form)
            .send()
            .await
        {
            Ok(resp) => {
                if resp.status().is_success() {
                    match resp.json::<serde_json::Value>().await {
                        Ok(json) => (StatusCode::OK, Json(json)).into_response(),
                        Err(_) => (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "Invalid JSON from fileserver",
                        )
                            .into_response(),
                    }
                } else {
                    (
                        StatusCode::BAD_GATEWAY,
                        format!("Fileserver returned error: {}", resp.status()),
                    )
                        .into_response()
                }
            }
            Err(e) => (
                StatusCode::BAD_GATEWAY,
                format!("Failed to reach fileserver: {}", e),
            )
                .into_response(),
        }
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "Fileserver not configured").into_response()
    }
}

pub(super) async fn api_download_file(
    State(ctx): State<SharedContext>,
    Extension(principal): Extension<crate::auth::AuthenticatedPrincipal>,
    Path(file_id): Path<String>,
) -> impl IntoResponse {
    if !principal
        .roles
        .iter()
        .any(|r| r == "admin" || r == "operator" || r == "submitter" || r == "viewer")
    {
        return (StatusCode::FORBIDDEN, "Forbidden: insufficient role").into_response();
    }

    let config = &ctx.config;
    if let (Some(url), Some(key)) = (&config.fileserver_url, &config.fileserver_api_key) {
        let client = &ctx.proxy_client;
        let download_url = format!("{}/api/v1/files/{}", url, file_id);

        match client
            .get(&download_url)
            .header("X-API-KEY", key)
            .send()
            .await
        {
            Ok(resp) => {
                if resp.status().is_success() {
                    let content_type = resp
                        .headers()
                        .get("content-type")
                        .and_then(|val| val.to_str().ok())
                        .unwrap_or("application/octet-stream")
                        .to_string();
                    let stream = resp.bytes_stream();
                    let body = axum::body::Body::from_stream(stream);
                    Response::builder()
                        .header("content-type", content_type)
                        .body(body)
                        .unwrap()
                        .into_response()
                } else {
                    (
                        resp.status(),
                        format!("Fileserver returned error: {}", resp.status()),
                    )
                        .into_response()
                }
            }
            Err(e) => (
                StatusCode::BAD_GATEWAY,
                format!("Failed to reach fileserver: {}", e),
            )
                .into_response(),
        }
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "Fileserver not configured").into_response()
    }
}

pub(super) async fn api_delete_file(
    State(ctx): State<SharedContext>,
    Extension(principal): Extension<crate::auth::AuthenticatedPrincipal>,
    Path(file_id): Path<String>,
) -> impl IntoResponse {
    if !principal
        .roles
        .iter()
        .any(|r| r == "admin" || r == "operator" || r == "submitter")
    {
        return (StatusCode::FORBIDDEN, "Forbidden: insufficient role").into_response();
    }

    let config = &ctx.config;
    if let (Some(url), Some(key)) = (&config.fileserver_url, &config.fileserver_api_key) {
        let client = &ctx.proxy_client;
        let delete_url = format!("{}/api/v1/files/{}", url, file_id);

        match client
            .delete(&delete_url)
            .header("X-API-KEY", key)
            .send()
            .await
        {
            Ok(resp) => {
                if resp.status().is_success() {
                    StatusCode::NO_CONTENT.into_response()
                } else {
                    (
                        resp.status(),
                        format!("Fileserver returned error: {}", resp.status()),
                    )
                        .into_response()
                }
            }
            Err(e) => (
                StatusCode::BAD_GATEWAY,
                format!("Failed to reach fileserver: {}", e),
            )
                .into_response(),
        }
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "Fileserver not configured").into_response()
    }
}
