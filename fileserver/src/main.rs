mod auth;
mod config;
mod s3;

use auth::{auth_middleware, s3_auth_middleware};
use axum::{
    extract::{DefaultBodyLimit, Multipart, Path, State},
    http::StatusCode,
    middleware,
    response::{IntoResponse, Json},
    routing::{delete, get, post},
    Router,
};
use config::{allow_insecure_from_env, AppState, Args, Config};
use serde::Serialize;
use std::net::SocketAddr;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio_util::io::ReaderStream;
use tower_http::cors::CorsLayer;
use tracing::{error, info, warn};
use uuid::Uuid;

#[derive(Serialize)]
struct UploadResponse {
    file_id: String,
}

#[tokio::main]
async fn main() {
    // Install default crypto provider for rustls
    let _ = rustls::crypto::ring::default_provider().install_default();

    // Setup persistent logging
    let file_appender = tracing_appender::rolling::never(".", "veloce-fileserver.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
    tracing_subscriber::fmt()
        .with_writer(non_blocking)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let args = <Args as clap::Parser>::parse();

    let config = Config::load(&args);

    if config.s3_proxy_enabled {
        info!(
            "S3 Proxy is enabled. Target endpoint: {}",
            config.s3_endpoint
        );
    }

    let allow_insecure = allow_insecure_from_env();
    if !allow_insecure {
        info!("S3-compat hardened: /s3/* requires X-API-KEY (legacy Authorization substring auth disabled)");
    } else {
        warn!("VELOCE_ALLOW_INSECURE=true — S3-compat accepts legacy Authorization substring auth (dev only)");
    }
    if config.s3_proxy_enabled && !allow_insecure {
        info!("S3 proxy upstream uses SigV4; clients must present X-API-KEY to the fileserver /s3/* endpoints");
    }

    let bind_address = config.bind_address.clone();
    let cert_path = config.cert_path.clone();
    let key_path = config.key_path.clone();
    let bind_addr: SocketAddr = bind_address.parse().expect("Invalid bind address");

    // Ensure the staging directory exists, creating it if necessary.
    if let Err(e) = std::fs::create_dir_all(&config.staging_dir) {
        error!(
            "Failed to create staging directory at {:?}: {}",
            &config.staging_dir, e
        );
        std::process::exit(1);
    }

    let state = config.into_app_state(allow_insecure);

    let app = build_app(state.clone());

    // run it on the configured address
    info!("fileserver listening on {} (HTTPS)", bind_address);
    let tls_config =
        match axum_server::tls_rustls::RustlsConfig::from_pem_file(&cert_path, &key_path).await {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to load TLS keys: {}", e);
                std::process::exit(1);
            }
        };

    // Spawn a background task to clean up old files in the staging directory
    let state_for_cleanup = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(3600)); // Every hour
        loop {
            interval.tick().await;
            info!("Cleaning up fileserver staging directory...");
            let mut entries = match tokio::fs::read_dir(&state_for_cleanup.staging_dir).await {
                Ok(entries) => entries,
                Err(e) => {
                    error!("Failed to read staging directory for cleanup: {}", e);
                    continue;
                }
            };

            while let Ok(Some(entry)) = entries.next_entry().await {
                let path = entry.path();
                if let Ok(metadata) = tokio::fs::metadata(&path).await {
                    if let Ok(modified) = metadata.modified() {
                        if let Ok(elapsed) = modified.elapsed() {
                            if elapsed.as_secs() > 7 * 24 * 3600 {
                                // 7 days (safety net)
                                info!("Cleaning up old file: {:?}", path);
                                let _ = tokio::fs::remove_file(path).await;
                            }
                        }
                    }
                }
            }
        }
    });

    axum_server::bind_rustls(bind_addr, tls_config)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

async fn root() -> &'static str {
    "Hello, World from fileserver!"
}

pub(crate) fn build_app(state: AppState) -> Router {
    let api_router = Router::new()
        .route("/files", post(upload_file))
        .route("/files/:id", get(download_file))
        .route("/files/:id", delete(delete_file))
        .route("/system/restart", post(restart_fileserver))
        .route("/system/logs", get(get_logs))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ));

    let s3_router = Router::new()
        .route("/:bucket", get(s3::s3_list_objects))
        .route("/:bucket/*key", axum::routing::put(s3::s3_put_router))
        .route("/:bucket/*key", post(s3::s3_post_router))
        .route("/:bucket/*key", get(s3::s3_get_object))
        .route("/:bucket/*key", axum::routing::head(s3::s3_head_object))
        .route("/:bucket/*key", delete(s3::s3_delete_router))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            s3_auth_middleware,
        ));

    Router::new()
        .route("/", get(root))
        .nest("/api/v1", api_router)
        .nest("/s3", s3_router)
        .layer(DefaultBodyLimit::disable())
        .layer(CorsLayer::permissive())
        .with_state(state)
}

async fn upload_file(
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    if let Some(mut field) = multipart
        .next_field()
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?
    {
        let file_id = Uuid::new_v4();
        let s3_dir = state.staging_dir.join("s3").join("veloce-staging");
        tokio::fs::create_dir_all(&s3_dir)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        let file_path = s3_dir.join(file_id.to_string());

        let mut file = File::create(&file_path)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        while let Some(chunk) = field
            .chunk()
            .await
            .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?
        {
            file.write_all(&chunk)
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        }

        return Ok(Json(UploadResponse {
            file_id: file_id.to_string(),
        }));
    }

    Err((StatusCode::BAD_REQUEST, "No file provided".to_string()))
}

async fn download_file(
    State(state): State<AppState>,
    Path(file_id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let uuid = Uuid::parse_str(&file_id)
        .map_err(|_| (StatusCode::BAD_REQUEST, "Invalid File ID".to_string()))?;

    let uuid_str = uuid.to_string();
    let s3_path = state
        .staging_dir
        .join("s3")
        .join("veloce-staging")
        .join(&uuid_str);
    let legacy_path = state.staging_dir.join(&uuid_str);

    let file_path = if tokio::fs::try_exists(&s3_path).await.unwrap_or(false) {
        s3_path
    } else {
        legacy_path
    };

    let file = File::open(&file_path)
        .await
        .map_err(|_| (StatusCode::NOT_FOUND, "File not found".to_string()))?;

    let stream = ReaderStream::new(file);
    let body = axum::body::Body::from_stream(stream);

    Ok(body)
}

async fn delete_file(
    State(state): State<AppState>,
    Path(file_id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let uuid = Uuid::parse_str(&file_id)
        .map_err(|_| (StatusCode::BAD_REQUEST, "Invalid File ID".to_string()))?;

    let uuid_str = uuid.to_string();
    let s3_path = state
        .staging_dir
        .join("s3")
        .join("veloce-staging")
        .join(&uuid_str);
    let legacy_path = state.staging_dir.join(&uuid_str);

    let file_path = if tokio::fs::try_exists(&s3_path).await.unwrap_or(false) {
        s3_path
    } else if tokio::fs::try_exists(&legacy_path).await.unwrap_or(false) {
        legacy_path
    } else {
        return Err((StatusCode::NOT_FOUND, "File not found".to_string()));
    };

    tokio::fs::remove_file(&file_path)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let meta_path = format!("{}.meta.json", file_path.to_string_lossy());
    if tokio::fs::try_exists(&meta_path).await.unwrap_or(false) {
        let _ = tokio::fs::remove_file(&meta_path).await;
    }

    Ok(StatusCode::NO_CONTENT)
}

async fn restart_fileserver(State(_state): State<AppState>) -> impl IntoResponse {
    info!("Fileserver restart requested via API.");
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
        self_restart();
    });
    StatusCode::OK
}

#[derive(serde::Deserialize)]
struct LogArgs {
    lines: Option<usize>,
}

async fn get_logs(
    State(_state): State<AppState>,
    axum::extract::Query(args): axum::extract::Query<LogArgs>,
) -> impl IntoResponse {
    let lines = args.lines.unwrap_or(100);
    match std::fs::read_to_string("veloce-fileserver.log") {
        Ok(content) => {
            let filtered = content
                .lines()
                .rev()
                .take(lines)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect::<Vec<_>>()
                .join("\n");
            (StatusCode::OK, filtered)
        }
        Err(_) => (StatusCode::NOT_FOUND, "Log file not found".to_string()),
    }
}

/// Initiates a self-restart via process replacement.
pub fn self_restart() -> ! {
    use std::os::unix::process::CommandExt;
    use std::process::Command;

    let args: Vec<String> = std::env::args().collect();
    let binary = &args[0];

    info!(
        "Initiating self-restart via process replacement: {}",
        binary
    );

    let err = Command::new(binary).args(&args[1..]).exec();

    error!("Failed to perform self-restart: {}", err);
    std::process::exit(1);
}
