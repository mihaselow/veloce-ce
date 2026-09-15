use axum::{
    extract::{Path, Request, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio_util::io::ReaderStream;
use uuid::Uuid;

use crate::config::AppState;

#[derive(Serialize)]
#[serde(rename = "ListBucketResult", rename_all = "PascalCase")]
struct ListBucketResult {
    #[serde(rename = "@xmlns")]
    xmlns: String,
    name: String,
    prefix: String,
    key_count: usize,
    max_keys: usize,
    is_truncated: bool,
    #[serde(rename = "Contents")]
    contents: Vec<S3Object>,
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct S3Object {
    key: String,
    last_modified: String,
    size: u64,
}

#[derive(Deserialize)]
pub(crate) struct S3Query {
    #[serde(rename = "list-type")]
    list_type: Option<u8>,
    prefix: Option<String>,
    // Multipart specific
    uploads: Option<String>,
    #[serde(rename = "uploadId")]
    upload_id: Option<String>,
    #[serde(rename = "partNumber")]
    part_number: Option<usize>,
}

#[derive(Serialize)]
#[serde(rename = "InitiateMultipartUploadResult", rename_all = "PascalCase")]
struct InitiateMultipartUploadResult {
    #[serde(rename = "@xmlns")]
    xmlns: String,
    bucket: String,
    key: String,
    upload_id: String,
}

#[derive(Deserialize)]
#[serde(rename = "CompleteMultipartUpload", rename_all = "PascalCase")]
struct CompleteMultipartUpload {
    #[serde(rename = "Part")]
    parts: Vec<MultipartPart>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct MultipartPart {
    part_number: usize,
    e_tag: String,
}

#[derive(Serialize)]
#[serde(rename = "CompleteMultipartUploadResult", rename_all = "PascalCase")]
struct CompleteMultipartUploadResult {
    #[serde(rename = "@xmlns")]
    xmlns: String,
    location: String,
    bucket: String,
    key: String,
    e_tag: String,
}
fn get_s3_file_path(staging_dir: &std::path::Path, bucket: &str, key: &str) -> std::path::PathBuf {
    if bucket == "legacy" {
        staging_dir.join(key.trim_start_matches('/'))
    } else {
        staging_dir
            .join("s3")
            .join(bucket)
            .join(key.trim_start_matches('/'))
    }
}

fn get_s3_bucket_dir(staging_dir: &std::path::Path, bucket: &str) -> std::path::PathBuf {
    if bucket == "legacy" {
        staging_dir.to_path_buf()
    } else {
        staging_dir.join("s3").join(bucket)
    }
}

async fn load_meta_headers(file_path: &std::path::Path) -> axum::http::HeaderMap {
    let mut headers = axum::http::HeaderMap::new();
    let meta_path = format!("{}.meta.json", file_path.to_string_lossy());
    if let Ok(json) = tokio::fs::read_to_string(&meta_path).await {
        if let Ok(meta_map) =
            serde_json::from_str::<std::collections::HashMap<String, String>>(&json)
        {
            for (k, v) in meta_map {
                if let (Ok(hk), Ok(hv)) = (
                    axum::http::HeaderName::from_bytes(k.as_bytes()),
                    axum::http::HeaderValue::from_str(&v),
                ) {
                    headers.insert(hk, hv);
                }
            }
        }
    }
    headers
}

pub(crate) async fn s3_put_router(
    state: State<AppState>,
    path: Path<(String, String)>,
    query: axum::extract::Query<S3Query>,
    req: Request,
) -> Result<Response, (StatusCode, String)> {
    if state.s3_proxy_enabled {
        let (bucket, key) = path.0.clone();
        let query_str = req.uri().query().map(|s| s.to_string());
        let headers = req.headers().clone();
        use http_body_util::BodyExt;
        let body_bytes = req
            .into_body()
            .collect()
            .await
            .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?
            .to_bytes()
            .to_vec();
        return proxy_s3_request(
            &state,
            http::Method::PUT,
            &bucket,
            Some(&key),
            query_str.as_deref(),
            &headers,
            body_bytes,
        )
        .await;
    }
    if query.part_number.is_some() && query.upload_id.is_some() {
        s3_upload_part(state, path, query, req)
            .await
            .map(|r| r.into_response())
    } else {
        s3_put_object(state, path, req)
            .await
            .map(|r| r.into_response())
    }
}

pub(crate) async fn s3_put_object(
    state: State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    req: Request,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    if bucket == "legacy" {
        return Err((
            StatusCode::FORBIDDEN,
            "legacy bucket is read-only".to_string(),
        ));
    }
    let bucket_dir = get_s3_bucket_dir(&state.staging_dir, &bucket);
    if let Err(e) = tokio::fs::create_dir_all(&bucket_dir).await {
        return Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string()));
    }

    let file_path = get_s3_file_path(&state.staging_dir, &bucket, &key);
    if let Some(parent) = file_path.parent() {
        let _ = tokio::fs::create_dir_all(parent).await;
    }

    let mut meta_headers = std::collections::HashMap::new();
    for (k, v) in req.headers() {
        let key_str = k.as_str();
        if key_str.starts_with("x-amz-meta-") {
            if let Ok(val) = v.to_str() {
                meta_headers.insert(key_str.to_string(), val.to_string());
            }
        }
    }
    if !meta_headers.is_empty() {
        let meta_path = format!("{}.meta.json", file_path.to_string_lossy());
        if let Ok(json) = serde_json::to_string(&meta_headers) {
            let _ = tokio::fs::write(meta_path, json).await;
        }
    }

    let mut file = File::create(&file_path)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    use http_body_util::BodyExt;
    let mut body = req.into_body();
    while let Some(chunk) = body.frame().await {
        let chunk = chunk.map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
        if let Some(data) = chunk.data_ref() {
            file.write_all(data)
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        }
    }

    Ok(StatusCode::OK)
}

pub(crate) async fn s3_get_object(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    req: Request,
) -> Result<Response, (StatusCode, String)> {
    if state.s3_proxy_enabled {
        let query_str = req.uri().query().map(|s| s.to_string());
        let headers = req.headers().clone();
        return proxy_s3_request(
            &state,
            http::Method::GET,
            &bucket,
            Some(&key),
            query_str.as_deref(),
            &headers,
            Vec::new(),
        )
        .await;
    }
    let file_path = get_s3_file_path(&state.staging_dir, &bucket, &key);

    let file = File::open(&file_path)
        .await
        .map_err(|_| (StatusCode::NOT_FOUND, "File not found".to_string()))?;

    let headers_map = load_meta_headers(&file_path).await;

    let stream = ReaderStream::new(file);
    let body = axum::body::Body::from_stream(stream);

    let mut builder = Response::builder().status(StatusCode::OK);
    for (k, v) in headers_map.iter() {
        builder = builder.header(k, v);
    }
    Ok(builder.body(body).unwrap())
}

pub(crate) async fn s3_head_object(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    req: Request,
) -> Result<Response, (StatusCode, String)> {
    if state.s3_proxy_enabled {
        let query_str = req.uri().query().map(|s| s.to_string());
        let headers = req.headers().clone();
        return proxy_s3_request(
            &state,
            http::Method::HEAD,
            &bucket,
            Some(&key),
            query_str.as_deref(),
            &headers,
            Vec::new(),
        )
        .await;
    }
    let file_path = get_s3_file_path(&state.staging_dir, &bucket, &key);

    if let Ok(metadata) = tokio::fs::metadata(&file_path).await {
        let headers = load_meta_headers(&file_path).await;
        let mut builder = Response::builder().status(StatusCode::OK).header(
            axum::http::header::CONTENT_LENGTH,
            metadata.len().to_string(),
        );

        for (name, value) in headers.iter() {
            builder = builder.header(name, value);
        }

        let res = builder.body(axum::body::Body::empty()).unwrap();
        Ok(res)
    } else {
        Err((StatusCode::NOT_FOUND, "File not found".to_string()))
    }
}

pub(crate) async fn s3_delete_router(
    state: State<AppState>,
    path: Path<(String, String)>,
    query: axum::extract::Query<S3Query>,
    req: Request,
) -> Result<Response, (StatusCode, String)> {
    if state.s3_proxy_enabled {
        let (bucket, key) = path.0.clone();
        let query_str = req.uri().query().map(|s| s.to_string());
        let headers = req.headers().clone();
        return proxy_s3_request(
            &state,
            http::Method::DELETE,
            &bucket,
            Some(&key),
            query_str.as_deref(),
            &headers,
            Vec::new(),
        )
        .await;
    }
    if query.upload_id.is_some() {
        s3_abort_multipart(state, path, query)
            .await
            .map(|r| r.into_response())
    } else {
        s3_delete_object(state, path)
            .await
            .map(|r| r.into_response())
    }
}

pub(crate) async fn s3_delete_object(
    state: State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    if bucket == "legacy" {
        return Err((
            StatusCode::FORBIDDEN,
            "legacy bucket is read-only".to_string(),
        ));
    }
    let file_path = get_s3_file_path(&state.staging_dir, &bucket, &key);

    if file_path.exists() {
        let _ = tokio::fs::remove_file(format!("{}.meta.json", file_path.to_string_lossy())).await;
        tokio::fs::remove_file(&file_path)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err((StatusCode::NOT_FOUND, "File not found".to_string()))
    }
}

pub(crate) async fn s3_list_objects(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
    axum::extract::Query(query): axum::extract::Query<S3Query>,
    req: Request,
) -> Result<Response, (StatusCode, String)> {
    if state.s3_proxy_enabled {
        let query_str = req.uri().query().map(|s| s.to_string());
        let headers = req.headers().clone();
        return proxy_s3_request(
            &state,
            http::Method::GET,
            &bucket,
            None,
            query_str.as_deref(),
            &headers,
            Vec::new(),
        )
        .await;
    }
    if query.list_type != Some(2) {
        return Err((
            StatusCode::BAD_REQUEST,
            "Only list-type=2 is supported".to_string(),
        ));
    }

    let bucket_dir = get_s3_bucket_dir(&state.staging_dir, &bucket);
    if !bucket_dir.exists() {
        return Err((StatusCode::NOT_FOUND, "Bucket not found".to_string()));
    }

    let prefix = query.prefix.unwrap_or_default();

    let bucket_dir_clone = bucket_dir.clone();
    let prefix_clone = prefix.clone();

    let objects = tokio::task::spawn_blocking(move || {
        let mut results = Vec::new();
        for entry in walkdir::WalkDir::new(&bucket_dir_clone)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            if entry.file_type().is_file() {
                let path = entry.path();

                // Skip metadata sidecar files
                if path.extension().is_some_and(|ext| ext == "json")
                    && path
                        .file_name()
                        .unwrap()
                        .to_string_lossy()
                        .ends_with(".meta.json")
                {
                    continue;
                }

                if let Ok(rel_path) = path.strip_prefix(&bucket_dir_clone) {
                    let key = rel_path.to_string_lossy().to_string().replace('\\', "/");
                    if key.starts_with(&prefix_clone) {
                        if let Ok(metadata) = entry.metadata() {
                            results.push(S3Object {
                                key,
                                last_modified: "2023-01-01T00:00:00.000Z".to_string(),
                                size: metadata.len(),
                            });
                        }
                    }
                }
            }
        }
        results
    })
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let result = ListBucketResult {
        xmlns: "http://s3.amazonaws.com/doc/2006-03-01/".into(),
        name: bucket,
        prefix,
        key_count: objects.len(),
        max_keys: 1000,
        is_truncated: false,
        contents: objects,
    };

    let xml = quick_xml::se::to_string(&result)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let res = Response::builder()
        .status(StatusCode::OK)
        .header(axum::http::header::CONTENT_TYPE, "application/xml")
        .body(axum::body::Body::from(format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{}",
            xml
        )))
        .unwrap();
    Ok(res)
}

pub(crate) async fn s3_post_router(
    state: State<AppState>,
    path: Path<(String, String)>,
    query: axum::extract::Query<S3Query>,
    req: Request,
) -> Result<Response, (StatusCode, String)> {
    if state.s3_proxy_enabled {
        let (bucket, key) = path.0.clone();
        let query_str = req.uri().query().map(|s| s.to_string());
        let headers = req.headers().clone();
        use http_body_util::BodyExt;
        let body_bytes = req
            .into_body()
            .collect()
            .await
            .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?
            .to_bytes()
            .to_vec();
        return proxy_s3_request(
            &state,
            http::Method::POST,
            &bucket,
            Some(&key),
            query_str.as_deref(),
            &headers,
            body_bytes,
        )
        .await;
    }
    if query.uploads.is_some() {
        s3_initiate_multipart(state, path)
            .await
            .map(|r| r.into_response())
    } else if query.upload_id.is_some() {
        s3_complete_multipart(state, path, query, req)
            .await
            .map(|r| r.into_response())
    } else {
        Err((
            StatusCode::BAD_REQUEST,
            "Invalid POST operation".to_string(),
        ))
    }
}

pub(crate) async fn s3_initiate_multipart(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    if bucket == "legacy" {
        return Err((
            StatusCode::FORBIDDEN,
            "legacy bucket is read-only".to_string(),
        ));
    }
    let upload_id = Uuid::new_v4().to_string();
    let temp_dir = get_s3_bucket_dir(&state.staging_dir, &bucket)
        .join(".veloce_multipart")
        .join(&upload_id);

    if let Err(e) = tokio::fs::create_dir_all(&temp_dir).await {
        return Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string()));
    }

    let result = InitiateMultipartUploadResult {
        xmlns: "http://s3.amazonaws.com/doc/2006-03-01/".into(),
        bucket,
        key,
        upload_id,
    };

    let xml = quick_xml::se::to_string(&result)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok((
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "application/xml")],
        format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{}", xml),
    ))
}

pub(crate) async fn s3_upload_part(
    State(state): State<AppState>,
    Path((bucket, _key)): Path<(String, String)>,
    query: axum::extract::Query<S3Query>,
    req: Request,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    if bucket == "legacy" {
        return Err((
            StatusCode::FORBIDDEN,
            "legacy bucket is read-only".to_string(),
        ));
    }

    let upload_id = query.upload_id.as_ref().unwrap();
    let part_number = query.part_number.unwrap();
    let temp_dir = get_s3_bucket_dir(&state.staging_dir, &bucket)
        .join(".veloce_multipart")
        .join(upload_id);

    if !temp_dir.exists() {
        return Err((StatusCode::NOT_FOUND, "Upload ID not found".to_string()));
    }

    let part_path = temp_dir.join(format!("{}", part_number));
    let mut file = File::create(&part_path)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    use http_body_util::BodyExt;
    let mut body = req.into_body();
    let mut ctx = md5::Context::new();

    while let Some(chunk) = body.frame().await {
        let chunk = chunk.map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
        if let Some(data) = chunk.data_ref() {
            file.write_all(data)
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
            ctx.consume(data);
        }
    }

    let digest = ctx.finalize();
    let etag = format!("{:x}", digest);

    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        "ETag",
        axum::http::HeaderValue::from_str(&format!("\"{}\"", etag)).unwrap(),
    );

    Ok((StatusCode::OK, headers))
}

pub(crate) async fn s3_complete_multipart(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    query: axum::extract::Query<S3Query>,
    req: Request,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    if bucket == "legacy" {
        return Err((
            StatusCode::FORBIDDEN,
            "legacy bucket is read-only".to_string(),
        ));
    }

    let upload_id = query.upload_id.as_ref().unwrap();
    let temp_dir = get_s3_bucket_dir(&state.staging_dir, &bucket)
        .join(".veloce_multipart")
        .join(upload_id);

    if !temp_dir.exists() {
        return Err((StatusCode::NOT_FOUND, "Upload ID not found".to_string()));
    }

    use http_body_util::BodyExt;
    let body_bytes = req
        .into_body()
        .collect()
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?
        .to_bytes();

    let payload: CompleteMultipartUpload = quick_xml::de::from_reader(body_bytes.as_ref())
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("XML Parse Error: {}", e)))?;

    let file_path = get_s3_file_path(&state.staging_dir, &bucket, &key);
    if let Some(parent) = file_path.parent() {
        let _ = tokio::fs::create_dir_all(parent).await;
    }

    let mut final_file = File::create(&file_path)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Combine parts in order
    let mut combined_md5_ctx = md5::Context::new();
    for part in &payload.parts {
        let part_path = temp_dir.join(format!("{}", part.part_number));
        let mut part_file = tokio::fs::File::open(&part_path)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let mut part_md5_ctx = md5::Context::new();
        let mut buffer = [0u8; 8192];
        use tokio::io::AsyncReadExt;
        use tokio::io::AsyncWriteExt;

        loop {
            let bytes_read = part_file
                .read(&mut buffer)
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
            if bytes_read == 0 {
                break;
            }
            let chunk = &buffer[..bytes_read];
            part_md5_ctx.consume(chunk);
            combined_md5_ctx.consume(chunk);

            final_file
                .write_all(chunk)
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        }

        // Verify the client-provided ETag matches the data on disk
        let computed_etag = format!("{:x}", part_md5_ctx.finalize());
        if computed_etag != part.e_tag.trim_matches('"') {
            let _ = tokio::fs::remove_file(&file_path).await;
            return Err((
                StatusCode::BAD_REQUEST,
                format!("ETag mismatch for part {}", part.part_number),
            ));
        }
    }

    let _ = tokio::fs::remove_dir_all(&temp_dir).await;

    let final_md5 = format!("{:x}-{}", combined_md5_ctx.finalize(), payload.parts.len());

    let result = CompleteMultipartUploadResult {
        xmlns: "http://s3.amazonaws.com/doc/2006-03-01/".into(),
        location: format!("https://localhost:9001/s3/{}/{}", bucket, key), // mock
        bucket,
        key,
        e_tag: format!("\"{}\"", final_md5),
    };

    let xml = quick_xml::se::to_string(&result)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok((
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "application/xml")],
        format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{}", xml),
    ))
}

pub(crate) async fn s3_abort_multipart(
    State(state): State<AppState>,
    Path((bucket, _key)): Path<(String, String)>,
    query: axum::extract::Query<S3Query>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    if bucket == "legacy" {
        return Err((
            StatusCode::FORBIDDEN,
            "legacy bucket is read-only".to_string(),
        ));
    }

    let upload_id = query.upload_id.as_ref().unwrap();
    let temp_dir = get_s3_bucket_dir(&state.staging_dir, &bucket)
        .join(".veloce_multipart")
        .join(upload_id);

    let _ = tokio::fs::remove_dir_all(&temp_dir).await;

    Ok(StatusCode::NO_CONTENT)
}

async fn proxy_s3_request(
    state: &AppState,
    method: http::Method,
    bucket: &str,
    key: Option<&str>,
    query_string: Option<&str>,
    headers: &axum::http::HeaderMap,
    body_bytes: Vec<u8>,
) -> Result<Response, (StatusCode, String)> {
    use aws_credential_types::Credentials;
    use aws_sigv4::http_request::{sign, SignableBody, SignableRequest, SigningSettings};
    use aws_sigv4::sign::v4;
    use aws_smithy_runtime_api::client::identity::Identity;

    let mut s3_url = if let Some(k) = key {
        format!("{}/{}/{}", state.s3_endpoint, bucket, k)
    } else {
        format!("{}/{}", state.s3_endpoint, bucket)
    };
    if let Some(qs) = query_string {
        s3_url.push('?');
        s3_url.push_str(qs);
    }

    let credentials = Credentials::new(
        &state.s3_access_key,
        &state.s3_secret_key,
        None,
        None,
        "veloce-fileserver",
    );
    let identity = Identity::new(credentials, None);

    let signing_settings = SigningSettings::default();
    let signing_params = v4::SigningParams::builder()
        .identity(&identity)
        .region(&state.s3_region)
        .name("s3")
        .time(std::time::SystemTime::now())
        .settings(signing_settings)
        .build()
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to build signing params: {}", e),
            )
        })?
        .into();

    let parsed_url = reqwest::Url::parse(&s3_url).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Invalid S3 endpoint URL: {}", e),
        )
    })?;
    let host = parsed_url.host_str().unwrap_or("");

    let mut headers_vec = Vec::new();
    headers_vec.push(("host", host));

    for (k, v) in headers.iter() {
        let name = k.as_str();
        if name.eq_ignore_ascii_case("authorization")
            || name.eq_ignore_ascii_case("host")
            || name.eq_ignore_ascii_case("x-api-key")
            || name.starts_with("x-amz-signature")
        {
            continue;
        }
        if let Ok(val) = v.to_str() {
            headers_vec.push((name, val));
        }
    }

    let signable_req = SignableRequest::new(
        method.as_str(),
        &s3_url,
        headers_vec.iter().map(|(k, v)| (*k, *v)),
        SignableBody::Bytes(&body_bytes),
    )
    .map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to build signable request: {}", e),
        )
    })?;

    let (signing_instructions, _signature) = sign(signable_req, &signing_params)
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Signing failed: {}", e),
            )
        })?
        .into_parts();

    let mut req_builder = state.proxy_client.request(method, &s3_url);

    for (name, value) in signing_instructions.headers() {
        req_builder = req_builder.header(name, value);
    }

    let response = req_builder.body(body_bytes).send().await.map_err(|e| {
        (
            StatusCode::BAD_GATEWAY,
            format!("Failed to proxy to S3: {}", e),
        )
    })?;

    let status = StatusCode::from_u16(response.status().as_u16())
        .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let mut res_builder = Response::builder().status(status);

    for (k, v) in response.headers().iter() {
        res_builder = res_builder.header(k, v);
    }

    let resp_bytes = response.bytes().await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to read S3 response body: {}", e),
        )
    })?;

    let res = res_builder
        .body(axum::body::Body::from(resp_bytes))
        .unwrap();
    Ok(res)
}
#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use std::path::PathBuf;
    use tower::ServiceExt;

    use crate::auth::{s3_api_key_authorized, s3_legacy_auth_authorized, s3_request_authorized};

    struct TestEnv {
        pub app: axum::Router,
        pub temp_dir: PathBuf,
    }

    impl Drop for TestEnv {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.temp_dir);
        }
    }

    fn setup_test_app(allow_insecure: bool) -> TestEnv {
        let temp_dir = std::env::temp_dir().join(Uuid::new_v4().to_string());
        std::fs::create_dir_all(&temp_dir).unwrap();

        let state = AppState {
            staging_dir: temp_dir.clone(),
            api_key: "test_secret_key".to_string(),
            allow_insecure,
            s3_proxy_enabled: false,
            s3_endpoint: "".to_string(),
            s3_region: "".to_string(),
            s3_access_key: "".to_string(),
            s3_secret_key: "".to_string(),
            proxy_client: reqwest::Client::new(),
        };

        TestEnv {
            app: crate::build_app(state),
            temp_dir,
        }
    }

    #[tokio::test]
    async fn test_s3_put_object() {
        let env = setup_test_app(false);

        let put_req = Request::builder()
            .method("PUT")
            .uri("/s3/my-bucket/test-file.txt")
            .header("X-API-KEY", "test_secret_key")
            .body(axum::body::Body::from("hello s3"))
            .unwrap();

        let response = env.app.clone().oneshot(put_req).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_s3_get_object() {
        let env = setup_test_app(false);

        // Setup: Put file first
        let put_req = Request::builder()
            .method("PUT")
            .uri("/s3/my-bucket/test-file.txt")
            .header("X-API-KEY", "test_secret_key")
            .body(axum::body::Body::from("hello s3"))
            .unwrap();
        let _ = env.app.clone().oneshot(put_req).await.unwrap();

        // Download the file
        let get_req = Request::builder()
            .method("GET")
            .uri("/s3/my-bucket/test-file.txt")
            .header("X-API-KEY", "test_secret_key")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = env.app.clone().oneshot(get_req).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"hello s3");
    }

    #[tokio::test]
    async fn test_s3_head_object() {
        let env = setup_test_app(false);

        // Setup: Put file first
        let put_req = Request::builder()
            .method("PUT")
            .uri("/s3/my-bucket/test-file.txt")
            .header("X-API-KEY", "test_secret_key")
            .body(axum::body::Body::from("hello s3"))
            .unwrap();
        let _ = env.app.clone().oneshot(put_req).await.unwrap();

        // HEAD the file
        let head_req = Request::builder()
            .method("HEAD")
            .uri("/s3/my-bucket/test-file.txt")
            .header("X-API-KEY", "test_secret_key")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = env.app.clone().oneshot(head_req).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get("content-length").unwrap(), "8");
    }

    #[tokio::test]
    async fn test_s3_delete_object() {
        let env = setup_test_app(false);

        // Setup: Put file first
        let put_req = Request::builder()
            .method("PUT")
            .uri("/s3/my-bucket/test-file.txt")
            .header("X-API-KEY", "test_secret_key")
            .body(axum::body::Body::from("hello s3"))
            .unwrap();
        let _ = env.app.clone().oneshot(put_req).await.unwrap();

        // DELETE the file
        let delete_req = Request::builder()
            .method("DELETE")
            .uri("/s3/my-bucket/test-file.txt")
            .header("X-API-KEY", "test_secret_key")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = env.app.clone().oneshot(delete_req).await.unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        // Verify GET now returns 404
        let get_404_req = Request::builder()
            .method("GET")
            .uri("/s3/my-bucket/test-file.txt")
            .header("X-API-KEY", "test_secret_key")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = env.app.clone().oneshot(get_404_req).await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_s3_metadata() {
        let env = setup_test_app(false);

        // Put file with metadata
        let put_req = Request::builder()
            .method("PUT")
            .uri("/s3/my-bucket/test-meta.txt")
            .header("X-API-KEY", "test_secret_key")
            .header("x-amz-meta-solver", "openfoam")
            .body(axum::body::Body::from("hello metadata"))
            .unwrap();
        let response = env.app.clone().oneshot(put_req).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // HEAD file and check metadata
        let head_req = Request::builder()
            .method("HEAD")
            .uri("/s3/my-bucket/test-meta.txt")
            .header("X-API-KEY", "test_secret_key")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = env.app.clone().oneshot(head_req).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("x-amz-meta-solver").unwrap(),
            "openfoam"
        );
    }

    #[tokio::test]
    async fn test_s3_list_objects() {
        let env = setup_test_app(false);

        // Put files
        let put_req1 = Request::builder()
            .method("PUT")
            .uri("/s3/my-bucket/foo/bar.txt")
            .header("X-API-KEY", "test_secret_key")
            .body(axum::body::Body::from("1"))
            .unwrap();
        let _ = env.app.clone().oneshot(put_req1).await.unwrap();

        let put_req2 = Request::builder()
            .method("PUT")
            .uri("/s3/my-bucket/foo/baz.txt")
            .header("X-API-KEY", "test_secret_key")
            .body(axum::body::Body::from("2"))
            .unwrap();
        let _ = env.app.clone().oneshot(put_req2).await.unwrap();

        // List files with prefix
        let list_req = Request::builder()
            .method("GET")
            .uri("/s3/my-bucket?list-type=2&prefix=foo/")
            .header("X-API-KEY", "test_secret_key")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = env.app.clone().oneshot(list_req).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let xml = String::from_utf8_lossy(&body);
        assert!(xml.contains("<Key>foo/bar.txt</Key>"));
        assert!(xml.contains("<Key>foo/baz.txt</Key>"));
        assert!(xml.contains("<KeyCount>2</KeyCount>"));
    }

    #[tokio::test]
    async fn test_s3_presigned_url() {
        let env = setup_test_app(true);

        // Put file using header auth
        let put_req = Request::builder()
            .method("PUT")
            .uri("/s3/my-bucket/test.txt")
            .header(
                "Authorization",
                "AWS4-HMAC-SHA256 Credential=test_secret_key/20260101/us-east-1/s3/aws4_request",
            )
            .body(axum::body::Body::from("presigned"))
            .unwrap();
        let _ = env.app.clone().oneshot(put_req).await.unwrap();

        // Get file using query string auth
        let get_req = Request::builder()
            .method("GET")
            .uri("/s3/my-bucket/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=test_secret_key%2F20260101%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=dummy_signature")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = env.app.clone().oneshot(get_req).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"presigned");
    }

    #[tokio::test]
    async fn test_internal_bridge() {
        let env = setup_test_app(false);

        // Simulate a legacy file written directly to the staging dir root
        let uuid = "123e4567-e89b-12d3-a456-426614174000";
        let legacy_file_path = env.temp_dir.join(uuid);
        tokio::fs::write(&legacy_file_path, b"legacy data")
            .await
            .unwrap();

        // Access via the S3 legacy virtual bucket
        let get_req = Request::builder()
            .method("GET")
            .uri(format!("/s3/legacy/{}", uuid))
            .header("X-API-KEY", "test_secret_key")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = env.app.clone().oneshot(get_req).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"legacy data");
    }

    #[tokio::test]
    async fn test_s3_multipart() {
        let env = setup_test_app(false);

        // 1. Initiate Multipart Upload
        let init_req = Request::builder()
            .method("POST")
            .uri("/s3/my-bucket/huge.bin?uploads")
            .header("X-API-KEY", "test_secret_key")
            .body(axum::body::Body::empty())
            .unwrap();

        let init_resp = env.app.clone().oneshot(init_req).await.unwrap();
        assert_eq!(init_resp.status(), StatusCode::OK);

        let init_body = init_resp.into_body().collect().await.unwrap().to_bytes();
        let init_xml = String::from_utf8_lossy(&init_body);

        // Extract UploadId
        let start = init_xml.find("<UploadId>").unwrap() + 10;
        let end = init_xml.find("</UploadId>").unwrap();
        let upload_id = &init_xml[start..end];

        // 2. Upload Part 1
        let part_req = Request::builder()
            .method("PUT")
            .uri(format!(
                "/s3/my-bucket/huge.bin?partNumber=1&uploadId={}",
                upload_id
            ))
            .header("X-API-KEY", "test_secret_key")
            .body(axum::body::Body::from("part1-"))
            .unwrap();

        let part_resp = env.app.clone().oneshot(part_req).await.unwrap();
        assert_eq!(part_resp.status(), StatusCode::OK);
        let etag1 = part_resp
            .headers()
            .get("ETag")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        // 3. Upload Part 2
        let part_req2 = Request::builder()
            .method("PUT")
            .uri(format!(
                "/s3/my-bucket/huge.bin?partNumber=2&uploadId={}",
                upload_id
            ))
            .header("X-API-KEY", "test_secret_key")
            .body(axum::body::Body::from("part2"))
            .unwrap();

        let part_resp2 = env.app.clone().oneshot(part_req2).await.unwrap();
        assert_eq!(part_resp2.status(), StatusCode::OK);
        let etag2 = part_resp2
            .headers()
            .get("ETag")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        // 4. Complete
        let complete_xml = format!(
            r#"
            <CompleteMultipartUpload>
                <Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part>
                <Part><PartNumber>2</PartNumber><ETag>{}</ETag></Part>
            </CompleteMultipartUpload>
        "#,
            etag1, etag2
        );

        let comp_req = Request::builder()
            .method("POST")
            .uri(format!("/s3/my-bucket/huge.bin?uploadId={}", upload_id))
            .header("X-API-KEY", "test_secret_key")
            .body(axum::body::Body::from(complete_xml))
            .unwrap();

        let comp_resp = env.app.clone().oneshot(comp_req).await.unwrap();
        assert_eq!(comp_resp.status(), StatusCode::OK);

        // 5. Verify file contents via GET
        let get_req = Request::builder()
            .method("GET")
            .uri("/s3/my-bucket/huge.bin")
            .header("X-API-KEY", "test_secret_key")
            .body(axum::body::Body::empty())
            .unwrap();

        let get_resp = env.app.clone().oneshot(get_req).await.unwrap();
        assert_eq!(get_resp.status(), StatusCode::OK);

        let get_body = get_resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&get_body[..], b"part1-part2");
    }

    #[tokio::test]
    async fn test_s3_legacy_auth_rejected_hardened() {
        let env = setup_test_app(false);

        let put_req = Request::builder()
            .method("PUT")
            .uri("/s3/my-bucket/evil.txt")
            .header(
                "Authorization",
                "AWS4-HMAC-SHA256 Credential=test_secret_key/20260101/us-east-1/s3/aws4_request",
            )
            .body(axum::body::Body::from("should fail"))
            .unwrap();

        let response = env.app.clone().oneshot(put_req).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_s3_presigned_rejected_hardened() {
        let env = setup_test_app(false);

        let get_req = Request::builder()
            .method("GET")
            .uri("/s3/my-bucket/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=test_secret_key%2F20260101%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=dummy_signature")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = env.app.clone().oneshot(get_req).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn test_s3_auth_helpers() {
        let req = Request::builder()
            .uri("/s3/b/k")
            .header("X-API-KEY", "key123")
            .body(axum::body::Body::empty())
            .unwrap();
        assert!(s3_api_key_authorized(&req, "key123"));
        assert!(!s3_api_key_authorized(&req, "wrong"));

        let legacy = Request::builder()
            .uri("/s3/b/k")
            .header(
                "Authorization",
                "AWS4-HMAC-SHA256 Credential=key123/20260101/us-east-1/s3/aws4_request",
            )
            .body(axum::body::Body::empty())
            .unwrap();
        assert!(s3_legacy_auth_authorized(&legacy, "key123"));
        assert!(!s3_request_authorized(&legacy, "key123", false));
        assert!(s3_request_authorized(&legacy, "key123", true));
    }
}
