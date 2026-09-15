#![allow(clippy::all)]
use anyhow::{Context, Result};
use flate2;
use std::fs;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Component, Path, PathBuf};
use std::time::UNIX_EPOCH;
use tar;
use veloce_common::JobOutputArtifact;

pub(crate) fn compress_working_directory(wd: &Path, output_path: &Path) -> Result<()> {
    let file = std::fs::File::create(output_path)?;
    let enc = flate2::write::GzEncoder::new(file, flate2::Compression::default());
    let mut tar = tar::Builder::new(enc);
    tar.append_dir_all(".", wd)?;
    tar.finish()?;
    Ok(())
}

fn output_pattern_matches(pattern: &str, rel_path: &str) -> bool {
    let pattern = pattern.trim_start_matches("./");
    let rel_path = rel_path.trim_start_matches("./");
    if pattern.ends_with('/') {
        let dir_pattern = pattern.trim_end_matches('/');
        return rel_path
            .split('/')
            .next()
            .map(|first| glob_component_matches(dir_pattern, first))
            .unwrap_or(false);
    }
    glob_matches(pattern.as_bytes(), rel_path.as_bytes())
}

fn glob_matches(pattern: &[u8], text: &[u8]) -> bool {
    if pattern.is_empty() {
        return text.is_empty();
    }
    match pattern[0] {
        b'*' => {
            glob_matches(&pattern[1..], text)
                || (!text.is_empty() && text[0] != b'/' && glob_matches(pattern, &text[1..]))
        }
        b'?' => !text.is_empty() && text[0] != b'/' && glob_matches(&pattern[1..], &text[1..]),
        b'[' => match_char_class(pattern, text)
            .map(|(pattern_rest, text_rest)| glob_matches(pattern_rest, text_rest))
            .unwrap_or(false),
        ch => !text.is_empty() && ch == text[0] && glob_matches(&pattern[1..], &text[1..]),
    }
}

fn match_char_class<'a>(pattern: &'a [u8], text: &'a [u8]) -> Option<(&'a [u8], &'a [u8])> {
    if text.is_empty() || text[0] == b'/' {
        return None;
    }
    let end = pattern.iter().position(|&c| c == b']')?;
    if end <= 1 {
        return None;
    }
    let class = &pattern[1..end];
    let mut matched = false;
    let mut i = 0;
    while i < class.len() {
        if i + 2 < class.len() && class[i + 1] == b'-' {
            if class[i] <= text[0] && text[0] <= class[i + 2] {
                matched = true;
            }
            i += 3;
        } else {
            if class[i] == text[0] {
                matched = true;
            }
            i += 1;
        }
    }
    if matched {
        Some((&pattern[end + 1..], &text[1..]))
    } else {
        None
    }
}

fn glob_component_matches(pattern: &str, text: &str) -> bool {
    glob_matches(pattern.as_bytes(), text.as_bytes())
}

fn relative_output_path(root: &Path, candidate: &Path) -> Option<String> {
    let rel = candidate.strip_prefix(root).ok()?;
    let parts: Vec<String> = rel
        .components()
        .filter_map(|component| match component {
            Component::Normal(part) => Some(part.to_string_lossy().to_string()),
            _ => None,
        })
        .collect();
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("/"))
    }
}

fn output_type_for_path(
    asset: &veloce_common::apptainer::ContainerAsset,
    rel_path: &str,
) -> Option<String> {
    asset
        .manifest
        .file_handling
        .output_collection
        .iter()
        .find(|output| output_pattern_matches(&output.path, rel_path))
        .map(|output| output.output_type.clone())
}

fn validate_output_request(
    root: &Path,
    asset: &veloce_common::apptainer::ContainerAsset,
    requested_path: &str,
) -> Result<(PathBuf, String, String)> {
    let rel = Path::new(requested_path);
    if rel.is_absolute() {
        anyhow::bail!("Output path must be relative to the job working directory");
    }
    if rel
        .components()
        .any(|c| !matches!(c, Component::Normal(_) | Component::CurDir))
    {
        anyhow::bail!("Output path may not contain parent, prefix, or root components");
    }
    let normalized = rel
        .components()
        .filter_map(|c| match c {
            Component::Normal(part) => Some(part.to_string_lossy().to_string()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("/");
    if normalized.is_empty() {
        anyhow::bail!("Output path is empty");
    }
    let output_type = output_type_for_path(asset, &normalized)
        .ok_or_else(|| anyhow::anyhow!("Output path is not declared in the container manifest"))?;
    let candidate = root.join(&normalized);
    if candidate.exists() {
        let root_canon = root.canonicalize().with_context(|| {
            format!(
                "Failed to canonicalize job working directory {}",
                root.display()
            )
        })?;
        let candidate_canon = candidate.canonicalize().with_context(|| {
            format!("Failed to canonicalize output file {}", candidate.display())
        })?;
        if !candidate_canon.starts_with(&root_canon) {
            anyhow::bail!("Output path resolves outside the job working directory");
        }
        Ok((candidate_canon, normalized, output_type))
    } else {
        Ok((candidate, normalized, output_type))
    }
}

pub(crate) fn collect_declared_output_artifacts(
    root: &Path,
    asset: Option<&veloce_common::apptainer::ContainerAsset>,
) -> Result<Vec<JobOutputArtifact>> {
    let Some(asset) = asset else {
        return Ok(Vec::new());
    };
    if asset.manifest.file_handling.output_collection.is_empty() || !root.exists() {
        return Ok(Vec::new());
    }
    let root_canon = root.canonicalize().with_context(|| {
        format!(
            "Failed to canonicalize job working directory {}",
            root.display()
        )
    })?;
    let mut artifacts = Vec::new();
    collect_declared_output_artifacts_inner(&root_canon, &root_canon, asset, &mut artifacts)?;
    artifacts.sort_by(|a, b| a.path.cmp(&b.path));
    artifacts.dedup_by(|a, b| a.path == b.path);
    Ok(artifacts)
}

fn collect_declared_output_artifacts_inner(
    root: &Path,
    dir: &Path,
    asset: &veloce_common::apptainer::ContainerAsset,
    artifacts: &mut Vec<JobOutputArtifact>,
) -> Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let meta = fs::symlink_metadata(&path)?;
        if meta.file_type().is_symlink() {
            continue;
        }
        if meta.is_dir() {
            collect_declared_output_artifacts_inner(root, &path, asset, artifacts)?;
            continue;
        }
        if !meta.is_file() {
            continue;
        }
        if let Some(rel_path) = relative_output_path(root, &path) {
            if let Some(output_type) = output_type_for_path(asset, &rel_path) {
                let updated_at = meta
                    .modified()
                    .ok()
                    .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                    .map(|d| d.as_secs());
                artifacts.push(JobOutputArtifact {
                    path: rel_path,
                    output_type,
                    file_id: None,
                    size: Some(meta.len()),
                    updated_at,
                });
            }
        }
    }
    Ok(())
}

pub(crate) fn read_declared_output_chunk(
    root: &Path,
    asset: Option<&veloce_common::apptainer::ContainerAsset>,
    requested_path: &str,
    offset: u64,
    length: Option<u64>,
) -> Result<(String, Vec<u8>, Option<u64>)> {
    let asset = asset
        .ok_or_else(|| anyhow::anyhow!("Job has no container manifest with declared outputs"))?;
    let (path, normalized, _) = validate_output_request(root, asset, requested_path)?;
    if !path.exists() {
        return Ok((normalized, Vec::new(), None));
    }
    let mut file = File::open(&path)?;
    let size = file.metadata().ok().map(|m| m.len());
    file.seek(SeekFrom::Start(offset))?;
    let mut buffer = Vec::new();
    if let Some(len) = length {
        let mut limited_reader = file.take(len);
        limited_reader.read_to_end(&mut buffer)?;
    } else {
        file.read_to_end(&mut buffer)?;
    }
    Ok((normalized, buffer, size))
}
