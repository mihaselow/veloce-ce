#![allow(clippy::all)]
use anyhow::Result;
use std::path::Path;

pub(crate) async fn process_inputs(
    file_client: &veloce_common::file_client::FileClient,
    inputs: &[veloce_common::FileHandle],
    working_dir: &Path,
) -> Result<()> {
    for input in inputs {
        let dest = working_dir.join(&input.original_name);
        file_client.download_file(&input.file_id, &dest).await?;

        if input.is_archive {
            let working_dir_owned = working_dir.to_path_buf();
            let dest_owned = dest.clone();
            tokio::task::spawn_blocking(move || -> Result<()> {
                let file = std::fs::File::open(&dest_owned)?;
                let tar = flate2::read::GzDecoder::new(file);
                let mut archive = tar::Archive::new(tar);
                archive.unpack(&working_dir_owned)?;
                std::fs::remove_file(&dest_owned)?;
                Ok(())
            })
            .await??;
        } else if input.is_executable {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let mut perms = std::fs::metadata(&dest)?.permissions();
                perms.set_mode(0o755);
                std::fs::set_permissions(&dest, perms)?;
            }
        }
    }
    Ok(())
}
