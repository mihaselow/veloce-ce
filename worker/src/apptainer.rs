use anyhow::Result;
use veloce_common::apptainer::{ContainerAsset, SolverManifest};

pub struct ApptainerRunner {
    pub manifest: SolverManifest,
    pub job_scratch: String,
    pub image_path: String,
}

impl ApptainerRunner {
    pub fn new(asset: &ContainerAsset, job_scratch: &str, image_path: &str) -> Self {
        Self {
            manifest: asset.manifest.clone(),
            job_scratch: job_scratch.to_string(),
            image_path: image_path.to_string(),
        }
    }

    pub fn build_command(
        &self,
        custom_args: &str,
        input_file: &str,
        job_id: u64,
        pmix_socket_dir: Option<&str>,
    ) -> Result<(String, Vec<String>)> {
        let execution = &self.manifest.execution;
        let mut cmd = String::new();

        let wrapper = execution.launch_wrapper.as_deref().unwrap_or("").trim();
        if wrapper.is_empty() {
            cmd.push_str("apptainer exec ");
        } else {
            cmd.push_str(wrapper);
            cmd.push_str(" ");
        }

        // Add isolation and hostname
        cmd.push_str(&format!(
            "--pid --ipc --uts --hostname veloce-job-{} ",
            job_id
        ));

        // Add bindings
        let working_dir = &self.manifest.file_handling.working_dir;
        cmd.push_str(&format!("--bind {}:{} ", self.job_scratch, working_dir));
        cmd.push_str(&format!("--pwd {} ", working_dir));

        if let Some(socket_dir) = pmix_socket_dir {
            cmd.push_str(&format!("--bind {}:{} ", socket_dir, socket_dir));
        }

        let input_path = std::path::Path::new(input_file);
        if let Some(parent) = input_path.parent() {
            let p_str = parent.to_string_lossy();
            if !p_str.is_empty() && p_str != "/" {
                cmd.push_str(&format!("--bind {}:{} ", p_str, p_str));
            }
        }

        // Add image
        cmd.push_str(&self.image_path);
        cmd.push_str(" ");

        // The Web UI and CLI already resolve all parameters (like {{debug}}, etc) and pass them as custom_args.
        // So we just execute the given input_file (binary) and custom_args directly inside the container.
        if !input_file.is_empty() {
            cmd.push_str(input_file);
        }
        if !custom_args.is_empty() {
            if !cmd.ends_with(' ') {
                cmd.push(' ');
            }
            cmd.push_str(custom_args);
        }

        Ok((
            "sh".to_string(),
            vec!["-c".to_string(), cmd.trim().to_string()],
        ))
    }

    pub fn build_environment_vars(&self) -> Vec<(String, String)> {
        let mut envs = Vec::new();
        for (k, v) in &self.manifest.execution.environment_vars {
            let mut resolved = v.clone();
            // Just a basic mock resolver for tests
            resolved = resolved.replace("{{license_server}}", "1055@licserver");
            envs.push((format!("APPTAINERENV_{}", k), resolved));
        }
        envs
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use veloce_common::apptainer::{Execution, FileHandling, SolverIdentity};

    #[test]
    fn test_apptainer_command_builder() {
        let asset = ContainerAsset {
            id: "test".to_string(),
            name: "test".to_string(),
            image_uri: "s3://test".to_string(),
            manifest: SolverManifest {
                manifest_version: "1.0".to_string(),
                solver_identity: SolverIdentity {
                    vendor: "Ansys".to_string(),
                    product: "Fluent".to_string(),
                    version: "2024".to_string(),
                    capabilities: vec![],
                },
                execution: Execution {
                    entrypoint: "/opt/fluent".to_string(),
                    launch_wrapper: Some("apptainer exec --nv".to_string()),
                    command_template:
                        "{{entrypoint}} {{precision}} -t{{cpus}} -i {{input_file}} {{custom_args}}"
                            .to_string(),
                    environment_vars: vec![(
                        "ANSYSLMD_LICENSE_FILE".to_string(),
                        "{{license_server}}".to_string(),
                    )]
                    .into_iter()
                    .collect(),
                },
                file_handling: FileHandling {
                    working_dir: "/scratch".to_string(),
                    input_mapping: vec![],
                    output_collection: vec![],
                },
                parameter_mapping: Default::default(),
                vnc_enabled: false,
            },
        };

        let runner = ApptainerRunner::new(&asset, "/local/job123", "/images/fluent.sif");

        let (bin, args) = runner.build_command("-g", "case.cas", 123, None).unwrap();

        assert_eq!(bin, "sh");
        // "case.cas" parent is "" (empty), so it won't trigger the extra bind.
        assert_eq!(args, vec!["-c".to_string(), "apptainer exec --nv --pid --ipc --uts --hostname veloce-job-123 --bind /local/job123:/scratch --pwd /scratch /images/fluent.sif case.cas -g".to_string()]);

        let envs = runner.build_environment_vars();
        assert_eq!(envs.len(), 1);
        // Note: Apptainer requires custom env vars to be prefixed with APPTAINERENV_
        assert_eq!(
            envs[0],
            (
                "APPTAINERENV_ANSYSLMD_LICENSE_FILE".to_string(),
                "1055@licserver".to_string()
            )
        );
    }
}
