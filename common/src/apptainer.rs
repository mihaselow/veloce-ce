use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SolverIdentity {
    pub vendor: String,
    pub product: String,
    pub version: String,
    #[serde(default)]
    pub capabilities: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Execution {
    pub entrypoint: String,
    pub launch_wrapper: Option<String>,
    pub command_template: String,
    #[serde(default)]
    pub environment_vars: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct InputMapping {
    pub role: String,
    #[serde(rename = "match")]
    pub match_pattern: String,
    #[serde(default)]
    pub required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OutputCollection {
    pub path: String,
    #[serde(rename = "type")]
    pub output_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FileHandling {
    pub working_dir: String,
    #[serde(default)]
    pub input_mapping: Vec<InputMapping>,
    #[serde(default)]
    pub output_collection: Vec<OutputCollection>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct ParameterMapping {
    pub options: Option<Vec<String>>,
    pub default: Option<String>,
    pub flag: Option<String>,
    #[serde(rename = "type")]
    pub param_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SolverManifest {
    pub manifest_version: String,
    pub solver_identity: SolverIdentity,
    pub execution: Execution,
    pub file_handling: FileHandling,
    #[serde(default)]
    pub parameter_mapping: HashMap<String, ParameterMapping>,
    #[serde(default)]
    pub vnc_enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ContainerAsset {
    pub id: String,        // UUID
    pub name: String,      // Display name
    pub image_uri: String, // e.g. "s3://veloce-system-containers/ansys_fluent.sif"
    pub manifest: SolverManifest,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_solver_manifest_deserialization() {
        let json_data = r#"{
          "manifest_version": "1.0",
          "solver_identity": {
            "vendor": "Ansys",
            "product": "Fluent",
            "version": "2024 R1",
            "capabilities": ["CFD", "MPI", "GPU"]
          },
          "execution": {
            "entrypoint": "/opt/ansys_inc/v241/fluent/bin/fluent",
            "launch_wrapper": "apptainer exec --nv",
            "command_template": "{{entrypoint}} {{precision}} -g -t{{cpus}} -i {{input_file}} {{custom_args}}",
            "environment_vars": {
              "ANSYSLMD_LICENSE_FILE": "{{license_server}}",
              "OMPI_MCA_pml": "ucx"
            }
          },
          "file_handling": {
            "working_dir": "/scratch",
            "input_mapping": [
              { "role": "main_input", "match": "*.jou", "required": true },
              { "role": "case_file", "match": "*.cas", "required": true }
            ],
            "output_collection": [
              { "path": "*.dat", "type": "result" },
              { "path": "residuals.png", "type": "telemetry" }
            ]
          },
          "parameter_mapping": {
            "precision": { "options": ["2d", "3d", "2ddp", "3ddp"], "default": "3ddp" },
            "iterations": { "flag": "-iter", "type": "integer" }
          }
        }"#;

        let manifest: SolverManifest = serde_json::from_str(json_data).unwrap();
        assert_eq!(manifest.manifest_version, "1.0");
        assert_eq!(manifest.solver_identity.vendor, "Ansys");
        assert_eq!(
            manifest
                .execution
                .environment_vars
                .get("OMPI_MCA_pml")
                .unwrap(),
            "ucx"
        );
        assert_eq!(manifest.file_handling.input_mapping.len(), 2);

        let precision_mapping = manifest.parameter_mapping.get("precision").unwrap();
        assert_eq!(precision_mapping.options.as_ref().unwrap().len(), 4);
        assert_eq!(precision_mapping.default.as_deref().unwrap(), "3ddp");
    }
}
