use crate::{DagNode, Message, SubmitDag};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Deserialize, Serialize)]
pub struct CwlWorkflow {
    pub name: Option<String>,
    pub steps: HashMap<String, CwlStep>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CwlStep {
    pub run: String,
    pub r#in: Option<HashMap<String, String>>,
    pub out: Option<Vec<String>>,
    // Veloce extensions
    pub req_nodes: Option<usize>,
    pub req_cores: Option<u32>,
    pub req_memory: Option<u64>,
    pub args: Option<Vec<String>>,
    pub image_uri: Option<String>,
}

pub fn parse_cwl_to_dag(
    content: &str,
    user_id: &str,
    working_directory: &str,
) -> anyhow::Result<SubmitDag> {
    let workflow: CwlWorkflow = serde_yaml::from_str(content)?;

    let mut nodes = Vec::new();

    // We need to resolve step dependencies based on "in" mapping to "out".
    // "in": {"input_1": "step1/output_1"} means this step depends on "step1".

    for (step_id, step) in &workflow.steps {
        let mut depends_on = Vec::new();
        if let Some(inputs) = &step.r#in {
            for source in inputs.values() {
                if let Some(idx) = source.find('/') {
                    let dep_step = &source[..idx];
                    if dep_step != step_id
                        && workflow.steps.contains_key(dep_step)
                        && !depends_on.contains(&dep_step.to_string())
                    {
                        depends_on.push(dep_step.to_string());
                    }
                }
            }
        }

        let args = step.args.clone().unwrap_or_default();

        let submit = Message::Submit {
            job_name: Some(step_id.clone()),
            job_comment: workflow.name.clone(),
            binary: step.run.clone(),
            args,
            req_nodes: step.req_nodes.unwrap_or(1),
            req_cores: step.req_cores.unwrap_or(1),
            req_memory: step.req_memory.unwrap_or(1024),
            walltime: 0,
            priority: 0,
            user_id: user_id.to_string(),
            working_directory: working_directory.to_string(),
            array_indices: None,
            inputs: vec![],
            gres_req: BTreeMap::new(),
            env_vars: vec![],
            wait_for_licenses: false,
            estimated_walltime: None,
            priority_offset: None,
            dependencies: None,
            dependency_specs: None,
            qos: Default::default(),
            image_uri: step.image_uri.clone(),
            vnc_enabled: false,
            inherit_host_env: false,
            env_allowlist: None,
            job_profile: None,
        };

        nodes.push(DagNode {
            node_id: step_id.clone(),
            task: Box::new(submit),
            depends_on,
        });
    }

    Ok(SubmitDag {
        name: workflow
            .name
            .clone()
            .unwrap_or_else(|| "cwl_workflow".to_string()),
        nodes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_cwl_to_dag() {
        let content = r#"
name: "Sample Multi-Stage Simulation"
steps:
  meshing:
    run: "ansys-meshing"
    req_cores: 4
    req_memory: 8192
    args: ["-batch", "mesh.jou"]
    out: ["mesh.msh"]
  
  solving:
    run: "ansys-fluent"
    req_cores: 32
    req_memory: 65536
    in:
      mesh: "meshing/mesh.msh"
    args: ["-t32", "-i", "solve.jou"]
    out: ["results.dat"]

  post_processing:
    run: "python3"
    req_cores: 1
    req_memory: 2048
    in:
      data: "solving/results.dat"
    args: ["plot_results.py"]
        "#;

        let dag = parse_cwl_to_dag(content, "tester", "/tmp/work").expect("Failed to parse CWL");
        assert_eq!(dag.name, "Sample Multi-Stage Simulation");
        assert_eq!(dag.nodes.len(), 3);

        let solving_node = dag.nodes.iter().find(|n| n.node_id == "solving").unwrap();
        assert_eq!(solving_node.depends_on, vec!["meshing".to_string()]);

        if let Message::Submit {
            binary,
            req_cores,
            req_memory,
            user_id,
            working_directory,
            ..
        } = &*solving_node.task
        {
            assert_eq!(binary, "ansys-fluent");
            assert_eq!(*req_cores, 32);
            assert_eq!(*req_memory, 65536);
            assert_eq!(user_id, "tester");
            assert_eq!(working_directory, "/tmp/work");
        } else {
            panic!("Expected Message::Submit");
        }

        let post_node = dag
            .nodes
            .iter()
            .find(|n| n.node_id == "post_processing")
            .unwrap();
        assert_eq!(post_node.depends_on, vec!["solving".to_string()]);
    }
}
