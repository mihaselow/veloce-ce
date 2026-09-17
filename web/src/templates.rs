use veloce_common::apptainer::ContainerAsset;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct TemplateCard {
    pub(crate) name: String,
    pub(crate) versions: Vec<String>,
    pub(crate) first_version: String,
    pub(crate) availability: Option<String>,
}

pub(crate) fn template_group_name(container: &ContainerAsset) -> String {
    format!(
        "{} {}",
        container.manifest.solver_identity.vendor, container.manifest.solver_identity.product
    )
}

pub(crate) fn template_version(container: &ContainerAsset) -> String {
    container.manifest.solver_identity.version.clone()
}

pub(crate) fn template_cards(containers: Vec<ContainerAsset>) -> Vec<TemplateCard> {
    let mut grouped = std::collections::BTreeMap::<String, Vec<ContainerAsset>>::new();
    for template in containers {
        grouped
            .entry(template_group_name(&template))
            .or_default()
            .push(template);
    }

    grouped
        .into_iter()
        .filter_map(|(name, mut group)| {
            group.sort_by_key(template_version);
            let versions = group.iter().map(template_version).fold(
                Vec::<String>::new(),
                |mut versions, version| {
                    if !versions.contains(&version) {
                        versions.push(version);
                    }
                    versions
                },
            );
            let first_version = versions.first().cloned()?;
            Some(TemplateCard {
                name,
                versions,
                first_version,
                availability: None,
            })
        })
        .collect()
}
