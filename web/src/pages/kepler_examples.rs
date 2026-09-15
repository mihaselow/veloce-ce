use gloo_net::http::Request;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub(crate) struct KeplerExampleEntry {
    pub id: String,
    pub label: String,
    pub path: String,
    pub filename: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub(crate) struct KeplerExamplesIndex {
    pub examples: Vec<KeplerExampleEntry>,
}

pub(crate) async fn fetch_kepler_examples() -> Vec<KeplerExampleEntry> {
    match Request::get("/kepler/examples/index.json").send().await {
        Ok(resp) if resp.ok() => resp
            .json::<KeplerExamplesIndex>()
            .await
            .map(|index| index.examples)
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

pub(crate) async fn fetch_kepler_example_text(path: &str) -> Option<String> {
    match Request::get(path).send().await {
        Ok(resp) if resp.ok() => resp.text().await.ok(),
        _ => None,
    }
}

pub(crate) fn file_from_string(name: &str, contents: &str) -> web_sys::File {
    let bytes = js_sys::Uint8Array::from(contents.as_bytes());
    let parts = js_sys::Array::new();
    parts.push(&bytes);
    web_sys::File::new_with_u8_array_sequence(&parts, name).expect("create File from string")
}
