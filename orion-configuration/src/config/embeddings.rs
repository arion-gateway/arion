use std::time::Duration;

use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EmbeddingsService {
    pub name: SmolStr,
    #[serde(flatten)]
    pub provider: EmbeddingsServiceProvider,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EmbeddingsServiceProvider {
    Local(LocalEmbeddingsService),
    Remote(RemoteEmbeddingsService),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LocalEmbeddingsService {
    pub model_id: SmolStr,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub model_dir: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub dimensions: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RemoteEmbeddingsService {
    pub cluster: SmolStr,
    pub model_id: SmolStr,
    pub path: String,
    #[serde(with = "humantime_serde", skip_serializing_if = "Option::is_none", default)]
    pub timeout: Option<Duration>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub dimensions: Option<usize>,
}
