use orion_configuration::config::network_filters::http_connection_manager::http_filters::http_rbac::HttpRbac as HttpRbacConf;
use serde::{Deserialize, Serialize};
use triomphe::Arc;

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct HttpRbac {
    pub inner: Arc<HttpRbacConf>,
}

impl HttpRbac {
    pub fn new(conf: &HttpRbacConf) -> Self {
        Self { inner: Arc::new(conf.clone()) }
    }
}
