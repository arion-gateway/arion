// Copyright 2025 The kmesh Authors
//
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
//

use orion_configuration::config::{bootstrap::Bootstrap, Listener as ListenerConfig};

use crate::{
    clusters::cluster::PartialClusterType, listeners::listener::ListenerFactory, ConversionContext, Error, Result,
    SecretManager,
};

pub fn get_listeners_and_clusters(
    bootstrap: Bootstrap,
) -> Result<(SecretManager, Vec<ListenerFactory>, Vec<PartialClusterType>)> {
    let (secret_manager, clusters) = get_secrets_and_clusters(&bootstrap)?;
    let listeners = build_listener_factories(bootstrap.static_resources.listeners, &secret_manager)?;
    Ok((secret_manager, listeners, clusters))
}

pub fn get_secrets_and_clusters(bootstrap: &Bootstrap) -> Result<(SecretManager, Vec<PartialClusterType>)> {
    let secrets = bootstrap.static_resources.secrets.clone();
    let mut secret_manager = SecretManager::new();
    secrets.into_iter().try_for_each(|secret| secret_manager.add(&secret).map(|_| ()))?;

    let clusters = bootstrap
        .static_resources
        .clusters
        .iter()
        .cloned()
        .map(|c| PartialClusterType::try_from((Box::new(c), &secret_manager)))
        .collect::<Result<Vec<_>>>()?;
    if clusters.is_empty() {
        //shouldn't happen with new config
        return Err::<(SecretManager, Vec<_>), Error>("No valid clusters configured".into());
    }
    Ok((secret_manager, clusters))
}

pub fn build_listener_factories(
    listeners: Vec<ListenerConfig>,
    secret_manager: &SecretManager,
) -> Result<Vec<ListenerFactory>> {
    listeners
        .into_iter()
        .map(|l| ListenerFactory::try_from(ConversionContext::new((l, secret_manager))))
        .collect::<Result<Vec<_>>>()
}
