use orion_configuration::config::network_filters::http_connection_manager::http_filters::ext_proc::HeaderMutationRules;
use orion_data_plane_api::envoy_data_plane_api::envoy::{
    config::core::v3::header_value_option::HeaderAppendAction, service::ext_proc::v3::HeaderMutation,
};

use crate::Error;

pub fn apply_header_mutations(
    headers: &mut http::HeaderMap,
    mutation: &HeaderMutation,
    mutation_rules: Option<&HeaderMutationRules>,
) -> Result<(), Error> {
    for header_to_remove in &mutation.remove_headers {
        if let Some(rules) = mutation_rules {
            if !rules.is_modification_permitted(header_to_remove) {
                if rules.disallow_is_error {
                    return Err(Error::from(format!(
                        "Header removal not permitted by configuration: {header_to_remove}"
                    )));
                }
                continue;
            }
        }
        if let Ok(header_name) = http::HeaderName::from_bytes(header_to_remove.as_bytes()) {
            headers.remove(&header_name);
        }
    }
    for header_to_set in &mutation.set_headers {
        let Some(header) = &header_to_set.header else { continue };
        if let Some(rules) = mutation_rules {
            if !rules.is_modification_permitted(&header.key) {
                if rules.disallow_is_error {
                    return Err(Error::from(format!(
                        "Header modification not permitted by configuration: {}",
                        header.key
                    )));
                }
                continue;
            }
        }
        let Ok(header_name) = http::HeaderName::from_bytes(header.key.as_bytes()) else { continue };
        let header_value = if header.raw_value.is_empty() {
            http::HeaderValue::from_str(&header.value)
        } else {
            http::HeaderValue::from_bytes(&header.raw_value)
        };
        let Ok(header_value) = header_value else { continue };
        match header_to_set.append_action() {
            HeaderAppendAction::AppendIfExistsOrAdd => {
                headers.append(header_name, header_value);
            },
            HeaderAppendAction::AddIfAbsent => {
                if !headers.contains_key(&header_name) {
                    headers.append(header_name, header_value);
                }
            },
            HeaderAppendAction::OverwriteIfExistsOrAdd => {
                headers.insert(header_name, header_value);
            },
            HeaderAppendAction::OverwriteIfExists => {
                if headers.contains_key(&header_name) {
                    headers.insert(header_name, header_value);
                }
            },
        }
    }
    Ok(())
}
