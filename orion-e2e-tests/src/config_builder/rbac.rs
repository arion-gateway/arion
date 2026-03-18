use orion_data_plane_api::envoy_data_plane_api::{
    envoy::{
        config::{
            core::v3::CidrRange,
            rbac::v3::{
                permission::Rule as PermissionRule, principal::Identifier as PrincipalIdentifier,
                rbac::Action as RbacAction, Permission, Policy, Principal, Rbac,
            },
            route::v3::{header_matcher::HeaderMatchSpecifier, HeaderMatcher},
        },
        extensions::filters::{http::rbac::v3::Rbac as HttpRbac, network::rbac::v3::Rbac as NetworkRbac},
        r#type::{
            matcher::v3::{string_matcher::MatchPattern, StringMatcher},
            v3::Int32Range,
        },
    },
    google::protobuf::UInt32Value,
};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct NetworkRbacBuilder {
    action: RbacAction,
    policies: HashMap<String, Policy>,
}

impl NetworkRbacBuilder {
    #[must_use]
    pub fn allow() -> Self {
        Self { action: RbacAction::Allow, policies: HashMap::new() }
    }

    #[must_use]
    pub fn deny() -> Self {
        Self { action: RbacAction::Deny, policies: HashMap::new() }
    }

    #[must_use]
    pub fn policy(mut self, name: impl Into<String>, policy: impl Into<Policy>) -> Self {
        self.policies.insert(name.into(), policy.into());
        self
    }

    #[must_use]
    pub fn build(self) -> NetworkRbac {
        NetworkRbac {
            rules: Some(Rbac { action: self.action.into(), policies: self.policies, ..Default::default() }),
            ..Default::default()
        }
    }
}

impl From<NetworkRbacBuilder> for NetworkRbac {
    fn from(builder: NetworkRbacBuilder) -> Self {
        builder.build()
    }
}

#[derive(Debug, Clone, Default)]
pub struct NetworkRbacPolicyBuilder {
    permissions: Vec<Permission>,
    principals: Vec<Principal>,
}

impl NetworkRbacPolicyBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn permission_any(mut self) -> Self {
        self.permissions.push(Permission { rule: Some(PermissionRule::Any(true)) });
        self
    }

    #[must_use]
    pub fn permission_destination_ip(mut self, address_prefix: impl Into<String>, prefix_len: u32) -> Self {
        self.permissions.push(Permission {
            rule: Some(PermissionRule::DestinationIp(CidrRange {
                address_prefix: address_prefix.into(),
                prefix_len: Some(UInt32Value { value: prefix_len }),
            })),
        });
        self
    }

    #[must_use]
    pub fn permission_destination_port(mut self, port: u32) -> Self {
        self.permissions.push(Permission { rule: Some(PermissionRule::DestinationPort(port)) });
        self
    }

    #[must_use]
    pub fn permission_destination_port_range(mut self, start: i32, end: i32) -> Self {
        self.permissions
            .push(Permission { rule: Some(PermissionRule::DestinationPortRange(Int32Range { start, end })) });
        self
    }

    #[must_use]
    pub fn principal_any(mut self) -> Self {
        self.principals.push(Principal { identifier: Some(PrincipalIdentifier::Any(true)) });
        self
    }

    #[must_use]
    pub fn principal_source_ip(mut self, address_prefix: impl Into<String>, prefix_len: u32) -> Self {
        self.principals.push(Principal {
            identifier: Some(PrincipalIdentifier::DirectRemoteIp(CidrRange {
                address_prefix: address_prefix.into(),
                prefix_len: Some(UInt32Value { value: prefix_len }),
            })),
        });
        self
    }

    #[must_use]
    pub fn build(self) -> Policy {
        Policy { permissions: self.permissions, principals: self.principals, ..Default::default() }
    }
}

impl From<NetworkRbacPolicyBuilder> for Policy {
    fn from(builder: NetworkRbacPolicyBuilder) -> Self {
        builder.build()
    }
}

#[derive(Debug, Clone)]
pub struct HttpRbacBuilder {
    action: RbacAction,
    policies: HashMap<String, Policy>,
}

impl HttpRbacBuilder {
    #[must_use]
    pub fn allow() -> Self {
        Self { action: RbacAction::Allow, policies: HashMap::new() }
    }

    #[must_use]
    pub fn deny() -> Self {
        Self { action: RbacAction::Deny, policies: HashMap::new() }
    }

    #[must_use]
    pub fn policy(mut self, name: impl Into<String>, policy: impl Into<Policy>) -> Self {
        self.policies.insert(name.into(), policy.into());
        self
    }

    #[must_use]
    pub fn build(self) -> HttpRbac {
        HttpRbac {
            rules: Some(Rbac { action: self.action.into(), policies: self.policies, ..Default::default() }),
            ..Default::default()
        }
    }
}

impl From<HttpRbacBuilder> for HttpRbac {
    fn from(builder: HttpRbacBuilder) -> Self {
        builder.build()
    }
}

#[derive(Debug, Clone, Default)]
pub struct HttpRbacPolicyBuilder {
    permissions: Vec<Permission>,
    principals: Vec<Principal>,
}

impl HttpRbacPolicyBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn permission_any(mut self) -> Self {
        self.permissions.push(Permission { rule: Some(PermissionRule::Any(true)) });
        self
    }

    #[must_use]
    pub fn permission_header_exact(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.permissions.push(Permission {
            rule: Some(PermissionRule::Header(HeaderMatcher {
                name: name.into(),
                header_match_specifier: Some(HeaderMatchSpecifier::StringMatch(StringMatcher {
                    match_pattern: Some(MatchPattern::Exact(value.into())),
                    ..Default::default()
                })),
                ..Default::default()
            })),
        });
        self
    }

    #[must_use]
    pub fn permission_header_present(mut self, name: impl Into<String>) -> Self {
        self.permissions.push(Permission {
            rule: Some(PermissionRule::Header(HeaderMatcher {
                name: name.into(),
                header_match_specifier: Some(HeaderMatchSpecifier::PresentMatch(true)),
                ..Default::default()
            })),
        });
        self
    }

    #[must_use]
    pub fn principal_any(mut self) -> Self {
        self.principals.push(Principal { identifier: Some(PrincipalIdentifier::Any(true)) });
        self
    }

    #[must_use]
    pub fn principal_header_exact(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.principals.push(Principal {
            identifier: Some(PrincipalIdentifier::Header(HeaderMatcher {
                name: name.into(),
                header_match_specifier: Some(HeaderMatchSpecifier::StringMatch(StringMatcher {
                    match_pattern: Some(MatchPattern::Exact(value.into())),
                    ..Default::default()
                })),
                ..Default::default()
            })),
        });
        self
    }

    #[must_use]
    pub fn principal_header_present(mut self, name: impl Into<String>) -> Self {
        self.principals.push(Principal {
            identifier: Some(PrincipalIdentifier::Header(HeaderMatcher {
                name: name.into(),
                header_match_specifier: Some(HeaderMatchSpecifier::PresentMatch(true)),
                ..Default::default()
            })),
        });
        self
    }

    #[must_use]
    pub fn build(self) -> Policy {
        Policy { permissions: self.permissions, principals: self.principals, ..Default::default() }
    }
}

impl From<HttpRbacPolicyBuilder> for Policy {
    fn from(builder: HttpRbacPolicyBuilder) -> Self {
        builder.build()
    }
}
