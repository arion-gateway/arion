// Copyright 2026 The arion-gateway Authors
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

use crate::{Result, listeners::metadata::DownstreamConnectionMetadata};
use ahash::AHashMap;
use arion_configuration::config::listener::FilterChainMatch;
use ipnet::IpNet;
use smol_str::SmolStr;
use std::{collections::BTreeMap, net::IpAddr};
use triomphe::Arc;

/// Pre-compiled collection of filter chains and their hierarchical Envoy matcher tree.
///
/// Instead of scanning a `HashMap<FilterChainMatch, T>` in $O(N)$ on every incoming TCP
/// connection, [`FilterChains`] stores the filter chains in a contiguous `Vec<T>` and builds an
/// immutable 5-level decision tree ([`FilterChainMatcher`]) once at configuration time.
#[derive(Debug, Clone)]
pub struct FilterChains<T> {
    matcher: Arc<FilterChainMatcher>,
    chains: Vec<T>,
}

impl<T> Default for FilterChains<T> {
    fn default() -> Self {
        Self { matcher: Arc::new(FilterChainMatcher::empty()), chains: Vec::new() }
    }
}

impl<T> FromIterator<(FilterChainMatch, T)> for FilterChains<T> {
    fn from_iter<I: IntoIterator<Item = (FilterChainMatch, T)>>(iter: I) -> Self {
        let (match_rules, chains): (Vec<FilterChainMatch>, Vec<T>) = iter.into_iter().unzip();
        let matcher = Arc::new(FilterChainMatcher::new(&match_rules));
        Self { matcher, chains }
    }
}

impl<T> FilterChains<T> {
    /// Returns `true` if any [`FilterChainMatch`] in this collection has non-empty `server_names`.
    #[must_use]
    pub fn has_server_names(&self) -> bool {
        self.matcher.has_server_names
    }

    /// Returns an iterator over the stored filter chains.
    pub fn values(&self) -> std::slice::Iter<'_, T> {
        self.chains.iter()
    }

    /// Returns a mutable iterator over the stored filter chains.
    pub fn values_mut(&mut self) -> std::slice::IterMut<'_, T> {
        self.chains.iter_mut()
    }

    /// Transforms each filter chain value while reusing the already-compiled [`FilterChainMatcher`].
    pub fn try_map<U, E>(
        self,
        f: impl FnMut(T) -> std::result::Result<U, E>,
    ) -> std::result::Result<FilterChains<U>, E> {
        let chains = self.chains.into_iter().map(f).collect::<std::result::Result<Vec<U>, E>>()?;
        Ok(FilterChains { matcher: self.matcher, chains })
    }

    /// Selects the most specific matching filter chain for a connection according to the Envoy
    /// 5-level hierarchy:
    /// 1. `destination_port`
    /// 2. `destination_prefix_ranges` (Longest Prefix Match)
    /// 3. `server_names` (exact match, then wildcard suffix match by fewest labels, then fallback)
    /// 4. `source_prefix_ranges` (Longest Prefix Match)
    /// 5. `source_ports`
    pub fn select(
        &self,
        connection_metadata: &DownstreamConnectionMetadata,
        server_name: Option<&str>,
    ) -> Result<Option<&T>> {
        match self.matcher.select(connection_metadata, server_name) {
            Some(MatchLeaf::Single(idx)) => Ok(self.chains.get(idx)),
            Some(MatchLeaf::Multiple) => {
                Err("multiple filterchains matched a single connection. This is a bug in arion!".into())
            },
            None => Ok(None),
        }
    }
}

type Candidate<'a> = (usize, &'a FilterChainMatch);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MatchLeaf {
    Single(usize),
    Multiple,
}

impl MatchLeaf {
    #[inline]
    fn from_candidates(candidates: &[Candidate<'_>]) -> Option<Self> {
        match candidates {
            [] => None,
            [(idx, _)] => Some(Self::Single(*idx)),
            _ => Some(Self::Multiple),
        }
    }
}

#[derive(Debug, Clone)]
struct FilterChainMatcher {
    root: Option<DestPortMatcher>,
    has_server_names: bool,
}

impl FilterChainMatcher {
    fn empty() -> Self {
        Self { root: None, has_server_names: false }
    }

    fn new(match_rules: &[FilterChainMatch]) -> Self {
        let has_server_names = match_rules.iter().any(|m| !m.server_names.is_empty());
        let candidates: Vec<Candidate<'_>> = match_rules.iter().enumerate().collect();
        let root = DestPortMatcher::build(&candidates);
        Self { root, has_server_names }
    }

    #[inline]
    fn select(
        &self,
        connection_metadata: &DownstreamConnectionMetadata,
        server_name: Option<&str>,
    ) -> Option<MatchLeaf> {
        let root = self.root.as_ref()?;
        let source_addr = connection_metadata.peer_address();
        let destination_addr = connection_metadata.local_address();

        let level2 = root.select_next(destination_addr.port())?;
        let level3 = level2.select_next(destination_addr.ip())?;
        let level4 = level3.select_next(server_name.unwrap_or(""))?;
        let level5 = level4.select_next(source_addr.ip())?;
        level5.select_next(source_addr.port())
    }
}

type DestIpMatcher = IpMatcher<ServerNameMatcher>;
type SourceIpMatcher = IpMatcher<SourcePortMatcher>;

/// Level 1: Destination port matcher.
#[derive(Debug, Clone)]
enum DestPortMatcher {
    WildcardOnly(DestIpMatcher),
    WithExact { exact: AHashMap<u16, DestIpMatcher>, wildcard: Option<Box<DestIpMatcher>> },
}

impl DestPortMatcher {
    fn build(candidates: &[Candidate<'_>]) -> Option<Self> {
        if candidates.is_empty() {
            return None;
        }

        let has_exact = candidates.iter().any(|(_, m)| m.destination_port.is_some());
        if !has_exact {
            return DestIpMatcher::build(candidates, |m| &m.destination_prefix_ranges, ServerNameMatcher::build)
                .map(Self::WildcardOnly);
        }

        let mut port_groups: AHashMap<u16, Vec<Candidate<'_>>> = AHashMap::new();
        let mut wildcard_candidates: Vec<Candidate<'_>> = Vec::new();

        for &candidate @ (_, m) in candidates {
            if let Some(port) = m.destination_port {
                port_groups.entry(port).or_default().push(candidate);
            } else {
                wildcard_candidates.push(candidate);
            }
        }

        let exact = port_groups
            .into_iter()
            .filter_map(|(port, group)| {
                DestIpMatcher::build(&group, |m| &m.destination_prefix_ranges, ServerNameMatcher::build)
                    .map(|matcher| (port, matcher))
            })
            .collect();

        let wildcard =
            DestIpMatcher::build(&wildcard_candidates, |m| &m.destination_prefix_ranges, ServerNameMatcher::build)
                .map(Box::new);

        Some(Self::WithExact { exact, wildcard })
    }

    #[inline]
    fn select_next(&self, dst_port: u16) -> Option<&DestIpMatcher> {
        match self {
            Self::WildcardOnly(next) => Some(next),
            Self::WithExact { exact, wildcard } => exact.get(&dst_port).or(wildcard.as_deref()),
        }
    }
}

/// Level 2 & Level 4: CIDR Longest Prefix Match (Tuple Space Search ordered by descending prefix length).
#[derive(Debug, Clone)]
struct Ipv4PrefixLevel<Next> {
    mask: u32,
    prefixes: AHashMap<u32, Next>,
}

#[derive(Debug, Clone)]
struct Ipv6PrefixLevel<Next> {
    mask: u128,
    prefixes: AHashMap<u128, Next>,
}

#[derive(Debug, Clone)]
enum IpMatcher<Next> {
    WildcardOnly(Next),
    WithPrefixes { v4: Vec<Ipv4PrefixLevel<Next>>, v6: Vec<Ipv6PrefixLevel<Next>>, wildcard: Option<Box<Next>> },
}

#[inline]
const fn ipv4_mask(prefix_len: u8) -> u32 {
    if prefix_len == 0 { 0 } else { u32::MAX << (32 - prefix_len) }
}

#[inline]
const fn ipv6_mask(prefix_len: u8) -> u128 {
    if prefix_len == 0 { 0 } else { u128::MAX << (128 - prefix_len) }
}

impl<Next> IpMatcher<Next> {
    fn build<'a>(
        candidates: &[Candidate<'a>],
        get_ranges: fn(&FilterChainMatch) -> &[IpNet],
        build_next: fn(&[Candidate<'a>]) -> Option<Next>,
    ) -> Option<Self> {
        if candidates.is_empty() {
            return None;
        }

        let has_prefixes = candidates.iter().any(|(_, m)| !get_ranges(m).is_empty());
        if !has_prefixes {
            return build_next(candidates).map(Self::WildcardOnly);
        }

        let mut v4_groups: BTreeMap<u8, AHashMap<u32, Vec<Candidate<'a>>>> = BTreeMap::new();
        let mut v6_groups: BTreeMap<u8, AHashMap<u128, Vec<Candidate<'a>>>> = BTreeMap::new();
        let mut wildcard_candidates: Vec<Candidate<'a>> = Vec::new();

        for &candidate @ (i, m) in candidates {
            let ranges = get_ranges(m);
            if ranges.is_empty() {
                wildcard_candidates.push(candidate);
                continue;
            }
            for range in ranges {
                match range.trunc() {
                    IpNet::V4(v4net) => {
                        let group = v4_groups
                            .entry(v4net.prefix_len())
                            .or_default()
                            .entry(v4net.network().to_bits())
                            .or_default();
                        if group.last().map(|(idx, _)| *idx) != Some(i) {
                            group.push(candidate);
                        }
                    },
                    IpNet::V6(v6net) => {
                        let group = v6_groups
                            .entry(v6net.prefix_len())
                            .or_default()
                            .entry(v6net.network().to_bits())
                            .or_default();
                        if group.last().map(|(idx, _)| *idx) != Some(i) {
                            group.push(candidate);
                        }
                    },
                }
            }
        }

        // BTreeMap iterates in ascending order of prefix_len; reverse so Longest Prefix Match comes first.
        let v4 = v4_groups
            .into_iter()
            .rev()
            .filter_map(|(prefix_len, net_map)| {
                let prefixes: AHashMap<u32, Next> = net_map
                    .into_iter()
                    .filter_map(|(net_bits, group)| build_next(&group).map(|next| (net_bits, next)))
                    .collect();
                (!prefixes.is_empty()).then(|| Ipv4PrefixLevel { mask: ipv4_mask(prefix_len), prefixes })
            })
            .collect();

        let v6 = v6_groups
            .into_iter()
            .rev()
            .filter_map(|(prefix_len, net_map)| {
                let prefixes: AHashMap<u128, Next> = net_map
                    .into_iter()
                    .filter_map(|(net_bits, group)| build_next(&group).map(|next| (net_bits, next)))
                    .collect();
                (!prefixes.is_empty()).then(|| Ipv6PrefixLevel { mask: ipv6_mask(prefix_len), prefixes })
            })
            .collect();

        let wildcard = build_next(&wildcard_candidates).map(Box::new);

        Some(Self::WithPrefixes { v4, v6, wildcard })
    }

    #[inline]
    fn select_next(&self, ip: IpAddr) -> Option<&Next> {
        match self {
            Self::WildcardOnly(next) => Some(next),
            Self::WithPrefixes { v4, v6, wildcard } => {
                match ip {
                    IpAddr::V4(ipv4) => {
                        let bits = ipv4.to_bits();
                        for level in v4 {
                            if let Some(next) = level.prefixes.get(&(bits & level.mask)) {
                                return Some(next);
                            }
                        }
                    },
                    IpAddr::V6(ipv6) => {
                        let bits = ipv6.to_bits();
                        for level in v6 {
                            if let Some(next) = level.prefixes.get(&(bits & level.mask)) {
                                return Some(next);
                            }
                        }
                    },
                }
                wildcard.as_deref()
            },
        }
    }
}

/// Level 3: Server name (SNI) matcher.
#[derive(Debug, Clone)]
enum ServerNameMatcher {
    WildcardOnly(SourceIpMatcher),
    WithRules {
        exact: AHashMap<SmolStr, SourceIpMatcher>,
        wildcard_suffixes: AHashMap<SmolStr, SourceIpMatcher>,
        fallback: Option<Box<SourceIpMatcher>>,
    },
}

impl ServerNameMatcher {
    fn build(candidates: &[Candidate<'_>]) -> Option<Self> {
        if candidates.is_empty() {
            return None;
        }

        let has_rules = candidates.iter().any(|(_, m)| !m.server_names.is_empty());
        if !has_rules {
            return SourceIpMatcher::build(candidates, |m| &m.source_prefix_ranges, SourcePortMatcher::build)
                .map(Self::WildcardOnly);
        }

        let mut exact_groups: AHashMap<SmolStr, Vec<Candidate<'_>>> = AHashMap::new();
        let mut wildcard_groups: AHashMap<SmolStr, Vec<Candidate<'_>>> = AHashMap::new();
        let mut fallback_candidates: Vec<Candidate<'_>> = Vec::new();

        for &candidate @ (i, m) in candidates {
            let server_names = &m.server_names;
            if server_names.is_empty() {
                fallback_candidates.push(candidate);
                continue;
            }
            for sn in server_names {
                let group = if sn.match_subdomains() {
                    wildcard_groups.entry(sn.name().clone()).or_default()
                } else {
                    exact_groups.entry(sn.name().clone()).or_default()
                };
                if group.last().map(|(idx, _)| *idx) != Some(i) {
                    group.push(candidate);
                }
            }
        }

        let exact = exact_groups
            .into_iter()
            .filter_map(|(name, group)| {
                SourceIpMatcher::build(&group, |m| &m.source_prefix_ranges, SourcePortMatcher::build)
                    .map(|next| (name, next))
            })
            .collect();

        let wildcard_suffixes = wildcard_groups
            .into_iter()
            .filter_map(|(suffix, group)| {
                SourceIpMatcher::build(&group, |m| &m.source_prefix_ranges, SourcePortMatcher::build)
                    .map(|next| (suffix, next))
            })
            .collect();

        let fallback =
            SourceIpMatcher::build(&fallback_candidates, |m| &m.source_prefix_ranges, SourcePortMatcher::build)
                .map(Box::new);

        Some(Self::WithRules { exact, wildcard_suffixes, fallback })
    }

    #[inline]
    fn select_next(&self, server_name: &str) -> Option<&SourceIpMatcher> {
        match self {
            Self::WildcardOnly(next) => Some(next),
            Self::WithRules { exact, wildcard_suffixes, fallback } => {
                // 1. Exact match (score 0 - most specific)
                if let Some(next) = exact.get(server_name) {
                    return Some(next);
                }
                // 2. Subdomain wildcard match (e.g. ".more.specific" then ".specific")
                // Scanning dots from left to right checks suffixes from longest (fewest wildcard labels)
                // to shortest (most wildcard labels).
                if !wildcard_suffixes.is_empty() {
                    for (idx, byte) in server_name.bytes().enumerate() {
                        if byte == b'.'
                            && let Some(suffix) = server_name.get(idx..)
                            && let Some(next) = wildcard_suffixes.get(suffix)
                        {
                            return Some(next);
                        }
                    }
                }
                // 3. Fallback (NoRule)
                fallback.as_deref()
            },
        }
    }
}

/// Level 5: Source port matcher.
#[derive(Debug, Clone)]
enum SourcePortMatcher {
    WildcardOnly(MatchLeaf),
    WithExact { exact: AHashMap<u16, MatchLeaf>, wildcard: Option<MatchLeaf> },
}

impl SourcePortMatcher {
    fn build(candidates: &[Candidate<'_>]) -> Option<Self> {
        if candidates.is_empty() {
            return None;
        }

        let has_exact = candidates.iter().any(|(_, m)| !m.source_ports.is_empty());
        if !has_exact {
            return MatchLeaf::from_candidates(candidates).map(Self::WildcardOnly);
        }

        let mut port_groups: AHashMap<u16, Vec<Candidate<'_>>> = AHashMap::new();
        let mut wildcard_candidates: Vec<Candidate<'_>> = Vec::new();

        for &candidate @ (i, m) in candidates {
            let ports = &m.source_ports;
            if ports.is_empty() {
                wildcard_candidates.push(candidate);
                continue;
            }
            for &port in ports {
                let group = port_groups.entry(port).or_default();
                if group.last().map(|(idx, _)| *idx) != Some(i) {
                    group.push(candidate);
                }
            }
        }

        let exact = port_groups
            .into_iter()
            .filter_map(|(port, group)| MatchLeaf::from_candidates(&group).map(|leaf| (port, leaf)))
            .collect();

        let wildcard = MatchLeaf::from_candidates(&wildcard_candidates);

        Some(Self::WithExact { exact, wildcard })
    }

    #[inline]
    fn select_next(&self, src_port: u16) -> Option<MatchLeaf> {
        match self {
            Self::WildcardOnly(leaf) => Some(*leaf),
            Self::WithExact { exact, wildcard } => exact.get(&src_port).copied().or(*wildcard),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arion_configuration::config::listener::{MatchResult, ServerNameMatch};
    use smallvec::{SmallVec, smallvec};
    use std::{
        net::{Ipv4Addr, Ipv6Addr},
        str::FromStr,
    };

    /// Reference implementation of the original O(N) `select_filterchain` algorithm,
    /// used as an oracle to verify 100% behavioral equivalence.
    fn reference_select<'a, T>(
        entries: &'a [(FilterChainMatch, T)],
        connection_metadata: &DownstreamConnectionMetadata,
        server_name: Option<&str>,
    ) -> Result<Option<&'a T>> {
        fn match_subitem<'a, F: Fn(&FilterChainMatch, C) -> MatchResult, C: Copy>(
            function: F,
            comparand: C,
            iter: impl Iterator<Item = &'a FilterChainMatch>,
            scratchpad: &mut [MatchResult],
            possible_filters: &mut [bool],
        ) {
            let mut best_match = MatchResult::FailedMatch;
            for (i, match_config) in iter.enumerate() {
                if !possible_filters.get(i).copied().unwrap_or(false) {
                    continue;
                }
                let match_result = function(match_config, comparand);
                if let Some(slot) = scratchpad.get_mut(i) {
                    *slot = match_result;
                }
                if match_result > best_match {
                    best_match = match_result;
                }
            }
            for (i, result) in scratchpad.iter().enumerate() {
                if (*result != best_match || *result == MatchResult::FailedMatch)
                    && let Some(is_possible) = possible_filters.get_mut(i)
                {
                    *is_possible = false;
                }
            }
        }

        let source_addr = connection_metadata.peer_address();
        let destination_addr = connection_metadata.local_address();
        let num_chains = entries.len();
        let mut possible_filters: SmallVec<[bool; 8]> = smallvec![true; num_chains];
        let mut scratchpad: SmallVec<[MatchResult; 8]> = smallvec![MatchResult::NoRule; num_chains];

        match_subitem(
            FilterChainMatch::matches_destination_port,
            destination_addr.port(),
            entries.iter().map(|(k, _)| k),
            &mut scratchpad,
            &mut possible_filters,
        );
        match_subitem(
            FilterChainMatch::matches_destination_ip,
            destination_addr.ip(),
            entries.iter().map(|(k, _)| k),
            &mut scratchpad,
            &mut possible_filters,
        );
        match_subitem(
            FilterChainMatch::matches_server_name,
            server_name.unwrap_or(""),
            entries.iter().map(|(k, _)| k),
            &mut scratchpad,
            &mut possible_filters,
        );
        match_subitem(
            FilterChainMatch::matches_source_ip,
            source_addr.ip(),
            entries.iter().map(|(k, _)| k),
            &mut scratchpad,
            &mut possible_filters,
        );
        match_subitem(
            FilterChainMatch::matches_source_port,
            source_addr.port(),
            entries.iter().map(|(k, _)| k),
            &mut scratchpad,
            &mut possible_filters,
        );

        let mut final_candidates = possible_filters
            .into_iter()
            .zip(entries.iter().map(|(_, v)| v))
            .filter_map(|(include, value)| include.then_some(value));

        let first_match = final_candidates.next();
        if final_candidates.next().is_some() {
            Err("multiple filterchains matched a single connection. This is a bug in arion!".into())
        } else {
            Ok(first_match)
        }
    }

    #[test]
    fn empty_filter_chains_returns_none() {
        let chains: FilterChains<usize> = FilterChains::default();
        let meta = DownstreamConnectionMetadata::FromSocket {
            peer_address: (Ipv4Addr::LOCALHOST, 1000).into(),
            local_address: (Ipv4Addr::LOCALHOST, 8080).into(),
        };
        assert_eq!(chains.select(&meta, None).unwrap(), None);
    }

    #[test]
    fn ipv6_longest_prefix_match() {
        let chains: FilterChains<usize> = [
            (
                FilterChainMatch {
                    destination_prefix_ranges: vec![IpNet::from_str("2001:db8::/32").unwrap()],
                    ..Default::default()
                },
                10,
            ),
            (
                FilterChainMatch {
                    destination_prefix_ranges: vec![IpNet::from_str("2001:db8:abcd::/48").unwrap()],
                    ..Default::default()
                },
                20,
            ),
        ]
        .into_iter()
        .collect();

        let meta_48 = DownstreamConnectionMetadata::FromSocket {
            peer_address: (Ipv6Addr::LOCALHOST, 1000).into(),
            local_address: (Ipv6Addr::from_str("2001:db8:abcd::1").unwrap(), 443).into(),
        };
        assert_eq!(chains.select(&meta_48, None).unwrap().copied(), Some(20));

        let meta_32 = DownstreamConnectionMetadata::FromSocket {
            peer_address: (Ipv6Addr::LOCALHOST, 1000).into(),
            local_address: (Ipv6Addr::from_str("2001:db8:1111::1").unwrap(), 443).into(),
        };
        assert_eq!(chains.select(&meta_32, None).unwrap().copied(), Some(10));
    }

    #[test]
    fn ambiguous_overlap_returns_error() {
        let chains: FilterChains<usize> = [
            (
                FilterChainMatch {
                    server_names: vec![
                        ServerNameMatch::from_str("a.example.com").unwrap(),
                        ServerNameMatch::from_str("shared.example.com").unwrap(),
                    ],
                    ..Default::default()
                },
                0,
            ),
            (
                FilterChainMatch {
                    server_names: vec![
                        ServerNameMatch::from_str("shared.example.com").unwrap(),
                        ServerNameMatch::from_str("b.example.com").unwrap(),
                    ],
                    ..Default::default()
                },
                1,
            ),
        ]
        .into_iter()
        .collect();

        let meta = DownstreamConnectionMetadata::FromSocket {
            peer_address: (Ipv4Addr::LOCALHOST, 1000).into(),
            local_address: (Ipv4Addr::LOCALHOST, 443).into(),
        };
        assert_eq!(chains.select(&meta, Some("a.example.com")).unwrap().copied(), Some(0));
        assert_eq!(chains.select(&meta, Some("b.example.com")).unwrap().copied(), Some(1));
        chains.select(&meta, Some("shared.example.com")).unwrap_err();
    }

    #[test]
    fn no_backtracking_across_levels() {
        // Chain 0 matches port 8443 + SNI "api.example.com" + source_port 5000.
        // Chain 1 is a complete wildcard fallback (destination_port: None, server_names: []).
        // A connection on port 8443 with SNI "other.example.com" (or source_port 9999)
        // must prune Chain 1 at Level 1 (port 8443) and then fail to match Chain 0 at Level 3/5,
        // returning Ok(None) rather than backtracking to Chain 1.
        let entries = vec![
            (
                FilterChainMatch {
                    destination_port: Some(8443),
                    server_names: vec![ServerNameMatch::from_str("api.example.com").unwrap()],
                    source_ports: vec![5000],
                    ..Default::default()
                },
                0usize,
            ),
            (FilterChainMatch::default(), 1usize),
        ];
        let chains: FilterChains<usize> = entries.iter().cloned().collect();

        // Port 8443, wrong SNI -> None
        let meta_8443_5000 = DownstreamConnectionMetadata::FromSocket {
            peer_address: (Ipv4Addr::LOCALHOST, 5000).into(),
            local_address: (Ipv4Addr::LOCALHOST, 8443).into(),
        };
        assert_eq!(chains.select(&meta_8443_5000, Some("other.example.com")).unwrap(), None);
        assert_eq!(
            chains.select(&meta_8443_5000, Some("other.example.com")).unwrap(),
            reference_select(&entries, &meta_8443_5000, Some("other.example.com")).unwrap()
        );

        // Port 8443, right SNI, wrong source port -> None
        let meta_8443_9999 = DownstreamConnectionMetadata::FromSocket {
            peer_address: (Ipv4Addr::LOCALHOST, 9999).into(),
            local_address: (Ipv4Addr::LOCALHOST, 8443).into(),
        };
        assert_eq!(chains.select(&meta_8443_9999, Some("api.example.com")).unwrap(), None);
        assert_eq!(
            chains.select(&meta_8443_9999, Some("api.example.com")).unwrap(),
            reference_select(&entries, &meta_8443_9999, Some("api.example.com")).unwrap()
        );

        // Port 9000 -> falls back to Chain 1 at Level 1
        let meta_9000 = DownstreamConnectionMetadata::FromSocket {
            peer_address: (Ipv4Addr::LOCALHOST, 9999).into(),
            local_address: (Ipv4Addr::LOCALHOST, 9000).into(),
        };
        assert_eq!(chains.select(&meta_9000, Some("other.example.com")).unwrap().copied(), Some(1));
    }

    #[test]
    fn zero_prefix_len_beats_empty_ranges_and_truncates_host_bits() {
        // 0.0.0.0/0 has prefix_len 0 -> Matched(32) for any IPv4, which beats NoRule (empty ranges).
        // For IPv6, 0.0.0.0/0 does not match, so it falls back to Chain 2 (NoRule).
        // Also tests non-canonical CIDR with host bits set (10.1.2.3/8 -> 10.0.0.0/8).
        let entries = vec![
            (
                FilterChainMatch {
                    destination_prefix_ranges: vec![IpNet::from_str("10.1.2.3/8").unwrap()],
                    ..Default::default()
                },
                0usize,
            ),
            (
                FilterChainMatch {
                    destination_prefix_ranges: vec![IpNet::from_str("0.0.0.0/0").unwrap()],
                    ..Default::default()
                },
                1usize,
            ),
            (FilterChainMatch::default(), 2usize),
        ];
        let chains: FilterChains<usize> = entries.iter().cloned().collect();

        // 10.99.88.77 matches truncated 10.0.0.0/8 -> Chain 0
        let meta_subnet_10 = DownstreamConnectionMetadata::FromSocket {
            peer_address: (Ipv4Addr::LOCALHOST, 1000).into(),
            local_address: (Ipv4Addr::new(10, 99, 88, 77), 443).into(),
        };
        assert_eq!(chains.select(&meta_subnet_10, None).unwrap().copied(), Some(0));
        assert_eq!(
            chains.select(&meta_subnet_10, None).unwrap(),
            reference_select(&entries, &meta_subnet_10, None).unwrap()
        );

        // 192.168.1.1 matches 0.0.0.0/0 -> Chain 1 (beats NoRule Chain 2)
        let meta_ipv4_other = DownstreamConnectionMetadata::FromSocket {
            peer_address: (Ipv4Addr::LOCALHOST, 1000).into(),
            local_address: (Ipv4Addr::new(192, 168, 1, 1), 443).into(),
        };
        assert_eq!(chains.select(&meta_ipv4_other, None).unwrap().copied(), Some(1));
        assert_eq!(
            chains.select(&meta_ipv4_other, None).unwrap(),
            reference_select(&entries, &meta_ipv4_other, None).unwrap()
        );

        // IPv6 ::1 does not match 10.0.0.0/8 or 0.0.0.0/0 -> falls back to Chain 2 (NoRule)
        let meta_ipv6 = DownstreamConnectionMetadata::FromSocket {
            peer_address: (Ipv6Addr::LOCALHOST, 1000).into(),
            local_address: (Ipv6Addr::LOCALHOST, 443).into(),
        };
        assert_eq!(chains.select(&meta_ipv6, None).unwrap().copied(), Some(2));
        assert_eq!(chains.select(&meta_ipv6, None).unwrap(), reference_select(&entries, &meta_ipv6, None).unwrap());
    }

    #[test]
    fn duplicate_rules_within_single_chain_do_not_cause_ambiguity() {
        let chains: FilterChains<usize> = [(
            FilterChainMatch {
                destination_prefix_ranges: vec![
                    IpNet::from_str("10.0.0.1/8").unwrap(),
                    IpNet::from_str("10.0.0.2/8").unwrap(),
                ],
                server_names: vec![
                    ServerNameMatch::from_str("dup.example.com").unwrap(),
                    ServerNameMatch::from_str("dup.example.com").unwrap(),
                    ServerNameMatch::from_str("*.example.com").unwrap(),
                    ServerNameMatch::from_str("*.example.com").unwrap(),
                ],
                source_ports: vec![3000, 3000, 4000],
                ..Default::default()
            },
            42,
        )]
        .into_iter()
        .collect();

        let meta = DownstreamConnectionMetadata::FromSocket {
            peer_address: (Ipv4Addr::LOCALHOST, 3000).into(),
            local_address: (Ipv4Addr::new(10, 5, 5, 5), 443).into(),
        };
        assert_eq!(chains.select(&meta, Some("dup.example.com")).unwrap().copied(), Some(42));
        assert_eq!(chains.select(&meta, Some("sub.example.com")).unwrap().copied(), Some(42));
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn differential_equivalence_against_reference_implementation() {
        let entries = vec![
            (
                FilterChainMatch {
                    destination_port: Some(443),
                    destination_prefix_ranges: vec![IpNet::from_str("10.0.0.0/8").unwrap()],
                    server_names: vec![
                        ServerNameMatch::from_str("api.service.internal").unwrap(),
                        ServerNameMatch::from_str("*.service.internal").unwrap(),
                    ],
                    source_prefix_ranges: vec![IpNet::from_str("172.16.0.0/12").unwrap()],
                    source_ports: vec![10000, 10001],
                },
                0usize,
            ),
            (
                FilterChainMatch {
                    destination_port: Some(443),
                    destination_prefix_ranges: vec![IpNet::from_str("10.0.0.0/8").unwrap()],
                    server_names: vec![ServerNameMatch::from_str("*.service.internal").unwrap()],
                    source_prefix_ranges: vec![IpNet::from_str("172.16.1.0/24").unwrap()],
                    source_ports: vec![],
                },
                1usize,
            ),
            (
                FilterChainMatch {
                    destination_port: Some(443),
                    destination_prefix_ranges: vec![IpNet::from_str("10.0.0.0/8").unwrap()],
                    server_names: vec![ServerNameMatch::from_str("*.internal").unwrap()],
                    source_prefix_ranges: vec![],
                    source_ports: vec![],
                },
                2usize,
            ),
            (
                FilterChainMatch {
                    destination_port: Some(443),
                    destination_prefix_ranges: vec![
                        IpNet::from_str("10.1.0.0/16").unwrap(),
                        IpNet::from_str("2001:db8::/32").unwrap(),
                    ],
                    server_names: vec![],
                    source_prefix_ranges: vec![],
                    source_ports: vec![],
                },
                3usize,
            ),
            (
                FilterChainMatch {
                    destination_port: Some(443),
                    destination_prefix_ranges: vec![],
                    server_names: vec![],
                    source_prefix_ranges: vec![],
                    source_ports: vec![],
                },
                4usize,
            ),
            (FilterChainMatch::default(), 5usize),
        ];

        let chains: FilterChains<usize> = entries.iter().cloned().collect();

        let dst_ports = [443u16, 8443];
        let dst_ips: [IpAddr; 4] = [
            Ipv4Addr::new(10, 0, 1, 1).into(),
            Ipv4Addr::new(10, 1, 1, 1).into(),
            Ipv4Addr::new(192, 168, 1, 1).into(),
            Ipv6Addr::from_str("2001:db8::1").unwrap().into(),
        ];
        let snis = [
            None,
            Some("api.service.internal"),
            Some("web.service.internal"),
            Some("deep.web.service.internal"),
            Some("other.internal"),
            Some("external.org"),
        ];
        let src_ips: [IpAddr; 3] = [
            Ipv4Addr::new(172, 16, 1, 5).into(),
            Ipv4Addr::new(172, 16, 2, 5).into(),
            Ipv4Addr::new(10, 0, 0, 1).into(),
        ];
        let src_ports = [10000u16, 10001, 55555];

        for &dst_port in &dst_ports {
            for &dst_ip in &dst_ips {
                for &sni in &snis {
                    for &src_ip in &src_ips {
                        for &src_port in &src_ports {
                            let meta = DownstreamConnectionMetadata::FromSocket {
                                peer_address: (src_ip, src_port).into(),
                                local_address: (dst_ip, dst_port).into(),
                            };
                            let actual = chains.select(&meta, sni);
                            let expected = reference_select(&entries, &meta, sni);
                            assert_eq!(
                                actual.as_ref().ok(),
                                expected.as_ref().ok(),
                                "mismatch for dst={dst_ip}:{dst_port}, sni={sni:?}, src={src_ip}:{src_port}"
                            );
                            assert_eq!(actual.is_err(), expected.is_err());
                        }
                    }
                }
            }
        }
    }

    /// Builds a `FilterChainMatch` from a 5-bit mask:
    /// - bit 0 (1): `destination_port`
    /// - bit 1 (2): `destination_prefix_ranges`
    /// - bit 2 (4): `server_names`
    /// - bit 3 (8): `source_prefix_ranges`
    /// - bit 4 (16): `source_ports`
    fn make_chain_from_mask(mask: usize) -> FilterChainMatch {
        FilterChainMatch {
            destination_port: ((mask & 1) != 0).then_some(8443),
            destination_prefix_ranges: if (mask & 2) != 0 {
                vec![IpNet::from_str("10.1.0.0/16").unwrap(), IpNet::from_str("2001:db8:1::/48").unwrap()]
            } else {
                vec![]
            },
            server_names: if (mask & 4) != 0 {
                vec![
                    ServerNameMatch::from_str("app.example.com").unwrap(),
                    ServerNameMatch::from_str("*.sub.example.com").unwrap(),
                ]
            } else {
                vec![]
            },
            source_prefix_ranges: if (mask & 8) != 0 {
                vec![IpNet::from_str("192.168.1.0/24").unwrap(), IpNet::from_str("fd00:1::/32").unwrap()]
            } else {
                vec![]
            },
            source_ports: if (mask & 16) != 0 { vec![5000, 5001] } else { vec![] },
        }
    }

    #[test]
    fn cartesian_product_single_chain_all_32_optional_field_masks() {
        // Tests all 2^5 = 32 combinations of optional FilterChainMatch fields in isolation
        // against the full Cartesian product of matching/non-matching connection attributes.
        let dst_ports = [(8443u16, true), (9000u16, false)];
        let dst_ips: [(IpAddr, bool); 4] = [
            (Ipv4Addr::new(10, 1, 2, 3).into(), true),
            (Ipv4Addr::new(10, 2, 2, 3).into(), false),
            (Ipv6Addr::from_str("2001:db8:1::1").unwrap().into(), true),
            (Ipv6Addr::from_str("2001:db8:2::1").unwrap().into(), false),
        ];
        let snis = [
            (Some("app.example.com"), true),
            (Some("x.sub.example.com"), true),
            (Some("a.b.sub.example.com"), true),
            (Some("sub.example.com"), false),
            (Some("other.example.com"), false),
            (None, false),
        ];
        let src_ips: [(IpAddr, bool); 4] = [
            (Ipv4Addr::new(192, 168, 1, 42).into(), true),
            (Ipv4Addr::new(192, 168, 2, 42).into(), false),
            (Ipv6Addr::from_str("fd00:1::99").unwrap().into(), true),
            (Ipv6Addr::from_str("fd00:2::99").unwrap().into(), false),
        ];
        let src_ports = [(5000u16, true), (5001u16, true), (9999u16, false)];

        for chain_mask in 0..32usize {
            let entries = vec![(make_chain_from_mask(chain_mask), chain_mask)];
            let chains: FilterChains<usize> = entries.iter().cloned().collect();
            assert_eq!(chains.has_server_names(), (chain_mask & 4) != 0);

            for &(dst_port, dp_match) in &dst_ports {
                for &(dst_ip, di_match) in &dst_ips {
                    for &(sni, sni_match) in &snis {
                        for &(src_ip, si_match) in &src_ips {
                            for &(src_port, sp_match) in &src_ports {
                                let input_mask = usize::from(dp_match)
                                    | (usize::from(di_match) << 1)
                                    | (usize::from(sni_match) << 2)
                                    | (usize::from(si_match) << 3)
                                    | (usize::from(sp_match) << 4);

                                let meta = DownstreamConnectionMetadata::FromSocket {
                                    peer_address: (src_ip, src_port).into(),
                                    local_address: (dst_ip, dst_port).into(),
                                };

                                let actual = chains.select(&meta, sni).unwrap().copied();
                                let expected_ref = reference_select(&entries, &meta, sni).unwrap().copied();
                                // A single chain with `chain_mask` matches iff every bit set in `chain_mask`
                                // is also set in `input_mask`.
                                let expected_analytic = ((input_mask & chain_mask) == chain_mask).then_some(chain_mask);

                                assert_eq!(
                                    actual, expected_analytic,
                                    "analytic mismatch for chain_mask={chain_mask:#07b}, input_mask={input_mask:#07b}"
                                );
                                assert_eq!(
                                    actual, expected_ref,
                                    "reference mismatch for chain_mask={chain_mask:#07b}, input_mask={input_mask:#07b}"
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn cartesian_product_all_32_chains_simultaneously() {
        // Compiles all 32 (2^5) FilterChainMatch optional field combinations into a single
        // `FilterChains` table so every node at every level of the tree exercises both
        // exact/prefix/rule branches AND wildcard/fallback branches simultaneously.
        let entries: Vec<(FilterChainMatch, usize)> =
            (0..32usize).map(|mask| (make_chain_from_mask(mask), mask)).collect();
        let chains: FilterChains<usize> = entries.iter().cloned().collect();
        assert!(chains.has_server_names());

        let dst_ports = [(8443u16, true), (9000u16, false)];
        let dst_ips: [(IpAddr, bool); 4] = [
            (Ipv4Addr::new(10, 1, 2, 3).into(), true),
            (Ipv4Addr::new(10, 2, 2, 3).into(), false),
            (Ipv6Addr::from_str("2001:db8:1::1").unwrap().into(), true),
            (Ipv6Addr::from_str("2001:db8:2::1").unwrap().into(), false),
        ];
        let snis = [
            (Some("app.example.com"), true),
            (Some("x.sub.example.com"), true),
            (Some("other.example.com"), false),
            (None, false),
        ];
        let src_ips: [(IpAddr, bool); 4] = [
            (Ipv4Addr::new(192, 168, 1, 42).into(), true),
            (Ipv4Addr::new(192, 168, 2, 42).into(), false),
            (Ipv6Addr::from_str("fd00:1::99").unwrap().into(), true),
            (Ipv6Addr::from_str("fd00:2::99").unwrap().into(), false),
        ];
        let src_ports = [(5000u16, true), (5001u16, true), (9999u16, false)];

        for &(dst_port, dp_match) in &dst_ports {
            for &(dst_ip, di_match) in &dst_ips {
                for &(sni, sni_match) in &snis {
                    for &(src_ip, si_match) in &src_ips {
                        for &(src_port, sp_match) in &src_ports {
                            let expected_mask = usize::from(dp_match)
                                | (usize::from(di_match) << 1)
                                | (usize::from(sni_match) << 2)
                                | (usize::from(si_match) << 3)
                                | (usize::from(sp_match) << 4);

                            let meta = DownstreamConnectionMetadata::FromSocket {
                                peer_address: (src_ip, src_port).into(),
                                local_address: (dst_ip, dst_port).into(),
                            };

                            let actual = chains.select(&meta, sni).unwrap().copied();
                            let expected_ref = reference_select(&entries, &meta, sni).unwrap().copied();

                            assert_eq!(actual, Some(expected_mask));
                            assert_eq!(actual, expected_ref);
                        }
                    }
                }
            }
        }
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn cartesian_product_multi_specificity_and_sparse_pairs() {
        // Exercises multi-tier specificity across all 5 levels:
        // - Level 1: None vs Some(443) vs Some(8443)
        // - Level 2: [] vs /0 vs /8 vs /16 vs /32 (IPv4) and /0 vs /32 vs /64 vs /128 (IPv6)
        // - Level 3: [] vs *.com vs *.example.com vs *.sub.example.com vs api.sub.example.com
        // - Level 4: [] vs /0 vs /12 vs /24 vs /32 (IPv4) and /0 vs /48 vs /128 (IPv6)
        // - Level 5: [] vs [1000] vs [2000, 2001]
        let entries = vec![
            // Sparse combination: only dst_ip /32 + src_port
            (
                FilterChainMatch {
                    destination_port: None,
                    destination_prefix_ranges: vec![IpNet::from_str("10.1.2.3/32").unwrap()],
                    server_names: vec![],
                    source_prefix_ranges: vec![],
                    source_ports: vec![2000],
                },
                0usize,
            ),
            // Sparse combination: only dst_ip /16 + SNI 3-label wildcard
            (
                FilterChainMatch {
                    destination_port: None,
                    destination_prefix_ranges: vec![IpNet::from_str("10.1.0.0/16").unwrap()],
                    server_names: vec![ServerNameMatch::from_str("*.sub.example.com").unwrap()],
                    source_prefix_ranges: vec![],
                    source_ports: vec![],
                },
                1usize,
            ),
            // Sparse combination: only dst_ip /16 + SNI 2-label wildcard + src_ip /24
            (
                FilterChainMatch {
                    destination_port: None,
                    destination_prefix_ranges: vec![IpNet::from_str("10.1.0.0/16").unwrap()],
                    server_names: vec![ServerNameMatch::from_str("*.example.com").unwrap()],
                    source_prefix_ranges: vec![IpNet::from_str("172.16.1.0/24").unwrap()],
                    source_ports: vec![],
                },
                2usize,
            ),
            // Sparse combination: only dst_ip /16 + SNI 1-label wildcard (*.com)
            (
                FilterChainMatch {
                    destination_port: None,
                    destination_prefix_ranges: vec![IpNet::from_str("10.1.0.0/16").unwrap()],
                    server_names: vec![ServerNameMatch::from_str("*.com").unwrap()],
                    source_prefix_ranges: vec![],
                    source_ports: vec![],
                },
                3usize,
            ),
            // Sparse combination: only SNI exact + src_port
            (
                FilterChainMatch {
                    destination_port: None,
                    destination_prefix_ranges: vec![],
                    server_names: vec![ServerNameMatch::from_str("api.sub.example.com").unwrap()],
                    source_prefix_ranges: vec![],
                    source_ports: vec![1000],
                },
                4usize,
            ),
            // Sparse combination: only src_ip /32
            (
                FilterChainMatch {
                    destination_port: None,
                    destination_prefix_ranges: vec![],
                    server_names: vec![],
                    source_prefix_ranges: vec![IpNet::from_str("172.16.1.5/32").unwrap()],
                    source_ports: vec![],
                },
                5usize,
            ),
            // Sparse combination: only src_ip /0 (IPv4 catch-all) + src_port 2001
            (
                FilterChainMatch {
                    destination_port: None,
                    destination_prefix_ranges: vec![],
                    server_names: vec![],
                    source_prefix_ranges: vec![IpNet::from_str("0.0.0.0/0").unwrap()],
                    source_ports: vec![2001],
                },
                6usize,
            ),
            // IPv6 /128 destination + IPv6 /128 source
            (
                FilterChainMatch {
                    destination_port: Some(8443),
                    destination_prefix_ranges: vec![IpNet::from_str("2001:db8:1::1/128").unwrap()],
                    server_names: vec![],
                    source_prefix_ranges: vec![IpNet::from_str("fd00:1::1/128").unwrap()],
                    source_ports: vec![],
                },
                7usize,
            ),
            // IPv6 /64 destination + IPv6 /0 source
            (
                FilterChainMatch {
                    destination_port: Some(8443),
                    destination_prefix_ranges: vec![IpNet::from_str("2001:db8:1::/64").unwrap()],
                    server_names: vec![],
                    source_prefix_ranges: vec![IpNet::from_str("::/0").unwrap()],
                    source_ports: vec![],
                },
                8usize,
            ),
        ];

        let chains: FilterChains<usize> = entries.iter().cloned().collect();

        let dst_ports = [443u16, 8443];
        let dst_ips: [IpAddr; 5] = [
            Ipv4Addr::new(10, 1, 2, 3).into(),
            Ipv4Addr::new(10, 1, 9, 9).into(),
            Ipv4Addr::new(192, 168, 0, 1).into(),
            Ipv6Addr::from_str("2001:db8:1::1").unwrap().into(),
            Ipv6Addr::from_str("2001:db8:1::2").unwrap().into(),
        ];
        let snis = [
            None,
            Some(""),
            Some("api.sub.example.com"),
            Some("other.sub.example.com"),
            Some("deep.other.sub.example.com"),
            Some("foo.example.com"),
            Some("bar.com"),
            Some(".example.com"),
            Some("unmatched.org"),
        ];
        let src_ips: [IpAddr; 5] = [
            Ipv4Addr::new(172, 16, 1, 5).into(),
            Ipv4Addr::new(172, 16, 1, 6).into(),
            Ipv4Addr::new(10, 0, 0, 1).into(),
            Ipv6Addr::from_str("fd00:1::1").unwrap().into(),
            Ipv6Addr::from_str("fd00:2::1").unwrap().into(),
        ];
        let src_ports = [1000u16, 2000, 2001, 9999];

        for &dst_port in &dst_ports {
            for &dst_ip in &dst_ips {
                for &sni in &snis {
                    for &src_ip in &src_ips {
                        for &src_port in &src_ports {
                            let meta = DownstreamConnectionMetadata::FromProxyProtocol {
                                original_peer_address: (src_ip, src_port).into(),
                                original_destination_address: (dst_ip, dst_port).into(),
                                protocol: ppp::v2::Protocol::Stream,
                                tlv_data: std::collections::HashMap::new(),
                                proxy_peer_address: (Ipv4Addr::LOCALHOST, 1).into(),
                                proxy_local_address: (Ipv4Addr::LOCALHOST, 2).into(),
                            };
                            let actual = chains.select(&meta, sni);
                            let expected = reference_select(&entries, &meta, sni);
                            assert_eq!(
                                actual.as_ref().ok(),
                                expected.as_ref().ok(),
                                "mismatch for dst={dst_ip}:{dst_port}, sni={sni:?}, src={src_ip}:{src_port}"
                            );
                            assert_eq!(actual.is_err(), expected.is_err());
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn filter_chains_iterators_and_try_map() {
        let mut chains: FilterChains<usize> =
            [(make_chain_from_mask(1), 10usize), (make_chain_from_mask(0), 20usize)].into_iter().collect();
        assert!(!chains.has_server_names());
        assert_eq!(chains.values().copied().collect::<Vec<_>>(), vec![10, 20]);

        for v in chains.values_mut() {
            *v += 1;
        }
        assert_eq!(chains.values().copied().collect::<Vec<_>>(), vec![11, 21]);

        let mapped = chains.clone().try_map(|v| Ok::<_, &str>(v * 2)).unwrap();
        assert_eq!(mapped.values().copied().collect::<Vec<_>>(), vec![22, 42]);

        let err_res = chains.try_map(|v| if v == 21 { Err("boom") } else { Ok(v) });
        assert_eq!(err_res.unwrap_err(), "boom");
    }
}
