// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::net::IpAddr;
use std::str::FromStr;

use ipnetwork::IpNetwork;

/// Authorization environment model.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Environment {
    source_ips: Vec<String>,
}

impl Environment {
    /// Create from a single IP string (returns None on empty input)
    pub fn of(source_ip: &str) -> Option<Self> {
        if source_ip.trim().is_empty() {
            None
        } else {
            Some(Self {
                source_ips: vec![source_ip.to_string()],
            })
        }
    }

    /// Create from a list of IP strings (returns None on empty list)
    pub fn of_list<I, S>(source_ips: I) -> Option<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let v: Vec<String> = source_ips.into_iter().map(Into::into).collect();
        if v.is_empty() {
            None
        } else {
            Some(Self { source_ips: v })
        }
    }

    /// Returns true if this environment matches `other` according to rules:
    /// - if self.source_ips empty -> true
    /// - if other's source_ips empty -> false
    /// - target = other.source_ips[0]; check if any src pattern in self matches target
    pub fn is_match(&self, other: &Environment) -> bool {
        if self.source_ips.is_empty() {
            return true;
        }
        if other.source_ips.is_empty() {
            return false;
        }
        let target = &other.source_ips[0];
        for pattern in &self.source_ips {
            if is_ip_in_range(target, pattern) {
                return true;
            }
        }
        false
    }

    pub fn source_ips(&self) -> &Vec<String> {
        &self.source_ips
    }

    pub fn set_source_ips<I, S>(&mut self, ips: I)
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.source_ips = ips.into_iter().map(Into::into).collect();
    }
}

fn is_ip_in_range(target: &str, pattern: &str) -> bool {
    if target.trim().is_empty() || pattern.trim().is_empty() {
        return false;
    }

    // CIDR
    if pattern.contains('/') {
        if let (Ok(net), Ok(ip)) = (IpNetwork::from_str(pattern), IpAddr::from_str(target)) {
            return net.contains(ip);
        }
        return false;
    }

    // Range a-b
    if pattern.contains('-') {
        let parts: Vec<&str> = pattern.splitn(2, '-').collect();
        if parts.len() == 2 {
            if let (Ok(start), Ok(end), Ok(ip)) = (
                IpAddr::from_str(parts[0].trim()),
                IpAddr::from_str(parts[1].trim()),
                IpAddr::from_str(target),
            ) {
                return ip_in_range(ip, start, end);
            }
        }
        return false;
    }

    // exact match
    target == pattern
}

fn ip_to_u128(ip: IpAddr) -> u128 {
    match ip {
        IpAddr::V4(a) => u32::from(a) as u128,
        IpAddr::V6(b) => {
            let octets = b.octets();
            let mut acc = 0u128;
            for byte in &octets {
                acc = (acc << 8) | (*byte as u128);
            }
            acc
        }
    }
}

fn ip_in_range(ip: IpAddr, start: IpAddr, end: IpAddr) -> bool {
    match (ip, start, end) {
        (IpAddr::V4(a), IpAddr::V4(b), IpAddr::V4(c)) => {
            let a = u32::from(a);
            let b = u32::from(b);
            let c = u32::from(c);
            b <= a && a <= c
        }
        (IpAddr::V6(a), IpAddr::V6(b), IpAddr::V6(c)) => {
            let a = ip_to_u128(IpAddr::V6(a));
            let b = ip_to_u128(IpAddr::V6(b));
            let c = ip_to_u128(IpAddr::V6(c));
            b <= a && a <= c
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_of_and_list() {
        assert!(Environment::of("").is_none());
        assert!(Environment::of("127.0.0.1").is_some());
        assert!(Environment::of_list(Vec::<String>::new()).is_none());
        assert!(Environment::of_list(vec!["1.2.3.4"]).is_some());
    }

    #[test]
    fn test_exact_match() {
        let a = Environment::of("1.2.3.4").unwrap();
        let b = Environment::of("1.2.3.4").unwrap();
        assert!(a.is_match(&b));
        let c = Environment::of("1.2.3.5").unwrap();
        assert!(!a.is_match(&c));
    }

    #[test]
    fn test_cidr_match() {
        let a = Environment::of_list(vec!["192.168.0.0/16"]).unwrap();
        let b = Environment::of("192.168.1.5").unwrap();
        assert!(a.is_match(&b));
        let c = Environment::of("10.0.0.1").unwrap();
        assert!(!a.is_match(&c));
    }

    #[test]
    fn test_range_match() {
        let a = Environment::of_list(vec!["192.168.0.1-192.168.0.10"]).unwrap();
        assert!(a.is_match(&Environment::of("192.168.0.5").unwrap()));
        assert!(!a.is_match(&Environment::of("192.168.0.11").unwrap()));
    }

    #[test]
    fn test_empty_behavior() {
        let a = Environment::default();
        let b = Environment::of("1.2.3.4").unwrap();
        assert!(a.is_match(&b));
        let c = Environment::default();
        assert!(!b.is_match(&c));
    }
}
