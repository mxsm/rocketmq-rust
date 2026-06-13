// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;

use rocketmq_remoting::protocol::remoting_command::RemotingCommand;

/// Java-compatible ACL exception type.
pub type AclException = rocketmq_error::AuthError;

/// Java-compatible ACL signing algorithm type.
pub type SigningAlgorithm = rocketmq_auth::SignatureAlgorithm;

/// Java-compatible ACL constants.
pub struct AclConstants;

impl AclConstants {
    pub const CONFIG_ACCESS_KEY: &'static str = "accessKey";
    pub const CONFIG_SECRET_KEY: &'static str = "secretKey";
    pub const PUB: &'static str = "PUB";
    pub const SUB: &'static str = "SUB";
    pub const DENY: &'static str = "DENY";
    pub const PUB_SUB: &'static str = "PUB|SUB";
    pub const SUB_PUB: &'static str = "SUB|PUB";
}

/// Java-compatible ACL permission bits.
pub struct Permission;

impl Permission {
    pub const DENY: u8 = 1;
    pub const ANY: u8 = 1 << 1;
    pub const PUB: u8 = 1 << 2;
    pub const SUB: u8 = 1 << 3;

    pub fn parse_perm_from_string(permission: Option<&str>) -> u8 {
        rocketmq_auth::permission::Permission::parse(permission).bits()
    }
}

/// Java-compatible ACL signer facade.
pub struct AclSigner;

impl AclSigner {
    pub const DEFAULT_CHARSET: &'static str = rocketmq_auth::authentication::acl_signer::DEFAULT_CHARSET;
    pub const DEFAULT_ALGORITHM: SigningAlgorithm = SigningAlgorithm::HmacSha1;

    pub fn cal_signature(data: &[u8], secret_key: &str) -> Result<String, AclException> {
        rocketmq_auth::authentication::acl_signer::cal_signature(data, secret_key)
    }

    pub fn cal_signature_str(data: &str, secret_key: &str) -> Result<String, AclException> {
        Self::cal_signature(data.as_bytes(), secret_key)
    }

    pub fn cal_signature_with_algorithm(
        data: &[u8],
        secret_key: &str,
        algorithm: SigningAlgorithm,
    ) -> Result<String, AclException> {
        rocketmq_auth::authentication::acl_signer::cal_signature_with_algorithm(data, secret_key, algorithm)
    }
}

/// Java-compatible ACL utility facade.
pub struct AclUtils;

impl AclUtils {
    fn net_address_scope_error(net_address: &str) -> AclException {
        AclException::Other(format!(
            "NetAddress examine scope Exception netAddress is {net_address}"
        ))
    }

    pub fn combine_request_content(request: &RemotingCommand, fields: &BTreeMap<String, String>) -> Vec<u8> {
        let value_len = fields
            .iter()
            .filter(|(key, _)| key.as_str() != "Signature")
            .map(|(_, value)| value.len())
            .sum::<usize>();
        let body_len = request.body().map_or(0, |body| body.len());
        let mut content = Vec::with_capacity(value_len + body_len);

        for (key, value) in fields {
            if key.as_str() != "Signature" {
                content.extend_from_slice(value.as_bytes());
            }
        }
        if let Some(body) = request.body() {
            content.extend_from_slice(body);
        }

        content
    }

    pub fn combine_bytes(first: Option<&[u8]>, second: Option<&[u8]>) -> Vec<u8> {
        match (
            first.filter(|bytes| !bytes.is_empty()),
            second.filter(|bytes| !bytes.is_empty()),
        ) {
            (None, None) => Vec::new(),
            (Some(first), None) => first.to_vec(),
            (None, Some(second)) => second.to_vec(),
            (Some(first), Some(second)) => {
                let mut combined = Vec::with_capacity(first.len() + second.len());
                combined.extend_from_slice(first);
                combined.extend_from_slice(second);
                combined
            }
        }
    }

    pub fn cal_signature(data: &[u8], secret_key: &str) -> Result<String, AclException> {
        AclSigner::cal_signature(data, secret_key)
    }

    pub fn ipv6_address_check(net_address: &str) -> Result<(), AclException> {
        if !Self::is_asterisk(net_address) && !Self::is_minus(net_address) {
            return Ok(());
        }

        let asterisk = net_address.find('*');
        let minus = net_address.find('-');
        if let Some(asterisk) = asterisk {
            if asterisk != net_address.len() - 1 {
                return Err(Self::net_address_scope_error(net_address));
            }
        }

        let Some(minus) = minus else {
            return Ok(());
        };
        match asterisk {
            None => {
                if let Some(last_colon) = net_address.rfind(':') {
                    if minus <= last_colon {
                        return Err(Self::net_address_scope_error(net_address));
                    }
                }
            }
            Some(_) => {
                let Some(last_colon) = net_address.rfind(':') else {
                    return Ok(());
                };
                if let Some(second_last_colon) = net_address[..last_colon].rfind(':') {
                    if minus <= second_last_colon {
                        return Err(Self::net_address_scope_error(net_address));
                    }
                }
            }
        }

        Ok(())
    }

    pub fn v6ip_process(net_address: &str) -> String {
        let is_asterisk = Self::is_asterisk(net_address);
        let is_minus = Self::is_minus(net_address);
        let (sub_address, part) = if is_asterisk && is_minus {
            let last_colon = net_address.rfind(':').unwrap_or(net_address.len());
            let second_last_colon = net_address[..last_colon].rfind(':').unwrap_or(0);
            (&net_address[..second_last_colon], 6)
        } else if !is_asterisk && !is_minus {
            (net_address, 8)
        } else {
            let last_colon = net_address.rfind(':').unwrap_or(net_address.len());
            (&net_address[..last_colon], 7)
        };
        Self::expand_ip(sub_address, part)
    }

    pub fn verify(net_address: &str, index: usize) -> Result<(), AclException> {
        if Self::is_scope_address(net_address, index) {
            Ok(())
        } else {
            Err(Self::net_address_scope_error(net_address))
        }
    }

    pub fn is_scope_address(net_address: &str, index: usize) -> bool {
        if Self::is_colon(net_address) {
            let net_address = Self::expand_ip(net_address, 8);
            let parts = net_address
                .split(':')
                .filter(|part| !part.is_empty())
                .collect::<Vec<_>>();
            return Self::is_ipv6_scope_parts(&parts, index);
        }

        let parts = net_address
            .split('.')
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>();
        parts.len() == 4 && Self::is_scope_parts(&parts, index)
    }

    pub fn is_scope_parts(parts: &[&str], index: usize) -> bool {
        parts
            .iter()
            .take(index)
            .all(|part| part.trim().parse::<i32>().is_ok_and(Self::is_scope_value))
    }

    pub fn is_colon(net_address: &str) -> bool {
        net_address.contains(':')
    }

    pub fn is_scope_value(num: i32) -> bool {
        (0..=255).contains(&num)
    }

    pub fn is_asterisk(asterisk: &str) -> bool {
        asterisk.contains('*')
    }

    pub fn is_comma(comma: &str) -> bool {
        comma.contains(',')
    }

    pub fn is_minus(minus: &str) -> bool {
        minus.contains('-')
    }

    pub fn is_ipv6_scope_parts(parts: &[&str], index: usize) -> bool {
        parts.iter().take(index).all(|part| {
            i32::from_str_radix(part, 16)
                .map(Self::is_ipv6_scope_value)
                .unwrap_or(false)
        })
    }

    pub fn is_ipv6_scope_value(num: i32) -> bool {
        (0..=0xffff).contains(&num)
    }

    pub fn expand_ip(net_address: &str, part: usize) -> String {
        let mut net_address = net_address.to_uppercase();
        let separator_count = net_address.matches(':').count();
        if part > separator_count {
            let pad_str = ":".repeat(part - separator_count + 1);
            net_address = net_address.replace("::", &pad_str);
        }

        net_address
            .split(':')
            .map(|part| {
                if part.len() < 4 {
                    format!("{part:0>4}")
                } else {
                    part.to_string()
                }
            })
            .collect::<Vec<_>>()
            .join(":")
    }

    #[allow(non_snake_case)]
    pub fn IPv6AddressCheck(net_address: &str) -> Result<(), AclException> {
        Self::ipv6_address_check(net_address)
    }

    #[allow(non_snake_case)]
    pub fn v6ipProcess(net_address: &str) -> String {
        Self::v6ip_process(net_address)
    }

    #[allow(non_snake_case)]
    pub fn expandIP(net_address: &str, part: usize) -> String {
        Self::expand_ip(net_address, part)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    #[test]
    fn acl_constants_match_java_values() {
        assert_eq!(AclConstants::CONFIG_ACCESS_KEY, "accessKey");
        assert_eq!(AclConstants::CONFIG_SECRET_KEY, "secretKey");
        assert_eq!(AclConstants::PUB, "PUB");
        assert_eq!(AclConstants::SUB, "SUB");
        assert_eq!(AclConstants::DENY, "DENY");
        assert_eq!(AclConstants::PUB_SUB, "PUB|SUB");
        assert_eq!(AclConstants::SUB_PUB, "SUB|PUB");
    }

    #[test]
    fn permission_parse_perm_from_string_matches_java_bits() {
        assert_eq!(Permission::parse_perm_from_string(None), Permission::DENY);
        assert_eq!(Permission::parse_perm_from_string(Some("PUB")), Permission::PUB);
        assert_eq!(Permission::parse_perm_from_string(Some("SUB")), Permission::SUB);
        assert_eq!(
            Permission::parse_perm_from_string(Some("PUB|SUB")),
            Permission::PUB | Permission::SUB
        );
        assert_eq!(
            Permission::parse_perm_from_string(Some("SUB|PUB")),
            Permission::PUB | Permission::SUB
        );
        assert_eq!(Permission::parse_perm_from_string(Some("UNKNOWN")), Permission::DENY);
    }

    #[test]
    fn acl_signer_matches_java_hmac_sha1_base64() {
        assert_eq!(
            AclSigner::cal_signature_str("alicetopic-a", "secret").unwrap(),
            "3yomro7y0WqWcbV+9tQa4av5w3Q="
        );
        assert_eq!(
            AclSigner::cal_signature_with_algorithm(b"alicetopic-a", "secret", SigningAlgorithm::HmacSha256).unwrap(),
            "+VoSl/q1CHGyZYyeI+H1C8SK6N+inDouZi6grscuA/Q="
        );
        assert_eq!(AclSigner::DEFAULT_ALGORITHM, SigningAlgorithm::HmacSha1);
        assert_eq!(AclSigner::DEFAULT_CHARSET, "UTF-8");
    }

    #[test]
    fn acl_utils_combine_bytes_matches_java_null_and_empty_rules() {
        assert_eq!(AclUtils::combine_bytes(None, None), b"");
        assert_eq!(AclUtils::combine_bytes(Some(b""), Some(b"body")), b"body");
        assert_eq!(AclUtils::combine_bytes(Some(b"head"), Some(b"")), b"head");
        assert_eq!(AclUtils::combine_bytes(Some(b"head"), Some(b"body")), b"headbody");
    }

    #[test]
    fn acl_utils_combine_request_content_uses_sorted_values_and_skips_signature_like_java() {
        let request = RemotingCommand::create_remoting_command(10).set_body(Bytes::from_static(b"body"));
        let fields = BTreeMap::from([
            ("Signature".to_string(), "stale".to_string()),
            ("a".to_string(), "first".to_string()),
            ("z".to_string(), "last".to_string()),
        ]);

        assert_eq!(AclUtils::combine_request_content(&request, &fields), b"firstlastbody");
    }

    #[test]
    fn acl_utils_expand_ip_matches_java_padding_rules() {
        assert_eq!(
            AclUtils::expand_ip("2::ac5:78", 8),
            "0002:0000:0000:0000:0000:0000:0AC5:0078"
        );
        assert_eq!(AclUtils::expandIP("2::ac5:78", 6), "0002:0000:0000:0000:0AC5:0078");
        assert_eq!(
            AclUtils::expand_ip("2001:db8::1", 8),
            "2001:0DB8:0000:0000:0000:0000:0000:0001"
        );
    }

    #[test]
    fn acl_utils_v6ip_process_matches_java_wildcard_and_range_rules() {
        assert_eq!(
            AclUtils::v6ip_process("2::ac5:78:1-200:*"),
            "0002:0000:0000:0000:0AC5:0078"
        );
        assert_eq!(
            AclUtils::v6ipProcess("2::ac5:78:1-200"),
            "0002:0000:0000:0000:0000:0AC5:0078"
        );
        assert_eq!(
            AclUtils::v6ip_process("2::ac5:78:*"),
            "0002:0000:0000:0000:0000:0AC5:0078"
        );
    }

    #[test]
    fn acl_utils_ipv6_address_check_matches_java_scope_rules() {
        assert!(AclUtils::ipv6_address_check("2::ac5:78:1-200:*").is_ok());
        assert!(AclUtils::ipv6_address_check("2::ac5:78:1-200").is_ok());
        assert!(AclUtils::IPv6AddressCheck("2::ac5:78:*").is_ok());
        assert!(AclUtils::ipv6_address_check("2::ac5:78:*:1-200").is_err());
        assert!(AclUtils::ipv6_address_check("2::ac5:1-200:78:*").is_err());
        assert!(AclUtils::ipv6_address_check("2::ac5:1-200:78").is_err());
    }

    #[test]
    fn acl_utils_verify_returns_typed_error_for_out_of_scope_addresses() {
        assert!(AclUtils::verify("192.168.0.1", 4).is_ok());
        assert!(AclUtils::verify("192.168.256.1", 4).is_err());
        assert!(AclUtils::verify("2::ac5:78", 8).is_ok());
        assert!(AclUtils::verify("GG::1", 8).is_err());
    }
}
