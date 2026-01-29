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

use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::path::PathBuf;

use cheetah_string::CheetahString;

pub const ACCESS_KEY: &str = "AccessKey";
pub const SECRET_KEY: &str = "SecretKey";
pub const SIGNATURE: &str = "Signature";
pub const SECURITY_TOKEN: &str = "SecurityToken";

fn get_key_file_path() -> PathBuf {
    if let Ok(key_file) = std::env::var("rocketmq.client.keyFile") {
        PathBuf::from(key_file)
    } else if let Ok(home) = std::env::var("HOME").or_else(|_| std::env::var("USERPROFILE")) {
        PathBuf::from(home).join("key")
    } else {
        PathBuf::from("key")
    }
}

#[derive(Debug, Clone, Default)]
pub struct SessionCredentials {
    access_key: Option<CheetahString>,
    secret_key: Option<CheetahString>,
    security_token: Option<CheetahString>,
    signature: Option<CheetahString>,
}

impl SessionCredentials {
    pub fn new() -> Self {
        let mut credentials = Self::default();
        credentials.load_from_file();
        credentials
    }

    pub fn with_keys(access_key: impl Into<CheetahString>, secret_key: impl Into<CheetahString>) -> Self {
        Self {
            access_key: Some(access_key.into()),
            secret_key: Some(secret_key.into()),
            security_token: None,
            signature: None,
        }
    }

    pub fn with_token(
        access_key: impl Into<CheetahString>,
        secret_key: impl Into<CheetahString>,
        security_token: impl Into<CheetahString>,
    ) -> Self {
        Self {
            access_key: Some(access_key.into()),
            secret_key: Some(secret_key.into()),
            security_token: Some(security_token.into()),
            signature: None,
        }
    }

    fn load_from_file(&mut self) {
        let key_file = get_key_file_path();
        if let Ok(content) = fs::read_to_string(key_file) {
            if let Ok(properties) = self.parse_properties(&content) {
                self.update_content(&properties);
            }
        }
    }

    fn parse_properties(&self, content: &str) -> Result<HashMap<String, String>, ()> {
        let mut properties = HashMap::new();
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if let Some(pos) = line.find('=') {
                let key = line[..pos].trim().to_string();
                let value = line[pos + 1..].trim().to_string();
                properties.insert(key, value);
            }
        }
        Ok(properties)
    }

    pub fn update_content(&mut self, properties: &HashMap<String, String>) {
        if let Some(value) = properties.get(ACCESS_KEY) {
            self.access_key = Some(CheetahString::from_string(value.trim().to_string()));
        }
        if let Some(value) = properties.get(SECRET_KEY) {
            self.secret_key = Some(CheetahString::from_string(value.trim().to_string()));
        }
        if let Some(value) = properties.get(SECURITY_TOKEN) {
            self.security_token = Some(CheetahString::from_string(value.trim().to_string()));
        }
    }

    pub fn access_key(&self) -> Option<&CheetahString> {
        self.access_key.as_ref()
    }

    pub fn set_access_key(&mut self, access_key: impl Into<CheetahString>) {
        self.access_key = Some(access_key.into());
    }

    pub fn secret_key(&self) -> Option<&CheetahString> {
        self.secret_key.as_ref()
    }

    pub fn set_secret_key(&mut self, secret_key: impl Into<CheetahString>) {
        self.secret_key = Some(secret_key.into());
    }

    pub fn signature(&self) -> Option<&CheetahString> {
        self.signature.as_ref()
    }

    pub fn set_signature(&mut self, signature: impl Into<CheetahString>) {
        self.signature = Some(signature.into());
    }

    pub fn security_token(&self) -> Option<&CheetahString> {
        self.security_token.as_ref()
    }

    pub fn set_security_token(&mut self, security_token: impl Into<CheetahString>) {
        self.security_token = Some(security_token.into());
    }
}

impl PartialEq for SessionCredentials {
    fn eq(&self, other: &Self) -> bool {
        self.access_key == other.access_key && self.secret_key == other.secret_key && self.signature == other.signature
    }
}

impl Eq for SessionCredentials {}

impl std::hash::Hash for SessionCredentials {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.access_key.hash(state);
        self.secret_key.hash(state);
        self.signature.hash(state);
    }
}

impl fmt::Display for SessionCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SessionCredentials [accessKey={:?}, secretKey={:?}, signature={:?}, SecurityToken={:?}]",
            self.access_key, self.secret_key, self.signature, self.security_token
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_credentials_new_and_default() {
        let credentials = SessionCredentials::new();
        assert!(credentials.access_key().is_none());
        let credentials = SessionCredentials::default();
        assert!(credentials.secret_key().is_none());
    }

    #[test]
    fn session_credentials_with_keys() {
        let access_key = "ak";
        let secret_key = "sk";
        let credentials = SessionCredentials::with_keys(access_key, secret_key);
        assert_eq!(credentials.access_key().unwrap(), access_key);
        assert_eq!(credentials.secret_key().unwrap(), secret_key);
        assert!(credentials.security_token().is_none());
        assert!(credentials.signature().is_none());
    }

    #[test]
    fn session_credentials_with_token() {
        let access_key = "ak";
        let secret_key = "sk";
        let token = "token";
        let credentials = SessionCredentials::with_token(access_key, secret_key, token);
        assert_eq!(credentials.access_key().unwrap(), access_key);
        assert_eq!(credentials.secret_key().unwrap(), secret_key);
        assert_eq!(credentials.security_token().unwrap(), token);
        assert!(credentials.signature().is_none());
    }

    #[test]
    fn session_credentials_parse_properties() {
        let content = "AccessKey=ak123\nSecretKey=sk456\n#comment\n \nSecurityToken=tk789\nSignature=sig123";
        let credentials = SessionCredentials::default();
        let properties = credentials.parse_properties(content).unwrap();
        assert_eq!(properties.get("AccessKey").unwrap(), "ak123");
        assert_eq!(properties.get("SecretKey").unwrap(), "sk456");
        assert_eq!(properties.get("SecurityToken").unwrap(), "tk789");
        assert_eq!(properties.get("Signature").unwrap(), "sig123");
    }

    #[test]
    fn session_credentials_update_content() {
        let mut properties = HashMap::new();
        properties.insert(ACCESS_KEY.to_string(), "ak".to_string());
        properties.insert(SECRET_KEY.to_string(), "sk".to_string());
        properties.insert(SECURITY_TOKEN.to_string(), "token".to_string());

        let mut credentials = SessionCredentials::default();
        credentials.update_content(&properties);

        assert_eq!(credentials.access_key().unwrap(), "ak");
        assert_eq!(credentials.secret_key().unwrap(), "sk");
        assert_eq!(credentials.security_token().unwrap(), "token");
    }

    #[test]
    fn session_credentials_getters_and_setters() {
        let mut credentials = SessionCredentials::default();
        credentials.set_access_key("ak");
        credentials.set_secret_key("sk");
        credentials.set_security_token("token");
        credentials.set_signature("sig");

        assert_eq!(credentials.access_key().unwrap(), "ak");
        assert_eq!(credentials.secret_key().unwrap(), "sk");
        assert_eq!(credentials.security_token().unwrap(), "token");
        assert_eq!(credentials.signature().unwrap(), "sig");
    }

    #[test]
    fn session_credentials_eq_and_hash() {
        let c1 = SessionCredentials::with_keys("ak", "sk");
        let c2 = SessionCredentials::with_keys("ak", "sk");
        let c3 = SessionCredentials::with_keys("ak2", "sk");

        assert_eq!(c1, c2);
        assert_ne!(c1, c3);

        use std::collections::hash_map::DefaultHasher;
        use std::hash::Hash;
        use std::hash::Hasher;
        let mut h1 = DefaultHasher::new();
        c1.hash(&mut h1);
        let mut h2 = DefaultHasher::new();
        c2.hash(&mut h2);
        assert_eq!(h1.finish(), h2.finish());
    }

    #[test]
    fn session_credentials_display() {
        let credentials = SessionCredentials::with_token("ak", "sk", "tk");
        let display = format!("{}", credentials);
        let expected = "SessionCredentials [accessKey=Some(\"ak\"), secretKey=Some(\"sk\"), signature=None, \
                        SecurityToken=Some(\"tk\")]";
        assert_eq!(display, expected);
    }
}
