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
