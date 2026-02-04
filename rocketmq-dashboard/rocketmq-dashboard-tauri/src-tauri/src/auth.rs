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

use serde::Deserialize;
use serde::Serialize;
use std::fs;
use std::path::PathBuf;
use tauri::AppHandle;
use tauri::Manager;

const CONFIG_FILE: &str = "auth_config.json";
const DEFAULT_USERNAME: &str = "admin";
const DEFAULT_PASSWORD: &str = "admin123";

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AuthConfig {
    pub username: String,
    pub password_hash: String,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            username: DEFAULT_USERNAME.to_string(),
            password_hash: hash_password(DEFAULT_PASSWORD),
        }
    }
}

impl AuthConfig {
    /// Load authentication configuration from file or create default
    pub fn load(app: &AppHandle) -> Result<Self, Box<dyn std::error::Error>> {
        let config_path = get_config_path(app)?;

        if config_path.exists() {
            let content = fs::read_to_string(&config_path)?;
            let config: AuthConfig = serde_json::from_str(&content)?;
            Ok(config)
        } else {
            // Create default configuration
            let config = AuthConfig::default();
            config.save(app)?;
            log::info!("Created default auth configuration at: {:?}", config_path);
            log::info!(
                "Default credentials - Username: {}, Password: {}",
                DEFAULT_USERNAME,
                DEFAULT_PASSWORD
            );
            Ok(config)
        }
    }

    /// Save configuration to file
    pub fn save(&self, app: &AppHandle) -> Result<(), Box<dyn std::error::Error>> {
        let config_path = get_config_path(app)?;

        // Ensure parent directory exists
        if let Some(parent) = config_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let content = serde_json::to_string_pretty(self)?;
        fs::write(&config_path, content)?;
        log::info!("Auth configuration saved to: {:?}", config_path);
        Ok(())
    }

    /// Verify username and password
    pub fn verify(&self, username: &str, password: &str) -> bool {
        self.username == username && self.password_hash == hash_password(password)
    }

    /// Update password
    #[allow(dead_code)]
    pub fn update_password(&mut self, new_password: &str) {
        self.password_hash = hash_password(new_password);
    }
}

/// Get configuration file path
fn get_config_path(app: &AppHandle) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let app_config_dir = app
        .path()
        .app_config_dir()
        .map_err(|e| format!("Failed to get app config directory: {}", e))?;

    Ok(app_config_dir.join(CONFIG_FILE))
}

/// Simple password hashing (for production, use bcrypt or argon2)
fn hash_password(password: &str) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::Hash;
    use std::hash::Hasher;

    let mut hasher = DefaultHasher::new();
    password.hash(&mut hasher);
    format!("{:x}", hasher.finish())
}

#[derive(Serialize, Deserialize)]
pub struct LoginResponse {
    pub success: bool,
    pub message: String,
}

/// Tauri command to verify login credentials
#[tauri::command]
pub fn verify_login(username: String, password: String, auth_config: tauri::State<AuthConfig>) -> LoginResponse {
    log::info!("Login attempt for username: {}", username);

    if auth_config.verify(&username, &password) {
        log::info!("Login successful for username: {}", username);
        LoginResponse {
            success: true,
            message: "Login successful".to_string(),
        }
    } else {
        log::warn!("Login failed for username: {}", username);
        LoginResponse {
            success: false,
            message: "Invalid username or password".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_password_hash() {
        let password = "admin123";
        let hash1 = hash_password(password);
        let hash2 = hash_password(password);
        assert_eq!(hash1, hash2);

        let different_hash = hash_password("different");
        assert_ne!(hash1, different_hash);
    }

    #[test]
    fn test_verify_credentials() {
        let config = AuthConfig::default();
        assert!(config.verify(DEFAULT_USERNAME, DEFAULT_PASSWORD));
        assert!(!config.verify(DEFAULT_USERNAME, "wrong_password"));
        assert!(!config.verify("wrong_user", DEFAULT_PASSWORD));
    }

    #[test]
    fn test_update_password() {
        let mut config = AuthConfig::default();
        let new_password = "new_password123";

        config.update_password(new_password);
        assert!(config.verify(DEFAULT_USERNAME, new_password));
        assert!(!config.verify(DEFAULT_USERNAME, DEFAULT_PASSWORD));
    }
}
