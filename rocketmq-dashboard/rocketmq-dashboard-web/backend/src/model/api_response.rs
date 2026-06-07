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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ApiResponse<T> {
    pub success: bool,
    pub code: String,
    pub message: String,
    pub data: Option<T>,
}

impl<T> ApiResponse<T> {
    pub fn success(data: T) -> Self {
        Self {
            success: true,
            code: "OK".to_string(),
            message: "success".to_string(),
            data: Some(data),
        }
    }

    pub fn success_without_data() -> Self {
        Self {
            success: true,
            code: "OK".to_string(),
            message: "success".to_string(),
            data: None,
        }
    }
}

impl ApiResponse<serde_json::Value> {
    pub fn failure(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            success: false,
            code: code.into(),
            message: message.into(),
            data: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ApiResponse;

    #[test]
    fn success_response_uses_stable_shape() {
        let response = ApiResponse::success("UP");

        assert!(response.success);
        assert_eq!(response.code, "OK");
        assert_eq!(response.message, "success");
        assert_eq!(response.data, Some("UP"));
    }
}
