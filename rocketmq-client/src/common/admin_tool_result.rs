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

use crate::common::admin_tools_result_code_enum::AdminToolsResultCodeEnum;

pub struct AdminToolResult<T> {
    success: bool,
    code: i32,
    error_msg: String,
    data: Option<T>,
}

#[allow(dead_code)]
impl<T> AdminToolResult<T> {
    pub fn new(success: bool, code: i32, error_msg: String, data: Option<T>) -> Self {
        Self {
            success,
            code,
            error_msg,
            data,
        }
    }

    pub fn success(data: T) -> Self {
        Self::new(
            true,
            AdminToolsResultCodeEnum::Success.get_code(),
            "success".to_string(),
            Some(data),
        )
    }

    pub fn failure(error_code_enum: AdminToolsResultCodeEnum, error_msg: String) -> Self {
        Self::new(false, error_code_enum.get_code(), error_msg, None)
    }

    pub fn failure_with_data(error_code_enum: AdminToolsResultCodeEnum, error_msg: String, data: T) -> Self {
        Self::new(false, error_code_enum.get_code(), error_msg, Some(data))
    }

    #[inline]
    pub fn is_success(&self) -> bool {
        self.success
    }

    #[inline]
    pub fn set_success(&mut self, success: bool) {
        self.success = success;
    }

    #[inline]
    pub fn get_code(&self) -> i32 {
        self.code
    }

    #[inline]
    pub fn set_code(&mut self, code: i32) {
        self.code = code;
    }

    #[inline]
    pub fn get_error_msg(&self) -> &str {
        &self.error_msg
    }

    #[inline]
    pub fn set_error_msg(&mut self, error_msg: String) {
        self.error_msg = error_msg;
    }

    #[inline]
    pub fn get_data(&self) -> Option<&T> {
        self.data.as_ref()
    }

    #[inline]
    pub fn set_data(&mut self, data: T) {
        self.data = Some(data);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::admin_tools_result_code_enum::AdminToolsResultCodeEnum;

    #[test]
    fn new_initializes_correctly() {
        let result = AdminToolResult::new(true, 200, "success".to_string(), Some(42));
        assert!(result.is_success());
        assert_eq!(result.get_code(), 200);
        assert_eq!(result.get_error_msg(), "success");
        assert_eq!(result.get_data(), Some(&42));
    }

    #[test]
    fn success_initializes_correctly() {
        let result = AdminToolResult::success(42);
        assert!(result.is_success());
        assert_eq!(result.get_code(), 200);
        assert_eq!(result.get_error_msg(), "success");
        assert_eq!(result.get_data(), Some(&42));
    }

    #[test]
    fn failure_initializes_correctly() {
        let result = AdminToolResult::<i32>::failure(AdminToolsResultCodeEnum::RemotingError, "error".to_string());
        assert!(!result.is_success());
        assert_eq!(result.get_code(), -1001);
        assert_eq!(result.get_error_msg(), "error");
        assert!(result.get_data().is_none());
    }

    #[test]
    fn failure_with_data_initializes_correctly() {
        let result =
            AdminToolResult::failure_with_data(AdminToolsResultCodeEnum::RemotingError, "error".to_string(), 42);
        assert!(!result.is_success());
        assert_eq!(result.get_code(), -1001);
        assert_eq!(result.get_error_msg(), "error");
        assert_eq!(result.get_data(), Some(&42));
    }

    #[test]
    fn set_success_updates_success() {
        let mut result = AdminToolResult::new(false, 200, "success".to_string(), Some(42));
        result.set_success(true);
        assert!(result.is_success());
    }

    #[test]
    fn set_code_updates_code() {
        let mut result = AdminToolResult::new(true, 200, "success".to_string(), Some(42));
        result.set_code(-1001);
        assert_eq!(result.get_code(), -1001);
    }

    #[test]
    fn set_error_msg_updates_error_msg() {
        let mut result = AdminToolResult::new(true, 200, "success".to_string(), Some(42));
        result.set_error_msg("new error".to_string());
        assert_eq!(result.get_error_msg(), "new error");
    }

    #[test]
    fn set_data_updates_data() {
        let mut result = AdminToolResult::new(true, 200, "success".to_string(), Some(42));
        result.set_data(43);
        assert_eq!(result.get_data(), Some(&43));
    }
}
