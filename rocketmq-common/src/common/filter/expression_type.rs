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

pub struct ExpressionType;

impl ExpressionType {
    /// SQL92 expression type.
    pub const SQL92: &'static str = "SQL92";

    /// TAG expression type.
    pub const TAG: &'static str = "TAG";

    /// Checks if the given type is a TAG type.
    pub fn is_tag_type(type_: Option<&str>) -> bool {
        matches!(type_, None | Some("") | Some(ExpressionType::TAG))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expression_type() {
        assert_eq!(ExpressionType::SQL92, "SQL92");
        assert_eq!(ExpressionType::TAG, "TAG");
        assert!(ExpressionType::is_tag_type(None));
        assert!(ExpressionType::is_tag_type(Some("")));
        assert!(ExpressionType::is_tag_type(Some("TAG")));
        assert!(!ExpressionType::is_tag_type(Some("SQL92")));
        assert!(!ExpressionType::is_tag_type(Some("OTHER")));
    }
}
