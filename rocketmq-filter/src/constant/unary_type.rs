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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UnaryType {
    Negate,
    In,
    Not,
    BooleanCast,
    Like,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unary_type_debug() {
        assert_eq!(format!("{:?}", UnaryType::Negate), "Negate");
        assert_eq!(format!("{:?}", UnaryType::In), "In");
        assert_eq!(format!("{:?}", UnaryType::Not), "Not");
        assert_eq!(format!("{:?}", UnaryType::BooleanCast), "BooleanCast");
        assert_eq!(format!("{:?}", UnaryType::Like), "Like");
    }

    #[test]
    fn test_unary_type_clone_and_copy() {
        let original = UnaryType::Not;
        let cloned = original;
        assert_eq!(original, cloned);

        let original = UnaryType::Like;
        let copied = original;
        assert_eq!(original, copied);
    }
}
