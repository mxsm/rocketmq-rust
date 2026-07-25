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

use crate::common::filter::op::Op;
use crate::common::filter::op::OpBase;
use std::cmp::Ordering;

#[derive(Debug, Clone)]
pub struct Operator {
    base: OpBase,
    priority: i32,
    comparable: bool,
}

impl Operator {
    fn new(symbol: impl Into<String>, priority: i32, comparable: bool) -> Self {
        Self {
            base: OpBase::new(symbol),
            priority,
            comparable,
        }
    }

    pub fn create_operator(operator: &str) -> Result<Self, String> {
        match operator {
            "(" => Ok(Self::new("(", 30, false)),
            ")" => Ok(Self::new(")", 30, false)),
            "&&" => Ok(Self::new("&&", 20, true)),
            "||" => Ok(Self::new("||", 15, true)),
            _ => Err(format!("unsupported operator: {}", operator)),
        }
    }

    pub fn priority(&self) -> i32 {
        self.priority
    }

    pub fn comparable(&self) -> bool {
        self.comparable
    }

    pub fn compare(&self, other: &Operator) -> Ordering {
        self.priority.cmp(&other.priority)
    }

    pub fn is_specified_op(&self, other: &str) -> bool {
        self.symbol() == other
    }
}

impl Op for Operator {
    fn symbol(&self) -> &str {
        self.base.symbol()
    }
}

#[cfg(test)]
mod tests {
    use crate::common::filter::operator::Operator;
    use std::cmp::Ordering;

    #[test]
    fn create_operator_error() {
        let result = Operator::create_operator("^");
        assert!(result.is_err());
    }

    #[test]
    fn compare_priorities() {
        let and = Operator::create_operator("&&").unwrap();
        let or = Operator::create_operator("||").unwrap();
        assert_eq!(and.compare(&or), Ordering::Greater);
        assert_eq!(and.compare(&and), Ordering::Equal);
        assert_eq!(or.compare(&and), Ordering::Less);
    }

    #[test]
    fn verify_specified_op() {
        let and = Operator::create_operator("&&").unwrap();
        assert!(and.is_specified_op("&&"));
    }

    #[test]
    fn verify_comparability() {
        assert!(!Operator::create_operator(")").unwrap().comparable());
        assert!(!Operator::create_operator("(").unwrap().comparable());
        assert!(Operator::create_operator("&&").unwrap().comparable());
        assert!(Operator::create_operator("||").unwrap().comparable());
    }

    #[test]
    fn verify_priorities() {
        let left = Operator::create_operator("(").unwrap();
        let right = Operator::create_operator(")").unwrap();
        let and = Operator::create_operator("&&").unwrap();
        let or = Operator::create_operator("||").unwrap();
        assert_eq!(left.priority(), right.priority());
        assert!(right.priority() > and.priority());
        assert!(and.priority() > or.priority());
    }
}
