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
pub trait Op: Send + Sync {
    fn symbol(&self) -> &str;
}

#[derive(Debug, Clone)]
pub struct OpBase {
    symbol: String,
}

impl OpBase {
    pub fn new(symbol: impl Into<String>) -> Self {
        Self { symbol: symbol.into() }
    }
    pub fn symbol(&self) -> &str {
        &self.symbol
    }
}

#[cfg(test)]
mod tests {
    use crate::common::filter::op::OpBase;

    #[test]
    fn create_new_op_base() {
        let op = OpBase::new("+");
        assert_eq!(op.symbol(), "+");
    }
}
