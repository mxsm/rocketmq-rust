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

#[derive(Debug, Clone)]
pub struct Operand {
    op: OpBase,
}

impl Operand {
    pub fn new(symbol: &str) -> Self {
        Self {
            op: OpBase::new(symbol),
        }
    }
}

impl Op for Operand {
    fn symbol(&self) -> &str {
        self.op.symbol()
    }
}

#[cfg(test)]
mod tests {
    use crate::common::filter::op::Op;
    use crate::common::filter::operand::Operand;

    #[test]
    fn create_operand() {
        let operand = Operand::new("+");
        assert_eq!(operand.symbol(), "+");
    }

    #[test]
    fn create_operand_with_different_symbols() {
        let symbols = vec!["+", "-", "*", "/", "==", "!=", ">", "<"];
        for sym in symbols {
            let operand = Operand::new(sym);
            assert_eq!(operand.symbol(), sym);
        }
    }

    #[test]
    fn create_operand_with_empty_string() {
        let operand = Operand::new("");
        assert_eq!(operand.symbol(), "");
    }

    #[test]
    fn operand_clone_works() {
        let operand = Operand::new("+");
        let cloned = operand.clone();
        assert_eq!(cloned.symbol(), "+");
    }
}
