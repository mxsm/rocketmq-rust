use crate::common::filter::op::Op;
use crate::common::filter::op::OpBase;

#[derive(Debug, Clone)]
pub struct Operand {
    op: OpBase,
}

impl Operand {
    pub fn new(symbol: &str) -> Self {
        Self { op: OpBase::new(symbol) }
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
}
