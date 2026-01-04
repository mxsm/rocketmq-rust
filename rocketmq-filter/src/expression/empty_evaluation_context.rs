use crate::expression::EvaluationContext;
use cheetah_string::CheetahString;
use std::collections::HashMap;

pub struct EmptyEvaluationContext;

impl EvaluationContext for EmptyEvaluationContext {
    fn get(&self, _name: &str) -> Option<&CheetahString> {
        None
    }

    fn key_values(&self) -> Option<HashMap<CheetahString, CheetahString>> {
        None
    }
}
