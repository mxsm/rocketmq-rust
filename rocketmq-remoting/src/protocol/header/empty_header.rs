use std::collections::HashMap;

use cheetah_string::CheetahString;

use crate::protocol::command_custom_header::CommandCustomHeader;

pub struct EmptyHeader {}

impl CommandCustomHeader for EmptyHeader {
    fn to_map(&self) -> Option<HashMap<CheetahString, CheetahString>> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_header_init() {
        let body = EmptyHeader {};
        assert!(matches!(body, EmptyHeader {}));
    }
    #[test]
    fn empty_header_map() {
        let body = EmptyHeader {};
        assert!(body.to_map().is_none());
    }
}
