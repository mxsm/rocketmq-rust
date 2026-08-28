use std::collections::HashMap;

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::command_custom_header::CommandCustomHeader;
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmptyHeader {}

impl CommandCustomHeader for EmptyHeader {
    fn to_map(&self) -> Option<HashMap<CheetahString, CheetahString>> {
        Some(HashMap::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    #[test]
    fn test_empty_header_lifecycle() {
        let header = EmptyHeader::default();

        let json = serde_json::to_string(&header).unwrap();
        assert_eq!(json, "{}");
        let de: EmptyHeader = serde_json::from_str(&json).unwrap();
        assert_eq!(header, de);
        assert_eq!(header.to_map(), Some(HashMap::new()));
    }
}
