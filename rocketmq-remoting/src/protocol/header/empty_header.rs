use std::collections::HashMap;

use cheetah_string::CheetahString;

use crate::protocol::command_custom_header::CommandCustomHeader;

pub struct EmptyHeader {}

impl CommandCustomHeader for EmptyHeader {
    fn to_map(&self) -> Option<HashMap<CheetahString, CheetahString>> {
        None
    }
}

