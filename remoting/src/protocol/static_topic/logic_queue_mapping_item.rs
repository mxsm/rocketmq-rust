use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogicQueueMappingItem {
    pub gen: i32, // Immutable
    #[serde(rename = "queueId")]
    pub queue_id: i32, // Immutable
    pub bname: Option<String>, // Immutable
    #[serde(rename = "logicOffset")]
    pub logic_offset: i64, /* Start of the logic offset, Important, can be changed by command
                   * only once */
    #[serde(rename = "startOffset")]
    pub start_offset: i64, // Start of the physical offset, Should always be 0, Immutable
    #[serde(rename = "endOffset")]
    pub end_offset: i64, // End of the physical offset, Excluded, Default to -1, Mutable
    #[serde(rename = "timeOfStart")]
    pub time_of_start: i64, // Mutable, Reserved
    #[serde(rename = "timeOfEnd")]
    pub time_of_end: i64, // Mutable, Reserved
}

impl Default for LogicQueueMappingItem {
    fn default() -> Self {
        Self {
            gen: 0,
            queue_id: 0,
            bname: None,
            logic_offset: 0,
            start_offset: 0,
            end_offset: -1,
            time_of_start: -1,
            time_of_end: -1,
        }
    }
}
