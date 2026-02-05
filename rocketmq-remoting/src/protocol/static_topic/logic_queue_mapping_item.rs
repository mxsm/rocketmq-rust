use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LogicQueueMappingItem {
    pub gen: i32, // Immutable
    #[serde(rename = "queueId")]
    pub queue_id: i32, // Immutable
    pub bname: Option<CheetahString>, // Immutable
    #[serde(rename = "logicOffset")]
    pub logic_offset: i64, // Start of the logic offset, Important, can be changed by command only once
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

impl LogicQueueMappingItem {
    pub fn compute_static_queue_offset_strictly(&self, physical_queue_offset: i64) -> i64 {
        if physical_queue_offset > self.start_offset {
            return self.logic_offset;
        }
        self.logic_offset + (physical_queue_offset - self.start_offset)
    }

    pub fn compute_static_queue_offset_loosely(&self, physical_queue_offset: i64) -> i64 {
        // Consider the newly mapped item
        if self.logic_offset < 0 {
            return -1;
        }
        if physical_queue_offset < self.start_offset {
            return self.logic_offset;
        }
        if self.end_offset >= self.start_offset && self.end_offset < physical_queue_offset {
            return self.logic_offset + (self.end_offset - self.start_offset);
        }
        self.logic_offset + (physical_queue_offset - self.start_offset)
    }

    pub fn compute_physical_queue_offset(&self, static_queue_offset: i64) -> i64 {
        self.start_offset + (static_queue_offset - self.logic_offset)
    }

    pub fn compute_offset_delta(&self) -> i64 {
        self.logic_offset - self.start_offset
    }

    pub fn check_if_end_offset_decided(&self) -> bool {
        self.end_offset > self.start_offset
    }

    pub fn compute_max_static_queue_offset(&self) -> i64 {
        if self.end_offset >= self.start_offset {
            self.logic_offset + self.end_offset - self.start_offset
        } else {
            self.logic_offset
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logic_queue_mapping_item_default() {
        let item = LogicQueueMappingItem::default();
        assert_eq!(item.gen, 0);
        assert_eq!(item.queue_id, 0);
        assert!(item.bname.is_none());
        assert_eq!(item.logic_offset, 0);
        assert_eq!(item.start_offset, 0);
        assert_eq!(item.end_offset, -1);
        assert_eq!(item.time_of_start, -1);
        assert_eq!(item.time_of_end, -1);
    }

    #[test]
    fn logic_queue_mapping_item_serde() {
        let item = LogicQueueMappingItem {
            gen: 1,
            queue_id: 2,
            bname: Some(CheetahString::from("broker-a")),
            logic_offset: 100,
            start_offset: 0,
            end_offset: 200,
            time_of_start: 123456,
            time_of_end: 789012,
        };
        let json = serde_json::to_string(&item).unwrap();
        let expected = r#"{"gen":1,"queueId":2,"bname":"broker-a","logicOffset":100,"startOffset":0,"endOffset":200,"timeOfStart":123456,"timeOfEnd":789012}"#;
        assert_eq!(json, expected);
        let deserialized: LogicQueueMappingItem = serde_json::from_str(&json).unwrap();
        assert_eq!(item, deserialized);
    }

    #[test]
    fn compute_static_queue_offset_strictly() {
        let item = LogicQueueMappingItem {
            start_offset: 100,
            logic_offset: 500,
            ..Default::default()
        };
        assert_eq!(item.compute_static_queue_offset_strictly(150), 500);
        assert_eq!(item.compute_static_queue_offset_strictly(50), 450);
    }

    #[test]
    fn compute_static_queue_offset_loosely() {
        let mut item = LogicQueueMappingItem {
            logic_offset: -1,
            ..Default::default()
        };
        assert_eq!(item.compute_static_queue_offset_loosely(100), -1);

        item.logic_offset = 500;
        item.start_offset = 100;
        assert_eq!(item.compute_static_queue_offset_loosely(50), 500);

        item.end_offset = 200;
        assert_eq!(item.compute_static_queue_offset_loosely(250), 600);

        assert_eq!(item.compute_static_queue_offset_loosely(150), 550);
    }

    #[test]
    fn compute_physical_queue_offset() {
        let item = LogicQueueMappingItem {
            start_offset: 100,
            logic_offset: 500,
            ..Default::default()
        };
        assert_eq!(item.compute_physical_queue_offset(550), 150);
    }

    #[test]
    fn compute_offset_delta() {
        let item = LogicQueueMappingItem {
            start_offset: 100,
            logic_offset: 500,
            ..Default::default()
        };
        assert_eq!(item.compute_offset_delta(), 400);
    }

    #[test]
    fn check_if_end_offset_decided() {
        let mut item = LogicQueueMappingItem {
            start_offset: 100,
            end_offset: 50,
            ..Default::default()
        };
        assert!(!item.check_if_end_offset_decided());
        item.end_offset = 150;
        assert!(item.check_if_end_offset_decided());
    }

    #[test]
    fn compute_max_static_queue_offset() {
        let mut item = LogicQueueMappingItem {
            logic_offset: 500,
            start_offset: 100,
            end_offset: 50,
            ..Default::default()
        };
        assert_eq!(item.compute_max_static_queue_offset(), 500);
        item.end_offset = 200;
        assert_eq!(item.compute_max_static_queue_offset(), 600);
    }
}
