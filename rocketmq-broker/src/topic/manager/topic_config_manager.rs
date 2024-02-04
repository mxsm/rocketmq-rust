use rocketmq_common::common::config_manager::ConfigManager;

pub(crate) struct TopicConfigManager {
    pub consumer_order_info_manager: String,
}

//Fully implemented will be removed
#[allow(unused_variables)]
impl ConfigManager for TopicConfigManager {
    fn decode0(&mut self, key: &[u8], body: &[u8]) {
        todo!()
    }

    fn stop(&mut self) -> bool {
        todo!()
    }

    fn config_file_path(&mut self) -> &str {
        todo!()
    }

    fn encode(&mut self) -> String {
        todo!()
    }

    fn encode_pretty(&mut self, pretty_format: bool) -> String {
        todo!()
    }

    fn decode(&mut self, json_string: &str) {
        todo!()
    }
}
