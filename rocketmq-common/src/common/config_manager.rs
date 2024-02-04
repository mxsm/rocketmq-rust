// Define the trait ConfigManager
pub trait ConfigManager {
    fn load(&mut self) -> bool;
    fn persist<T>(&mut self, topic_name: &str, t: T);
    fn persist_map<T>(&mut self, m: &HashMap<String, T>);
    fn persist(&mut self);
    fn decode0(&mut self, key: &[u8], body: &[u8]);
    fn stop(&mut self) -> bool;
    fn config_file_path(&mut self) -> &str;
    fn encode(&mut self) -> String;
    fn encode_pretty(&mut self, pretty_format: bool) -> String;
    fn decode(&mut self, json_string: &str);
}