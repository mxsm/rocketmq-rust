/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

pub struct SerdeJsonUtils;

impl SerdeJsonUtils {
    pub fn decode<T>(bytes: &[u8]) -> rocketmq_error::RocketMQResult<T>
    where
        T: serde::de::DeserializeOwned,
    {
        Ok(serde_json::from_slice::<T>(bytes)?)
    }

    pub fn from_json_str<T>(json: &str) -> rocketmq_error::RocketMQResult<T>
    where
        T: serde::de::DeserializeOwned,
    {
        Ok(serde_json::from_str(json)?)
    }

    pub fn from_json_slice<T>(json: &[u8]) -> rocketmq_error::RocketMQResult<T>
    where
        T: serde::de::DeserializeOwned,
    {
        Ok(serde_json::from_slice(json)?)
    }

    pub fn to_json<T>(value: &T) -> rocketmq_error::RocketMQResult<String>
    where
        T: serde::Serialize,
    {
        Ok(serde_json::to_string(value)?)
    }

    pub fn to_json_pretty<T>(value: &T) -> rocketmq_error::RocketMQResult<String>
    where
        T: serde::Serialize,
    {
        Ok(serde_json::to_string_pretty(value)?)
    }

    pub fn to_json_vec<T>(value: &T) -> rocketmq_error::RocketMQResult<Vec<u8>>
    where
        T: serde::Serialize,
    {
        Ok(serde_json::to_vec(value)?)
    }

    pub fn to_json_vec_pretty<T>(value: &T) -> rocketmq_error::RocketMQResult<Vec<u8>>
    where
        T: serde::Serialize,
    {
        Ok(serde_json::to_vec_pretty(value)?)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use serde_json::Value;

    use super::*;

    #[test]
    fn from_json_returns_expected_result() {
        let json = r#"{"key": "value"}"#;
        let result: Result<Value, _> = SerdeJsonUtils::from_json_str(json);
        assert!(result.is_ok());
    }

    #[test]
    fn from_json_returns_error_for_invalid_json() {
        let json = "invalid";
        let result: Result<Value, _> = SerdeJsonUtils::from_json_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn from_json_slice_returns_expected_result() {
        let json = r#"{"key": "value"}"#.as_bytes();
        let result: Result<Value, _> = SerdeJsonUtils::from_json_slice(json);
        assert!(result.is_ok());
    }

    #[test]
    fn from_json_slice_returns_error_for_invalid_json() {
        let json = "invalid".as_bytes();
        let result: Result<Value, _> = SerdeJsonUtils::from_json_slice(json);
        assert!(result.is_err());
    }

    #[test]
    fn to_json_returns_expected_result() {
        let value = json!({"key": "value"});
        let result = SerdeJsonUtils::to_json(&value);
        assert!(result.is_ok());
    }

    #[test]
    fn to_json_pretty_returns_expected_result() {
        let value = json!({"key": "value"});
        let result = SerdeJsonUtils::to_json_pretty(&value);
        assert!(result.is_ok());
    }

    #[test]
    fn to_json_vec_returns_expected_result() {
        let value = json!({"key": "value"});
        let result = SerdeJsonUtils::to_json_vec(&value);
        assert!(result.is_ok());
    }

    #[test]
    fn to_json_vec_pretty_returns_expected_result() {
        let value = json!({"key": "value"});
        let result = SerdeJsonUtils::to_json_vec_pretty(&value);
        assert!(result.is_ok());
    }

    use std::fmt::Debug;

    use rocketmq_error::RocketMQError;
    use serde::Deserialize;
    use serde::Serialize;

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct TestStruct {
        name: String,
        age: u8,
    }

    #[test]
    fn test_from_json_success() {
        let json_str = r#"{"name":"Alice","age":30}"#;
        let expected = TestStruct {
            name: "Alice".to_string(),
            age: 30,
        };
        let result: TestStruct = SerdeJsonUtils::from_json_str(json_str).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_from_json_error() {
        let json_str = r#"{"name":"Alice","age":"thirty"}"#;
        let result: rocketmq_error::RocketMQResult<TestStruct> =
            SerdeJsonUtils::from_json_str(json_str);
        assert!(result.is_err());
    }

    #[test]
    fn test_from_json_slice_success() {
        let json_slice = r#"{"name":"Bob","age":25}"#.as_bytes();
        let expected = TestStruct {
            name: "Bob".to_string(),
            age: 25,
        };
        let result: TestStruct = SerdeJsonUtils::from_json_slice(json_slice).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_from_json_slice_error() {
        let json_slice = r#"{"name":"Bob","age":"twenty-five"}"#.as_bytes();
        let result: rocketmq_error::RocketMQResult<TestStruct> =
            SerdeJsonUtils::from_json_slice(json_slice);
        assert!(result.is_err());
    }

    #[test]
    fn test_to_json_success() {
        let value = TestStruct {
            name: "Charlie".to_string(),
            age: 40,
        };
        let expected = r#"{"name":"Charlie","age":40}"#;
        let result: String = SerdeJsonUtils::to_json(&value).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_to_json_error() {
        // This test is a bit tricky since `to_json` should not normally fail
        // unless there's a bug in `serde_json`. We can't really force an error
        // in a meaningful way, so we'll just ensure that the method returns a
        // `Result` and does not panic.
        let value = TestStruct {
            name: "Charlie".to_string(),
            age: 40,
        };
        let result: rocketmq_error::RocketMQResult<String> = SerdeJsonUtils::to_json(&value);
        assert!(result.is_ok());
    }
}
