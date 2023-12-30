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

use std::collections::HashMap;

use anyhow::Error;
use serde::{Deserialize, Serialize};

use crate::protocol::command_custom_header::{CommandCustomHeader, FromMap};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GetRouteInfoRequestHeader {
    topic: String,

    #[serde(rename = "acceptStandardJsonOnly")]
    accept_standard_json_only: Option<bool>,
}

impl GetRouteInfoRequestHeader {
    pub fn new(topic: impl Into<String>, accept_standard_json_only: Option<bool>) -> Self {
        GetRouteInfoRequestHeader {
            topic: topic.into(),
            accept_standard_json_only,
        }
    }
}

impl FromMap for GetRouteInfoRequestHeader {
    type Target = GetRouteInfoRequestHeader;

    fn from(_map: &HashMap<String, String>) -> Option<Self::Target> {
        todo!()
    }
}

impl CommandCustomHeader for GetRouteInfoRequestHeader {
    fn check_fields(&self) -> anyhow::Result<(), Error> {
        //nothing needs to do.
        Ok(())
    }

    fn to_map(&self) -> Option<HashMap<String, String>> {
        let mut map = HashMap::with_capacity(2);
        map.insert(String::from("topic"), String::from(&self.topic));
        match self.accept_standard_json_only {
            None => {
                map.insert(
                    String::from("acceptStandardJsonOnly"),
                    String::from("false"),
                );
            }
            Some(val) => {
                map.insert(
                    String::from("acceptStandardJsonOnly"),
                    String::from(if val { "true" } else { "false" }),
                );
            }
        }
        Some(map)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::GetRouteInfoRequestHeader;
    use crate::protocol::command_custom_header::CommandCustomHeader;

    #[test]
    fn test_to_map_no_accept_standard_json_only() {
        let request_header = GetRouteInfoRequestHeader {
            topic: "test".into(),
            accept_standard_json_only: None,
        };

        let result: Option<HashMap<String, String>> = request_header.to_map();
        assert_eq!(
            result,
            Some(HashMap::from([
                (String::from("topic"), String::from("test")),
                (
                    String::from("acceptStandardJsonOnly"),
                    String::from("false")
                )
            ]))
        );
    }
}
