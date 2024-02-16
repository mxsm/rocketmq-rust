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
#[derive(Debug, PartialEq, Copy, Clone, Default)]
pub enum MessageType {
    #[default]
    NormalMsg,
    TransMsgHalf,
    TransMsgCommit,
    DelayMsg,
    OrderMsg,
}

impl MessageType {
    pub fn get_short_name(&self) -> &'static str {
        match self {
            MessageType::NormalMsg => "Normal",
            MessageType::TransMsgHalf => "Trans",
            MessageType::TransMsgCommit => "TransCommit",
            MessageType::DelayMsg => "Delay",
            MessageType::OrderMsg => "Order",
        }
    }

    pub fn get_by_short_name(short_name: &str) -> MessageType {
        match short_name {
            "Normal" => MessageType::NormalMsg,
            "Trans" => MessageType::TransMsgHalf,
            "TransCommit" => MessageType::TransMsgCommit,
            "Delay" => MessageType::DelayMsg,
            "Order" => MessageType::OrderMsg,
            _ => MessageType::NormalMsg,
        }
    }
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum MessageRequestMode {
    Pull,
    Pop,
}

impl MessageRequestMode {
    pub fn get_name(&self) -> &'static str {
        match self {
            MessageRequestMode::Pull => "PULL",
            MessageRequestMode::Pop => "POP",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_short_name() {
        assert_eq!(MessageType::NormalMsg.get_short_name(), "Normal");
        assert_eq!(MessageType::TransMsgHalf.get_short_name(), "Trans");
        assert_eq!(MessageType::TransMsgCommit.get_short_name(), "TransCommit");
        assert_eq!(MessageType::DelayMsg.get_short_name(), "Delay");
        assert_eq!(MessageType::OrderMsg.get_short_name(), "Order");
    }

    #[test]
    fn test_get_by_short_name() {
        assert_eq!(
            MessageType::get_by_short_name("Normal"),
            MessageType::NormalMsg
        );
        assert_eq!(
            MessageType::get_by_short_name("Trans"),
            MessageType::TransMsgHalf
        );
        assert_eq!(
            MessageType::get_by_short_name("TransCommit"),
            MessageType::TransMsgCommit
        );
        assert_eq!(
            MessageType::get_by_short_name("Delay"),
            MessageType::DelayMsg
        );
        assert_eq!(
            MessageType::get_by_short_name("Order"),
            MessageType::OrderMsg
        );
        assert_eq!(
            MessageType::get_by_short_name("Invalid"),
            MessageType::NormalMsg
        );
    }

    #[test]
    fn test_get_name() {
        assert_eq!(MessageRequestMode::Pull.get_name(), "PULL");
        assert_eq!(MessageRequestMode::Pop.get_name(), "POP");
    }
}
