// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::collections::HashSet;

use cheetah_string::CheetahString;
use rocketmq_common::common::consumer::consume_from_where::ConsumeFromWhere;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::body::connection::Connection;
use crate::protocol::heartbeat::consume_type::ConsumeType;
use crate::protocol::heartbeat::message_model::MessageModel;
use crate::protocol::heartbeat::subscription_data::SubscriptionData;

/// Represents a consumer connection with subscription information.
///
/// This struct is serialization-compatible with Java's ConsumerConnection class.
/// It contains consumer connection details, subscription data, and consumption configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerConnection {
    /// Set of active connections from consumers
    connection_set: HashSet<Connection>,
    /// Topic subscription table (Topic -> SubscriptionData)
    #[serde(default)]
    subscription_table: HashMap<CheetahString, SubscriptionData>,
    /// Type of consumption (push/pull)
    #[serde(skip_serializing_if = "Option::is_none")]
    consume_type: Option<ConsumeType>,
    /// Message consumption model (clustering/broadcasting)
    #[serde(skip_serializing_if = "Option::is_none")]
    message_model: Option<MessageModel>,
    /// Position to start consuming from
    #[serde(skip_serializing_if = "Option::is_none")]
    consume_from_where: Option<ConsumeFromWhere>,
}

impl ConsumerConnection {
    /// Creates a new ConsumerConnection instance.
    pub fn new() -> Self {
        ConsumerConnection {
            connection_set: HashSet::new(),
            subscription_table: HashMap::new(),
            consume_type: None,
            message_model: None,
            consume_from_where: None,
        }
    }

    /// Computes the minimum version across all connections.
    ///
    /// This method iterates through all connections and finds the minimum version number.
    /// Returns `i32::MAX` if there are no connections.
    ///
    /// # Returns
    /// The minimum version number, or `i32::MAX` if connection set is empty
    pub fn compute_min_version(&self) -> i32 {
        let mut min_version = i32::MAX;
        for connection in &self.connection_set {
            let version = connection.get_version();
            if version < min_version {
                min_version = version;
            }
        }
        min_version
    }

    /// Gets a clone of the connection set.
    ///
    /// # Returns
    /// A cloned HashSet of all connections
    pub fn get_connection_set(&self) -> &HashSet<Connection> {
        &self.connection_set
    }

    /// Sets the connection set.
    ///
    /// # Arguments
    /// * `connection_set` - The new set of connections
    pub fn set_connection_set(&mut self, connection_set: HashSet<Connection>) {
        self.connection_set = connection_set;
    }

    /// Inserts a connection into the connection set.
    ///
    /// # Arguments
    /// * `connection` - The connection to insert
    ///
    /// # Returns
    /// `true` if the connection was newly inserted, `false` if it already existed
    pub fn insert_connection(&mut self, connection: Connection) -> bool {
        self.connection_set.insert(connection)
    }

    /// Gets a reference to the subscription table.
    ///
    /// # Returns
    /// Reference to the subscription table (Topic -> SubscriptionData mapping)
    pub fn get_subscription_table(&self) -> &HashMap<CheetahString, SubscriptionData> {
        &self.subscription_table
    }

    /// Gets a mutable reference to the subscription table.
    ///
    /// # Returns
    /// Mutable reference to the subscription table
    pub fn get_subscription_table_mut(&mut self) -> &mut HashMap<CheetahString, SubscriptionData> {
        &mut self.subscription_table
    }

    /// Sets the subscription table.
    ///
    /// # Arguments
    /// * `subscription_table` - The new subscription table
    pub fn set_subscription_table(&mut self, subscription_table: HashMap<CheetahString, SubscriptionData>) {
        self.subscription_table = subscription_table;
    }

    /// Gets the consume type.
    ///
    /// # Returns
    /// The consume type, or None if not set
    pub fn get_consume_type(&self) -> Option<ConsumeType> {
        self.consume_type
    }

    /// Sets the consume type.
    ///
    /// # Arguments
    /// * `consume_type` - The consume type (push/pull)
    pub fn set_consume_type(&mut self, consume_type: ConsumeType) {
        self.consume_type = Some(consume_type);
    }

    /// Gets the message model.
    ///
    /// # Returns
    /// The message model, or None if not set
    pub fn get_message_model(&self) -> Option<MessageModel> {
        self.message_model
    }

    /// Sets the message model.
    ///
    /// # Arguments
    /// * `message_model` - The message model (clustering/broadcasting)
    pub fn set_message_model(&mut self, message_model: MessageModel) {
        self.message_model = Some(message_model);
    }

    /// Gets the consume from where setting.
    ///
    /// # Returns
    /// The consume from where setting, or None if not set
    pub fn get_consume_from_where(&self) -> Option<ConsumeFromWhere> {
        self.consume_from_where
    }

    /// Sets the consume from where setting.
    ///
    /// # Arguments
    /// * `consume_from_where` - Where to start consuming from
    pub fn set_consume_from_where(&mut self, consume_from_where: ConsumeFromWhere) {
        self.consume_from_where = Some(consume_from_where);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_connection() -> Connection {
        let mut conn = Connection::new();
        conn.set_client_id(CheetahString::from_static_str("test_client_1"));
        conn.set_client_addr(CheetahString::from_static_str("127.0.0.1:9876"));
        conn.set_version(1);
        conn
    }

    fn create_test_subscription() -> SubscriptionData {
        SubscriptionData {
            topic: CheetahString::from_static_str("test_topic"),
            sub_string: CheetahString::from_static_str("*"),
            ..Default::default()
        }
    }

    #[test]
    fn consumer_connection_serializes_correctly() {
        let mut consumer_conn = ConsumerConnection::new();
        let conn = create_test_connection();
        consumer_conn.insert_connection(conn);
        consumer_conn.set_consume_type(ConsumeType::ConsumeActively);
        consumer_conn.set_message_model(MessageModel::Clustering);

        let json_str = serde_json::to_string(&consumer_conn).unwrap();
        assert!(json_str.contains("connectionSet"));
        assert!(json_str.contains("consumeType"));
        assert!(json_str.contains("messageModel"));
    }

    #[test]
    fn consumer_connection_deserializes_correctly() {
        let json_str = r#"{
            "connectionSet": [
                {
                    "clientId": "test_client",
                    "clientAddr": "127.0.0.1:9876",
                    "language": "RUST",
                    "version": 1
                }
            ],
            "subscriptionTable": {
                "test_topic": {
                    "classFilterMode": false,
                    "topic": "test_topic",
                    "subString": "*",
                    "tagsSet": [],
                    "codeSet": [],
                    "subVersion": 123456,
                    "expressionType": "TAG"
                }
            },
            "consumeType": "CONSUME_ACTIVELY",
            "messageModel": "CLUSTERING",
            "consumeFromWhere": "CONSUME_FROM_LAST_OFFSET"
        }"#;

        let consumer_conn: ConsumerConnection = serde_json::from_str(json_str).unwrap();
        assert_eq!(consumer_conn.get_connection_set().len(), 1);
        assert_eq!(consumer_conn.get_subscription_table().len(), 1);
        assert!(consumer_conn.get_consume_type().is_some());
        assert!(consumer_conn.get_message_model().is_some());
        assert!(consumer_conn.get_consume_from_where().is_some());
    }

    #[test]
    fn consumer_connection_default_and_new() {
        let consumer_conn = ConsumerConnection::default();
        assert!(consumer_conn.get_connection_set().is_empty());
        assert!(consumer_conn.get_subscription_table().is_empty());
        assert!(consumer_conn.get_consume_type().is_none());
        assert!(consumer_conn.get_message_model().is_none());
        assert!(consumer_conn.get_consume_from_where().is_none());

        let consumer_conn = ConsumerConnection::new();
        assert!(consumer_conn.get_connection_set().is_empty());
        assert!(consumer_conn.get_subscription_table().is_empty());
        assert!(consumer_conn.get_consume_type().is_none());
        assert!(consumer_conn.get_message_model().is_none());
        assert!(consumer_conn.get_consume_from_where().is_none());
    }

    #[test]
    fn consumer_connection_compute_min_version() {
        let consumer_conn = ConsumerConnection::new();
        assert_eq!(consumer_conn.compute_min_version(), i32::MAX);

        let mut consumer_conn = ConsumerConnection::new();
        let mut conn = Connection::new();
        conn.set_version(42);
        consumer_conn.insert_connection(conn);
        assert_eq!(consumer_conn.compute_min_version(), 42);

        let mut consumer_conn = ConsumerConnection::new();
        let mut conn1 = Connection::new();
        conn1.set_client_id(CheetahString::from_static_str("client1"));
        conn1.set_version(100);
        let mut conn2 = Connection::new();
        conn2.set_client_id(CheetahString::from_static_str("client2"));
        conn2.set_version(50);
        let mut conn3 = Connection::new();
        conn3.set_client_id(CheetahString::from_static_str("client3"));
        conn3.set_version(75);
        consumer_conn.insert_connection(conn1);
        consumer_conn.insert_connection(conn2);
        consumer_conn.insert_connection(conn3);
        assert_eq!(consumer_conn.compute_min_version(), 50);
    }

    #[test]
    fn consumer_connection_set_connection_set() {
        let mut consumer_conn = ConsumerConnection::new();
        let mut conn_set = HashSet::new();
        conn_set.insert(create_test_connection());

        consumer_conn.set_connection_set(conn_set.clone());
        assert_eq!(consumer_conn.get_connection_set().len(), 1);
    }

    #[test]
    fn consumer_connection_insert_connection() {
        let mut consumer_conn = ConsumerConnection::new();
        let conn = create_test_connection();

        let result = consumer_conn.insert_connection(conn.clone());
        assert!(result);

        let result = consumer_conn.insert_connection(conn.clone());
        assert!(!result);

        assert_eq!(consumer_conn.get_connection_set().len(), 1);
        assert!(consumer_conn.get_connection_set().contains(&conn));
    }

    #[test]
    fn consumer_connection_subscription_table_operations() {
        let mut consumer_conn = ConsumerConnection::new();
        let mut sub_table = HashMap::new();

        let sub_data = create_test_subscription();
        sub_table.insert(CheetahString::from_static_str("test_topic"), sub_data.clone());

        consumer_conn.set_subscription_table(sub_table);
        assert_eq!(consumer_conn.get_subscription_table().len(), 1);
        assert!(consumer_conn
            .get_subscription_table()
            .contains_key(&CheetahString::from_static_str("test_topic")));

        let mut consumer_conn = ConsumerConnection::new();
        let sub_data = create_test_subscription();

        {
            let sub_table = consumer_conn.get_subscription_table_mut();
            sub_table.insert(CheetahString::from_static_str("test_topic"), sub_data.clone());
        }

        assert_eq!(consumer_conn.get_subscription_table().len(), 1);
        let retrieved_sub = consumer_conn
            .get_subscription_table()
            .get(&CheetahString::from_static_str("test_topic"));
        assert!(retrieved_sub.is_some());
    }

    #[test]
    fn consumer_connection_consume_type_operations() {
        let mut consumer_conn = ConsumerConnection::new();
        assert!(consumer_conn.get_consume_type().is_none());

        consumer_conn.set_consume_type(ConsumeType::ConsumeActively);
        assert_eq!(consumer_conn.get_consume_type(), Some(ConsumeType::ConsumeActively));
    }

    #[test]
    fn consumer_connection_message_model_operations() {
        let mut consumer_conn = ConsumerConnection::new();
        assert!(consumer_conn.get_message_model().is_none());

        consumer_conn.set_message_model(MessageModel::Clustering);
        assert_eq!(consumer_conn.get_message_model(), Some(MessageModel::Clustering));
    }

    #[test]
    fn consumer_connection_consume_from_where_operations() {
        let mut consumer_conn = ConsumerConnection::new();
        assert!(consumer_conn.get_consume_from_where().is_none());

        consumer_conn.set_consume_from_where(ConsumeFromWhere::ConsumeFromLastOffset);
        assert_eq!(
            consumer_conn.get_consume_from_where(),
            Some(ConsumeFromWhere::ConsumeFromLastOffset)
        );
    }
}
