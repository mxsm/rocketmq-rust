//  Licensed to the Apache Software Foundation (ASF) under one
//  or more contributor license agreements.  See the NOTICE file
//  distributed with this work for additional information
//  regarding copyright ownership.  The ASF licenses this file
//  to you under the Apache License, Version 2.0 (the
//  "License"); you may not use this file except in compliance
//  with the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an
//  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//  KIND, either express or implied.  See the License for the
//  specific language governing permissions and limitations
//  under the License.

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
    pub fn set_subscription_table(
        &mut self,
        subscription_table: HashMap<CheetahString, SubscriptionData>,
    ) {
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
