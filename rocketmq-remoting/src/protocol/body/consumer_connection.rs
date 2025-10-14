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

use std::collections::HashSet;
use std::sync::Arc;

use cheetah_string::CheetahString;
use dashmap::DashMap;
use parking_lot::RwLock;
use rocketmq_common::common::consumer::consume_from_where::ConsumeFromWhere;
use serde::ser::SerializeStruct;
use serde::Serialize;
use serde::Serializer;

use crate::protocol::body::connection::Connection;
use crate::protocol::heartbeat::consume_type::ConsumeType;
use crate::protocol::heartbeat::message_model::MessageModel;
use crate::protocol::heartbeat::subscription_data::SubscriptionData;

#[derive(Debug, Clone, Default)]
pub struct ConsumerConnection {
    connection_set: HashSet<Connection>,
    subscription_table: Arc<DashMap<CheetahString, SubscriptionData>>,
    consume_type: Arc<RwLock<ConsumeType>>,
    message_model: Arc<RwLock<MessageModel>>,
    consume_from_where: Arc<RwLock<ConsumeFromWhere>>,
}

impl ConsumerConnection {
    pub fn new() -> Self {
        ConsumerConnection {
            connection_set: HashSet::new(),
            subscription_table: Arc::new(DashMap::new()),
            consume_type: Arc::new(RwLock::new(ConsumeType::default())),
            message_model: Arc::new(RwLock::new(MessageModel::default())),
            consume_from_where: Arc::new(RwLock::new(ConsumeFromWhere::default())),
        }
    }
}

impl Serialize for ConsumerConnection {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_struct("ConsumerConnection", 5)?;
        s.serialize_field("connection_set", &self.connection_set)?;
        s.serialize_field("subscription_table", &*self.subscription_table)?;
        s.serialize_field("consume_type", &*self.consume_type.read())?;
        s.serialize_field("message_model", &*self.message_model.read())?;
        s.serialize_field("consume_from_where", &*self.consume_from_where.read())?;
        s.end()
    }
}

impl ConsumerConnection {
    pub fn get_connection_set(&self) -> HashSet<Connection> {
        self.connection_set.clone()
    }

    pub fn connection_set_insert(&mut self, connection: Connection) {
        self.connection_set.insert(connection);
    }

    pub fn set_connection_set(&mut self, connection_set: HashSet<Connection>) {
        self.connection_set = connection_set;
    }

    pub fn get_subscription_table(&self) -> Arc<DashMap<CheetahString, SubscriptionData>> {
        self.subscription_table.clone()
    }

    pub fn set_subscription_table(
        &mut self,
        subscription_table: DashMap<CheetahString, SubscriptionData>,
    ) {
        self.subscription_table = Arc::new(subscription_table);
    }

    pub fn get_consume_type(&self) -> ConsumeType {
        *self.consume_type.read()
    }

    pub fn set_consume_type(&mut self, consume_type: ConsumeType) {
        *self.consume_type.write() = consume_type;
    }

    pub fn get_message_model(&self) -> MessageModel {
        *self.message_model.read()
    }

    pub fn set_message_model(&mut self, message_model: MessageModel) {
        *self.message_model.write() = message_model;
    }

    pub fn get_consume_from_where(&self) -> ConsumeFromWhere {
        *self.consume_from_where.read()
    }

    pub fn set_consume_from_where(&mut self, consume_from_where: ConsumeFromWhere) {
        *self.consume_from_where.write() = consume_from_where;
    }
}
