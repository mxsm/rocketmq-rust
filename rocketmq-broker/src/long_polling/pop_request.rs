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
use std::fmt::Display;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_store::filter::MessageFilter;

pub struct PopRequest {
    remoting_command: RemotingCommand,
    ctx: ConnectionHandlerContext,
    complete: AtomicBool,
    op: i64,
    expired: u64,
    subscription_data: SubscriptionData,
    message_filter: Arc<Box<dyn MessageFilter>>,
}

impl PopRequest {
    pub fn new(
        remoting_command: RemotingCommand,
        ctx: ConnectionHandlerContext,
        expired: u64,
        subscription_data: SubscriptionData,
        message_filter: Arc<Box<dyn MessageFilter>>,
    ) -> Self {
        static COUNTER: AtomicI64 = AtomicI64::new(i64::MIN);
        let op = COUNTER.fetch_add(1, Ordering::SeqCst);

        PopRequest {
            remoting_command,
            ctx,
            complete: AtomicBool::new(false),
            op,
            expired,
            subscription_data,
            message_filter,
        }
    }

    //need to implement and optimize
    pub fn get_channel(&self) -> &Channel {
        unimplemented!("PopRequest::get_channel")
    }

    pub fn get_ctx(&self) -> &ConnectionHandlerContext {
        &self.ctx
    }

    pub fn get_remoting_command(&self) -> &RemotingCommand {
        &self.remoting_command
    }

    pub fn is_timeout(&self) -> bool {
        let now = get_current_millis();
        now > (self.expired - 50)
    }

    pub fn complete(&self) -> bool {
        self.complete
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .unwrap_or_default()
    }

    pub fn get_expired(&self) -> u64 {
        self.expired
    }

    pub fn get_subscription_data(&self) -> &SubscriptionData {
        &self.subscription_data
    }

    pub fn get_message_filter(&self) -> &Arc<Box<dyn MessageFilter>> {
        &self.message_filter
    }
}

impl PartialEq for PopRequest {
    fn eq(&self, other: &Self) -> bool {
        self.op == other.op
    }
}

impl Eq for PopRequest {}

impl PartialOrd for PopRequest {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PopRequest {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.expired
            .cmp(&other.expired)
            .then_with(|| self.op.cmp(&other.op))
    }
}

impl Display for PopRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PopRequest [op={}, expired={}, subscription_data={:?}]",
            self.op, self.expired, self.subscription_data
        )
    }
}
