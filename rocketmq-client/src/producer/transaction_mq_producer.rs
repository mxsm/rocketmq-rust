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
use std::sync::Arc;

use crate::producer::default_mq_producer::DefaultMQProducer;
use crate::producer::transaction_listener::TransactionListener;

#[derive(Clone)]
pub struct TransactionProducerConfig {
    pub transaction_check_listener: Option<Arc<Box<dyn TransactionListener>>>,
    pub check_thread_pool_min_size: u32,
    pub check_thread_pool_max_size: u32,
    pub check_request_hold_max: u32,
}

pub struct TransactionMQProducer {
    default_producer: DefaultMQProducer,
}
