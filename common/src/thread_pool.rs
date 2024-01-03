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

 use std::cmp;

 pub struct ThreadPool {
     inner: futures::executor::ThreadPool,
     rt: tokio::runtime::Runtime,
 }
 
 pub struct ThreadPoolBuilder {
     pool_size: usize,
     stack_size: usize,
     thread_name_prefix: Option<String>,
 }
 
 impl ThreadPoolBuilder {
     pub fn new() -> ThreadPoolBuilder {
         ThreadPoolBuilder {
             pool_size: cmp::max(1, num_cpus::get()),
             stack_size: 0,
             thread_name_prefix: None,
         }
     }
 
     pub fn pool_size(mut self, pool_size: usize) -> Self {
         self.pool_size = pool_size;
         self
     }
 
     pub fn stack_size(mut self, stack_size: usize) -> Self {
         self.stack_size = stack_size;
         self
     }
 
     pub fn create(&mut self) -> anyhow::Result<ThreadPool> {
         Err(anyhow::anyhow!("not implemented"))
     }
 }
 