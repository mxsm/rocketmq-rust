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

 use std::{cmp, future::Future};

 pub struct TokioExecutorService {
     inner: tokio::runtime::Runtime,
 }
 
 impl TokioExecutorService {
     pub fn new() -> TokioExecutorService {
         TokioExecutorService {
             inner: tokio::runtime::Builder::new_current_thread()
                 .enable_all()
                 .build()
                 .unwrap(),
         }
     }
 }
 
 impl TokioExecutorService {
     pub fn spawn<F>(&self, future: F)
     where
         F: Future<Output = ()> + Send + 'static,
     {
         self.inner.spawn(future);
     }
 }
 
 pub struct FuturesExecutorService {
     inner: futures::executor::ThreadPool,
 }
 impl FuturesExecutorService {
     pub fn spawn<F>(&self, future: F)
     where
         F: Future<Output = ()> + Send + 'static,
     {
         self.inner.spawn_ok(future);
     }
 }
 
 pub struct FuturesExecutorServiceBuilder {
     pool_size: usize,
     stack_size: usize,
     thread_name_prefix: Option<String>,
 }
 
 impl FuturesExecutorServiceBuilder {
     pub fn new() -> FuturesExecutorServiceBuilder {
         FuturesExecutorServiceBuilder {
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
 
     pub fn create(&mut self) -> anyhow::Result<FuturesExecutorService> {
         let thread_pool = futures::executor::ThreadPool::builder()
             .stack_size(self.stack_size)
             .pool_size(self.pool_size)
             .name_prefix(
                 self.thread_name_prefix
                     .as_ref()
                     .unwrap_or(&String::from("Default-Executor")),
             )
             .create()
             .unwrap();
         Ok(FuturesExecutorService { inner: thread_pool })
     }
 }
 