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

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

pub struct CompletableFuture<T> {
    result: Option<T>,
}

impl<T> CompletableFuture<T> {
    pub fn new() -> Self {
        CompletableFuture { result: None }
    }

    pub fn completed_future(&mut self, result: T) {
        self.result = Some(result);
    }
}

impl<T> Default for CompletableFuture<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Future for CompletableFuture<T> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.result {
            None => Poll::Pending,
            Some(_) => Poll::Ready(()),
        }
    }
}
