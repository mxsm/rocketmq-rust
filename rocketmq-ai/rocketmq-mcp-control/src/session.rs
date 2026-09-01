// Copyright 2026 The RocketMQ Rust Authors
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

use std::future::Future;
use std::pin::Pin;

use crate::guard::AuthorizedMutation;
use crate::model::MutationArguments;

pub type SessionFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionError {
    Conflict,
    Failed,
}

/// Narrow lifecycle required from one future mutation Admin implementation.
pub trait MutationAdminSession: Send {
    fn preflight<'a>(
        &'a mut self,
        authorized: &'a AuthorizedMutation,
        arguments: &'a MutationArguments,
    ) -> SessionFuture<'a, Result<(), SessionError>>;

    fn dry_run<'a>(
        &'a mut self,
        authorized: &'a AuthorizedMutation,
        arguments: &'a MutationArguments,
    ) -> SessionFuture<'a, Result<(), SessionError>>;

    fn execute<'a>(
        &'a mut self,
        authorized: &'a AuthorizedMutation,
        arguments: &'a MutationArguments,
    ) -> SessionFuture<'a, Result<(), SessionError>>;

    fn verify<'a>(
        &'a mut self,
        authorized: &'a AuthorizedMutation,
        arguments: &'a MutationArguments,
    ) -> SessionFuture<'a, Result<(), SessionError>>;

    fn shutdown(&mut self) -> SessionFuture<'_, Result<(), SessionError>>;
}

pub trait MutationSessionFactory: Send + Sync {
    fn open(&self) -> SessionFuture<'_, Result<Box<dyn MutationAdminSession>, SessionError>>;
}
