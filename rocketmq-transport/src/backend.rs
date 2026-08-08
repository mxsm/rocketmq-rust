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

use std::io;
use std::io::IoSlice;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadBuf;
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::tcp::OwnedWriteHalf;

use crate::connection::BoxedConnectionTransport;

pub(crate) enum ReadBackend {
    Tcp(OwnedReadHalf),
    Compat(tokio::io::ReadHalf<BoxedConnectionTransport>),
}

impl AsyncRead for ReadBackend {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut *self {
            Self::Tcp(reader) => Pin::new(reader).poll_read(context, buffer),
            Self::Compat(reader) => Pin::new(reader).poll_read(context, buffer),
        }
    }
}

pub(crate) enum WriteBackend {
    Tcp(OwnedWriteHalf),
    Compat(tokio::io::WriteHalf<BoxedConnectionTransport>),
}

impl AsyncWrite for WriteBackend {
    fn poll_write(mut self: Pin<&mut Self>, context: &mut Context<'_>, bytes: &[u8]) -> Poll<io::Result<usize>> {
        match &mut *self {
            Self::Tcp(writer) => Pin::new(writer).poll_write(context, bytes),
            Self::Compat(writer) => Pin::new(writer).poll_write(context, bytes),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            Self::Tcp(writer) => Pin::new(writer).poll_flush(context),
            Self::Compat(writer) => Pin::new(writer).poll_flush(context),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            Self::Tcp(writer) => Pin::new(writer).poll_shutdown(context),
            Self::Compat(writer) => Pin::new(writer).poll_shutdown(context),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            Self::Tcp(writer) => writer.is_write_vectored(),
            Self::Compat(writer) => writer.is_write_vectored(),
        }
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffers: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        match &mut *self {
            Self::Tcp(writer) => Pin::new(writer).poll_write_vectored(context, buffers),
            Self::Compat(writer) => Pin::new(writer).poll_write_vectored(context, buffers),
        }
    }
}
