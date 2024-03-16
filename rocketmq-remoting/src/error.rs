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
    fmt::{Display, Formatter},
    io,
};

use thiserror::Error;

use crate::error::RemotingError::{Io, RemotingCommandDecoderError};

#[derive(Debug, Error)]
pub enum RemotingError {
    RemotingCommandDecoderError(String),
    RemotingCommandEncoderError(String),
    RemotingCommandException(String),
    FromStrError(String),
    Io(io::Error),
}

impl From<io::Error> for RemotingError {
    fn from(err: io::Error) -> Self {
        Io(err)
    }
}

impl Display for RemotingError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RemotingCommandDecoderError(msg) => write!(f, "{}", msg),
            Io(err) => write!(f, "{:?}", err),
            RemotingError::RemotingCommandEncoderError(msg) => write!(f, "{}", msg),
            RemotingError::FromStrError(msg) => {
                write!(f, "{}", msg)
            }
            RemotingError::RemotingCommandException(msg) => write!(f, "{}", msg),
        }
    }
}
