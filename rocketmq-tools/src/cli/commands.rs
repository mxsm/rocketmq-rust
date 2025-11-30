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

//! CLI commands module
//!
//! This module contains refactored CLI command implementations that use
//! core business logic. These are thin wrappers that handle:
//! - Argument parsing (clap)
//! - Input validation
//! - Output formatting
//! - Error display
//!
//! # Available Commands
//!
//! - [`TopicClusterSubCommand`] - Get cluster list for a topic

pub mod topic_cluster_command;

// Re-export command structs for convenience
pub use self::topic_cluster_command::TopicClusterSubCommand;
