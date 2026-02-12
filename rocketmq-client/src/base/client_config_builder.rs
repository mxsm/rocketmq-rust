// Copyright 2023 The RocketMQ Rust Authors
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

//! ClientConfig builder module
//!
//! This module provides a fluent builder API for constructing ClientConfig instances,
//! making configuration more readable and allowing for validation before building.

use cheetah_string::CheetahString;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::LanguageCode;

use super::access_channel::AccessChannel;
use super::client_config::ClientConfig;
use super::client_config_validation::ClientConfigValidator;

/// Builder for creating [`ClientConfig`] instances with a fluent API
///
/// # Example
///
/// ```rust
/// use rocketmq_client_rust::base::client_config::ClientConfig;
///
/// let config = ClientConfig::builder()
///     .namesrv_addr("localhost:9876")
///     .instance_name("my_producer")
///     .enable_tls(true)
///     .poll_name_server_interval(60_000)
///     .build()
///     .unwrap();
/// ```
pub struct ClientConfigBuilder {
    config: ClientConfig,
}

impl Default for ClientConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ClientConfigBuilder {
    /// Creates a new builder with default configuration
    pub fn new() -> Self {
        Self {
            config: ClientConfig::new(),
        }
    }

    // ========================================================================
    // Name server configuration
    // ========================================================================

    /// Sets the name server address
    pub fn namesrv_addr(mut self, addr: impl Into<CheetahString>) -> Self {
        self.config.set_namesrv_addr(addr.into());
        self
    }

    // ========================================================================
    // Client identification
    // ========================================================================

    /// Sets the client IP address
    pub fn client_ip(mut self, ip: impl Into<CheetahString>) -> Self {
        self.config.set_client_ip(ip.into());
        self
    }

    /// Sets the instance name
    pub fn instance_name(mut self, name: impl Into<CheetahString>) -> Self {
        self.config.set_instance_name(name.into());
        self
    }

    // ========================================================================
    // Thread pool configuration--only java client has these options,
    // but we keep them for compatibility and future use
    // ========================================================================

    /// Sets the number of client callback executor threads
    pub fn client_callback_executor_threads(mut self, threads: usize) -> Self {
        self.config.set_client_callback_executor_threads(threads);
        self
    }

    /// Sets the concurrent heartbeat thread pool size
    pub fn concurrent_heartbeat_thread_pool_size(mut self, size: usize) -> Self {
        self.config.set_concurrent_heartbeat_thread_pool_size(size);
        self
    }

    // ========================================================================
    // Namespace configuration
    // ========================================================================

    /// Sets the namespace (deprecated in Java, kept for compatibility)
    pub fn namespace(mut self, namespace: impl Into<CheetahString>) -> Self {
        self.config.set_namespace(namespace.into());
        self
    }

    /// Sets the namespace v2
    pub fn namespace_v2(mut self, namespace: impl Into<CheetahString>) -> Self {
        self.config.set_namespace_v2(namespace.into());
        self
    }

    // ========================================================================
    // Access channel
    // ========================================================================

    /// Sets the access channel (Local or Cloud)
    pub fn access_channel(mut self, channel: AccessChannel) -> Self {
        self.config.set_access_channel(channel);
        self
    }

    // ========================================================================
    // Interval configuration
    // ========================================================================

    /// Sets the poll name server interval in milliseconds
    pub fn poll_name_server_interval(mut self, millis: u32) -> Self {
        self.config.set_poll_name_server_interval(millis);
        self
    }

    /// Sets the heartbeat broker interval in milliseconds
    pub fn heartbeat_broker_interval(mut self, millis: u32) -> Self {
        self.config.set_heartbeat_broker_interval(millis);
        self
    }

    /// Sets the persist consumer offset interval in milliseconds
    pub fn persist_consumer_offset_interval(mut self, millis: u32) -> Self {
        self.config.set_persist_consumer_offset_interval(millis);
        self
    }

    // ========================================================================
    // Timeout configuration
    // ========================================================================

    /// Sets the pull time delay in milliseconds when exception occurs
    pub fn pull_time_delay_millis_when_exception(mut self, millis: u32) -> Self {
        self.config.set_pull_time_delay_millis_when_exception(millis);
        self
    }

    /// Sets the MQ client API timeout in milliseconds
    pub fn mq_client_api_timeout(mut self, millis: u64) -> Self {
        self.config.set_mq_client_api_timeout(millis);
        self
    }

    /// Sets the detect timeout in milliseconds
    pub fn detect_timeout(mut self, millis: u32) -> Self {
        self.config.set_detect_timeout(millis);
        self
    }

    /// Sets the detect interval in milliseconds
    pub fn detect_interval(mut self, millis: u32) -> Self {
        self.config.set_detect_interval(millis);
        self
    }

    // ========================================================================
    // Boolean flags
    // ========================================================================

    /// Sets unit mode enabled
    pub fn enable_unit_mode(mut self, enabled: bool) -> Self {
        self.config.set_unit_mode(enabled);
        self
    }

    /// Sets VIP channel enabled
    pub fn enable_vip_channel(mut self, enabled: bool) -> Self {
        self.config.set_vip_channel_enabled(enabled);
        self
    }

    /// Sets heartbeat v2 enabled
    pub fn enable_heartbeat_v2(mut self, enabled: bool) -> Self {
        self.config.set_use_heartbeat_v2(enabled);
        self
    }

    /// Sets concurrent heartbeat enabled
    pub fn enable_concurrent_heartbeat(mut self, enabled: bool) -> Self {
        self.config.enable_concurrent_heartbeat = enabled;
        self
    }

    /// Sets TLS enabled
    pub fn enable_tls(mut self, enabled: bool) -> Self {
        self.config.set_use_tls(enabled);
        self
    }

    /// Sets decode read body enabled
    pub fn enable_decode_read_body(mut self, enabled: bool) -> Self {
        self.config.set_decode_read_body(enabled);
        self
    }

    /// Sets decode decompress body enabled
    pub fn enable_decode_decompress_body(mut self, enabled: bool) -> Self {
        self.config.set_decode_decompress_body(enabled);
        self
    }

    /// Sets stream request type enabled
    pub fn enable_stream_request_type(mut self, enabled: bool) -> Self {
        self.config.set_enable_stream_request_type(enabled);
        self
    }

    /// Sets send latency enabled
    pub fn enable_send_latency(mut self, enabled: bool) -> Self {
        self.config.set_send_latency_enable(enabled);
        self
    }

    /// Sets start detector enabled
    pub fn enable_start_detector(mut self, enabled: bool) -> Self {
        self.config.set_start_detector_enable(enabled);
        self
    }

    /// Sets heartbeat channel event listener enabled
    pub fn enable_heartbeat_channel_event_listener(mut self, enabled: bool) -> Self {
        self.config.set_enable_heartbeat_channel_event_listener(enabled);
        self
    }

    /// Sets trace enabled
    pub fn enable_trace(mut self, enabled: bool) -> Self {
        self.config.set_enable_trace(enabled);
        self
    }

    // ========================================================================
    // Other configuration
    // ========================================================================

    /// Sets the unit name
    pub fn unit_name(mut self, name: impl Into<CheetahString>) -> Self {
        self.config.set_unit_name(name.into());
        self
    }

    /// Sets the SOCKS proxy config
    pub fn socks_proxy_config(mut self, config: impl Into<CheetahString>) -> Self {
        self.config.set_socks_proxy_config(config.into());
        self
    }

    /// Sets the language code
    pub fn language(mut self, language: LanguageCode) -> Self {
        self.config.set_language(language);
        self
    }

    /// Sets the trace topic
    pub fn trace_topic(mut self, topic: impl Into<CheetahString>) -> Self {
        self.config.set_trace_topic(topic.into());
        self
    }

    /// Sets the trace message batch number
    pub fn trace_msg_batch_num(mut self, num: usize) -> Self {
        self.config.set_trace_msg_batch_num(num);
        self
    }

    /// Sets the max page size in get metadata
    pub fn max_page_size_in_get_metadata(mut self, size: usize) -> Self {
        self.config.set_max_page_size_in_get_metadata(size);
        self
    }

    // ========================================================================
    // Build methods
    // ========================================================================

    /// Builds the ClientConfig with validation
    ///
    /// # Errors
    ///
    /// Returns an error if any configuration value is invalid.
    pub fn build(self) -> RocketMQResult<ClientConfig> {
        // Validate configuration before returning
        self.validate()?;
        Ok(self.config)
    }

    /// Builds the ClientConfig without validation (internal use only)
    ///
    /// # Safety
    ///
    /// This method skips validation and should only be used when you're
    /// certain all values are correct.
    #[doc(hidden)]
    pub fn build_unvalidated(self) -> ClientConfig {
        self.config
    }

    /// Validates the current configuration
    fn validate(&self) -> RocketMQResult<()> {
        // Validate intervals
        ClientConfigValidator::validate_poll_name_server_interval(self.config.poll_name_server_interval)?;
        ClientConfigValidator::validate_heartbeat_broker_interval(self.config.heartbeat_broker_interval)?;
        ClientConfigValidator::validate_persist_consumer_offset_interval(self.config.persist_consumer_offset_interval)?;

        // Validate timeouts
        ClientConfigValidator::validate_mq_client_api_timeout(self.config.mq_client_api_timeout)?;

        // Validate numeric limits
        ClientConfigValidator::validate_trace_msg_batch_num(self.config.trace_msg_batch_num)?;
        ClientConfigValidator::validate_max_page_size_in_get_metadata(self.config.max_page_size_in_get_metadata)?;

        // Validate thread pool sizes
        ClientConfigValidator::validate_client_callback_executor_threads(self.config.client_callback_executor_threads)?;

        // Only validate concurrent heartbeat thread pool size if concurrent heartbeat is enabled
        if self.config.enable_concurrent_heartbeat {
            ClientConfigValidator::validate_concurrent_heartbeat_thread_pool_size(
                self.config.concurrent_heartbeat_thread_pool_size,
            )?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_basic() {
        let config = ClientConfig::builder()
            .namesrv_addr("localhost:9876")
            .instance_name("test_instance")
            .build()
            .unwrap();

        assert_eq!(config.get_namesrv_addr().unwrap().as_str(), "localhost:9876");
        assert_eq!(config.get_instance_name().as_str(), "test_instance");
    }

    #[test]
    fn test_builder_with_all_options() {
        let config = ClientConfig::builder()
            .namesrv_addr("localhost:9876")
            .instance_name("test")
            .client_ip("127.0.0.1")
            .poll_name_server_interval(60_000)
            .heartbeat_broker_interval(30_000)
            .enable_tls(true)
            .enable_concurrent_heartbeat(true)
            .concurrent_heartbeat_thread_pool_size(4)
            .build()
            .unwrap();

        assert_eq!(config.get_namesrv_addr().unwrap().as_str(), "localhost:9876");
        assert_eq!(config.get_instance_name().as_str(), "test");
        assert_eq!(config.get_client_ip().unwrap().as_str(), "127.0.0.1");
        assert_eq!(config.poll_name_server_interval, 60_000);
        assert_eq!(config.heartbeat_broker_interval, 30_000);
        assert!(config.use_tls);
        assert!(config.enable_concurrent_heartbeat);
        assert_eq!(config.concurrent_heartbeat_thread_pool_size, 4);
    }

    #[test]
    fn test_builder_validation_invalid_poll_interval() {
        let result = ClientConfig::builder()
            .poll_name_server_interval(100) // Too small
            .build();

        assert!(result.is_err());
    }

    #[test]
    fn test_builder_validation_invalid_heartbeat_interval() {
        let result = ClientConfig::builder()
            .heartbeat_broker_interval(100) // Too small
            .build();

        assert!(result.is_err());
    }

    #[test]
    fn test_builder_validation_invalid_thread_pool_size() {
        let result = ClientConfig::builder()
            .enable_concurrent_heartbeat(true)
            .concurrent_heartbeat_thread_pool_size(0) // Invalid
            .build();

        assert!(result.is_err());
    }

    #[test]
    fn test_builder_validation_invalid_trace_msg_batch_num() {
        let result = ClientConfig::builder().trace_msg_batch_num(0).build();

        assert!(result.is_err());
    }

    #[test]
    fn test_builder_unvalidated() {
        // This should not panic even with invalid values
        let config = ClientConfig::builder()
            .poll_name_server_interval(100) // Invalid
            .build_unvalidated();

        assert_eq!(config.poll_name_server_interval, 100);
    }
}
