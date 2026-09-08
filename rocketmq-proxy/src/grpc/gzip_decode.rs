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

use std::io::Read;
use std::sync::{Arc, Mutex, Weak};

use bytes::Bytes;
use rocketmq_proxy_core::error::canonical;
use rocketmq_proxy_core::ingress::grpc::service::ExecutionGuards;
use rocketmq_proxy_core::proto::v2;
use rocketmq_proxy_core::{GrpcConfig, ProxyError, ProxyResult};
use rocketmq_runtime::ResourcePermit;
use tokio_util::sync::CancellationToken;

// Includes the inflater window, tables, reader buffer and bounded bookkeeping.
// Variable gzip header copies are charged to the retained input reservation.
const DECODER_WORKSPACE: usize = 256 * 1024;

struct Slot {
    arena: Vec<u8>,
    _resident: ResourcePermit,
}

#[derive(Clone)]
pub(super) struct GzipDecodePool {
    idle: Arc<Mutex<Option<Vec<Slot>>>>,
}

pub(super) struct DecodeSlotLease {
    slot: Option<Slot>,
    pool: Weak<Mutex<Option<Vec<Slot>>>>,
}

impl Drop for DecodeSlotLease {
    fn drop(&mut self) {
        if let (Some(slot), Some(pool)) = (self.slot.take(), self.pool.upgrade()) {
            if let Some(idle) = pool.lock().unwrap_or_else(std::sync::PoisonError::into_inner).as_mut() {
                idle.push(slot);
            }
        }
    }
}

struct SendRetention {
    lease: Option<DecodeSlotLease>,
    _backings: Vec<Bytes>,
    _input: ResourcePermit,
}

struct RetainedBodyOwner {
    body: RetainedBody,
    retention: Arc<SendRetention>,
}

enum RetainedBody {
    Arena(std::ops::Range<usize>),
    Input(Bytes),
}

impl AsRef<[u8]> for RetainedBodyOwner {
    fn as_ref(&self) -> &[u8] {
        match &self.body {
            RetainedBody::Arena(range) => {
                // Only normalized arena bodies use this variant. The lease never moves
                // out of its retention until the final Bytes owner has been dropped.
                match self.retention.lease.as_ref().and_then(|lease| lease.slot.as_ref()) {
                    Some(slot) => &slot.arena[range.clone()],
                    None => unreachable!("an arena body always retains its decode slot"),
                }
            }
            RetainedBody::Input(body) => body.as_ref(),
        }
    }
}

impl GzipDecodePool {
    pub(super) fn new(config: &GrpcConfig, guards: &ExecutionGuards) -> ProxyResult<Self> {
        if config.max_send_messages_per_request == 0
            || config.max_message_body_size == 0
            || config.max_decompressed_request_bytes < config.max_message_body_size
        {
            return Err(ProxyError::invalid_metadata(
                "invalid send batch or decompression limits",
            ));
        }
        let pool = Self {
            idle: Arc::new(Mutex::new(Some(Vec::new()))),
        };
        if config.gzip_decode_slots == 0 {
            return Ok(pool);
        }
        let slot_bytes = config
            .max_decompressed_request_bytes
            .checked_add(DECODER_WORKSPACE)
            .ok_or_else(|| ProxyError::invalid_metadata("gzip arena size overflow"))?;
        let bytes = slot_bytes
            .checked_mul(config.gzip_decode_slots)
            .ok_or_else(|| ProxyError::invalid_metadata("gzip pool size overflow"))?;
        // Protobuf strings/maps may allocate more than their wire representation.
        let input = config
            .max_decoding_message_size
            .checked_mul(8)
            .ok_or_else(|| ProxyError::invalid_metadata("maximum input size overflow"))?;
        guards.validate_resident_bytes(bytes, input)?;
        let budget = guards.resident_budget(config.gzip_decode_slots, bytes)?;
        let mut slots = pool.idle.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        for _ in 0..config.gzip_decode_slots {
            // Provision only after the aggregate reservation has been validated.
            let mut arena = Vec::new();
            arena
                .try_reserve_exact(config.max_decompressed_request_bytes)
                .map_err(canonical::argument_with_source)?;
            let resident = budget
                .try_acquire_data(
                    arena
                        .capacity()
                        .checked_add(DECODER_WORKSPACE)
                        .ok_or_else(|| ProxyError::invalid_metadata("allocated gzip arena size overflow"))?,
                )
                .map_err(|_| ProxyError::invalid_metadata("allocated gzip pool exceeds its configured budget"))?;
            arena.resize(config.max_decompressed_request_bytes, 0);
            slots
                .as_mut()
                .ok_or_else(|| ProxyError::invalid_metadata("gzip pool closed during provisioning"))?
                .push(Slot {
                    arena,
                    _resident: resident,
                });
        }
        drop(slots);
        Ok(pool)
    }

    pub(super) fn decode(
        &self,
        mut request: v2::SendMessageRequest,
        config: &GrpcConfig,
        input: ResourcePermit,
        cancellation: &CancellationToken,
        mut lease: Option<DecodeSlotLease>,
    ) -> ProxyResult<v2::SendMessageRequest> {
        validate_request(&request, config)?;
        let mut retained = Vec::with_capacity(request.messages.len());
        let mut bodies = Vec::with_capacity(request.messages.len());
        let mut used = 0usize;
        for message in &mut request.messages {
            if cancellation.is_cancelled() {
                return Err(ProxyError::Draining);
            }
            let input_body = std::mem::take(&mut message.body);
            if is_gzip(message) {
                let slot = lease
                    .as_mut()
                    .and_then(|lease| lease.slot.as_mut())
                    .ok_or_else(|| ProxyError::invalid_metadata("gzip decode slot is unavailable"))?;
                let end = used
                    .checked_add(config.max_message_body_size)
                    .unwrap_or(usize::MAX)
                    .min(slot.arena.len());
                let start = used;
                let mut decoder = flate2::read::GzDecoder::new(input_body.as_ref());
                while used < end {
                    if cancellation.is_cancelled() {
                        return Err(ProxyError::Draining);
                    }
                    let count = decoder
                        .read(&mut slot.arena[used..end])
                        .map_err(canonical::argument_with_source)?;
                    if count == 0 {
                        break;
                    }
                    used += count;
                }
                let mut probe = [0u8; 1];
                if decoder.read(&mut probe).map_err(canonical::argument_with_source)? != 0 {
                    return Err(canonical::argument("decoded message or aggregate body limit exceeded").into());
                }
                bodies.push(RetainedBody::Arena(start..used));
                if let Some(system) = &mut message.system_properties {
                    system.body_encoding = v2::Encoding::Identity as i32;
                }
            } else {
                let end = used
                    .checked_add(input_body.len())
                    .filter(|end| *end <= config.max_decompressed_request_bytes)
                    .ok_or_else(|| ProxyError::from(canonical::argument("aggregate body limit exceeded")))?;
                if let Some(slot) = lease.as_mut().and_then(|lease| lease.slot.as_mut()) {
                    slot.arena[used..end].copy_from_slice(&input_body);
                    bodies.push(RetainedBody::Arena(used..end));
                } else {
                    bodies.push(RetainedBody::Input(input_body.clone()));
                }
                used = end;
            }
            retained.push(input_body);
        }
        let retention = Arc::new(SendRetention {
            lease,
            _input: input,
            _backings: retained,
        });
        for (message, body) in request.messages.iter_mut().zip(bodies) {
            message.body = Bytes::from_owner(RetainedBodyOwner {
                retention: retention.clone(),
                body,
            });
        }
        Ok(request)
    }

    fn close(&self) {
        self.idle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
    }

    pub(super) fn shutdown_guard(&self) -> DecodePoolShutdown {
        DecodePoolShutdown(self.clone())
    }

    pub(super) fn lease_for(&self, request: &v2::SendMessageRequest) -> ProxyResult<Option<DecodeSlotLease>> {
        if request.messages.iter().any(is_gzip) {
            self.try_borrow().map(Some)
        } else {
            Ok(None)
        }
    }

    fn try_borrow(&self) -> ProxyResult<DecodeSlotLease> {
        let slot = self
            .idle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_mut()
            .and_then(Vec::pop)
            .ok_or_else(|| ProxyError::too_many_requests("gzip decode slots"))?;
        Ok(DecodeSlotLease {
            slot: Some(slot),
            pool: Arc::downgrade(&self.idle),
        })
    }
}

pub(crate) struct DecodePoolShutdown(GzipDecodePool);

impl Drop for DecodePoolShutdown {
    fn drop(&mut self) {
        self.0.close();
    }
}

fn is_gzip(message: &v2::Message) -> bool {
    message
        .system_properties
        .as_ref()
        .is_some_and(|system| system.body_encoding == v2::Encoding::Gzip as i32)
}

pub(super) fn validate_request(request: &v2::SendMessageRequest, config: &GrpcConfig) -> ProxyResult<()> {
    if request.messages.len() > config.max_send_messages_per_request {
        return Err(canonical::argument("send message count exceeds the configured maximum").into());
    }
    for message in &request.messages {
        if let Some(system) = &message.system_properties {
            if !matches!(
                v2::Encoding::try_from(system.body_encoding),
                Ok(v2::Encoding::Identity | v2::Encoding::Gzip | v2::Encoding::Unspecified)
            ) {
                return Err(canonical::argument("unsupported message body encoding").into());
            }
        }
        if !is_gzip(message) && message.body.len() > config.max_message_body_size {
            return Err(canonical::argument("message body limit exceeded").into());
        }
    }
    Ok(())
}

pub(super) fn retained_input_bytes(request: &v2::SendMessageRequest, config: &GrpcConfig) -> ProxyResult<usize> {
    // Include the shared Tonic backing allocation, gzip header copies, protobuf
    // containers and allocator slack. This bounds retained input, not allocator RSS.
    let wire = rocketmq_proxy_core::ingress::grpc::service::admission::estimated_protobuf_retained_bytes(request)
        .saturating_sub(std::mem::size_of_val(request));
    if wire > config.max_decoding_message_size {
        return Err(canonical::argument("send request exceeds the decoding limit").into());
    }
    let mut bytes = config
        .max_decoding_message_size
        .checked_add(
            wire.checked_mul(2)
                .ok_or_else(|| ProxyError::from(canonical::argument("input size overflow")))?,
        )
        .ok_or_else(|| ProxyError::from(canonical::argument("input size overflow")))?;
    let mut add = |amount: usize| -> ProxyResult<()> {
        bytes = bytes
            .checked_add(amount)
            .ok_or_else(|| ProxyError::from(canonical::argument("input size overflow")))?;
        Ok(())
    };
    add(request
        .messages
        .capacity()
        .checked_mul(std::mem::size_of::<v2::Message>() + 256)
        .ok_or_else(|| ProxyError::from(canonical::argument("input capacity overflow")))?)?;
    for message in &request.messages {
        if let Some(topic) = &message.topic {
            add(topic.name.capacity())?;
            add(topic.resource_namespace.capacity())?;
        }
        add(message
            .user_properties
            .capacity()
            .checked_mul(2 * (2 * std::mem::size_of::<String>() + 1))
            .ok_or_else(|| ProxyError::from(canonical::argument("property capacity overflow")))?)?;
        for (key, value) in &message.user_properties {
            add(key.capacity())?;
            add(value.capacity())?;
        }
        if let Some(system) = &message.system_properties {
            for value in [
                &system.tag,
                &system.receipt_handle,
                &system.message_group,
                &system.trace_context,
                &system.lite_topic,
            ]
            .into_iter()
            .flatten()
            {
                add(value.capacity())?;
            }
            for value in [&system.message_id, &system.born_host, &system.store_host] {
                add(value.capacity())?;
            }
            add(system
                .keys
                .capacity()
                .checked_mul(std::mem::size_of::<String>())
                .ok_or_else(|| ProxyError::from(canonical::argument("key capacity overflow")))?)?;
            for value in &system.keys {
                add(value.capacity())?;
            }
            if let Some(digest) = &system.body_digest {
                add(digest.checksum.capacity())?;
            }
            if let Some(dlq) = &system.dead_letter_queue {
                add(dlq.topic.capacity())?;
                add(dlq.message_id.capacity())?;
            }
        }
    }
    let maximum = config
        .max_decoding_message_size
        .checked_mul(8)
        .ok_or_else(|| ProxyError::from(canonical::argument("input limit overflow")))?;
    if bytes > maximum {
        return Err(canonical::argument("retained send metadata exceeds the configured input budget").into());
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_runtime::{BudgetLimit, FullPolicy, ResourceBudgetTree};
    use std::io::Write;

    fn config() -> GrpcConfig {
        GrpcConfig {
            max_message_body_size: 5,
            max_decompressed_request_bytes: 8,
            max_send_messages_per_request: 4,
            gzip_decode_slots: 1,
            max_decoding_message_size: 4096,
            ..Default::default()
        }
    }

    fn pool(config: &GrpcConfig) -> GzipDecodePool {
        let guards = ExecutionGuards::try_with_resident_slots(
            &rocketmq_proxy_core::RuntimeConfig {
                process_memory_limit_bytes: 16 * 1024 * 1024,
                ..Default::default()
            },
            config.gzip_decode_slots,
        )
        .unwrap();
        GzipDecodePool::new(config, &guards).unwrap()
    }

    fn message(body: &[u8], gzip: bool) -> v2::Message {
        let body = if gzip {
            let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
            encoder.write_all(body).unwrap();
            Bytes::from(encoder.finish().unwrap())
        } else {
            Bytes::copy_from_slice(body)
        };
        v2::Message {
            body,
            system_properties: Some(v2::SystemProperties {
                body_encoding: if gzip {
                    v2::Encoding::Gzip
                } else {
                    v2::Encoding::Identity
                } as i32,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn decode(
        pool: &GzipDecodePool,
        config: &GrpcConfig,
        messages: Vec<v2::Message>,
    ) -> ProxyResult<v2::SendMessageRequest> {
        let request = v2::SendMessageRequest { messages };
        let tree = ResourceBudgetTree::new("input", BudgetLimit::new(1, 65536, FullPolicy::Reject)).unwrap();
        let input = tree
            .root()
            .try_acquire_data(retained_input_bytes(&request, config)?)
            .unwrap();
        let lease = pool.lease_for(&request)?;
        pool.decode(request, config, input, &CancellationToken::new(), lease)
    }

    #[test]
    fn gzip_limits_include_identity_and_preserve_arena_capacity() {
        let config = config();
        let pool = pool(&config);
        let capacity = pool.idle.lock().unwrap().as_ref().unwrap()[0].arena.capacity();
        for gzip_first in [false, true] {
            let decoded = decode(
                &pool,
                &config,
                vec![message(b"hello", gzip_first), message(b"abc", !gzip_first)],
            )
            .unwrap();
            assert_eq!(decoded.messages[0].body.as_ref(), b"hello");
            assert_eq!(decoded.messages[1].body.as_ref(), b"abc");
            assert_eq!(
                decoded.messages[0].system_properties.as_ref().unwrap().body_encoding,
                v2::Encoding::Identity as i32
            );
            drop(decoded);
            assert_eq!(
                pool.idle.lock().unwrap().as_ref().unwrap()[0].arena.capacity(),
                capacity
            );
        }
        assert!(decode(&pool, &config, vec![message(b"123456", true)]).is_err());
        assert!(decode(&pool, &config, vec![message(b"12345", true), message(b"6789", false)]).is_err());
        assert!(decode(&pool, &config, vec![message(b"12345", false), message(b"6789", true)]).is_err());
        let mut corrupt = message(b"hello", true);
        let mut bytes = corrupt.body.to_vec();
        let crc = bytes.len() - 8;
        bytes[crc] ^= 0xff;
        corrupt.body = bytes.into();
        assert!(decode(&pool, &config, vec![corrupt]).is_err());
        assert_eq!(
            pool.idle.lock().unwrap().as_ref().unwrap()[0].arena.capacity(),
            capacity
        );
    }

    #[test]
    fn body_slices_hold_input_and_slot_until_last_owner_even_after_pool_drop() {
        let config = config();
        let pool = pool(&config);
        let request = v2::SendMessageRequest {
            messages: vec![message(b"hello", true), message(b"abc", false)],
        };
        let budget = ResourceBudgetTree::new("input", BudgetLimit::new(1, 65536, FullPolicy::Reject))
            .unwrap()
            .root();
        let input = budget.try_acquire_data(123).unwrap();
        let lease = pool.lease_for(&request).unwrap();
        let decoded = pool
            .decode(request, &config, input, &CancellationToken::new(), lease)
            .unwrap();
        let body = decoded.messages[1].body.slice(1..2);
        drop(decoded);
        assert_eq!(budget.snapshot().current_bytes, 123);
        assert!(decode(&pool, &config, vec![message(b"hello", true)]).is_err());
        pool.close();
        assert!(pool.idle.lock().unwrap().is_none());
        let weak = Arc::downgrade(&pool.idle);
        drop(pool);
        assert!(weak.upgrade().is_none());
        assert_eq!(body.as_ref(), b"b");
        assert_eq!(budget.snapshot().current_bytes, 123);
        drop(body);
        assert_eq!(budget.snapshot().current_bytes, 0);
    }

    #[test]
    fn cancelled_decode_retains_resources_until_blocking_owner_exits() {
        let config = config();
        let pool = pool(&config);
        let request = v2::SendMessageRequest {
            messages: vec![message(b"hello", true)],
        };
        let budget = ResourceBudgetTree::new("input", BudgetLimit::new(1, 65536, FullPolicy::Reject))
            .unwrap()
            .root();
        let input = budget.try_acquire_data(123).unwrap();
        let lease = pool.lease_for(&request).unwrap();
        let cancel = CancellationToken::new();
        let cancellation = cancel.clone();
        let (release, wait) = std::sync::mpsc::channel();
        let worker_pool = pool.clone();
        let worker = std::thread::spawn(move || {
            wait.recv().unwrap();
            worker_pool.decode(request, &config, input, &cancellation, lease)
        });
        cancel.cancel();
        assert_eq!(budget.snapshot().current_bytes, 123);
        assert!(pool.idle.lock().unwrap().as_ref().unwrap().is_empty());
        release.send(()).unwrap();
        assert!(worker.join().unwrap().is_err());
        assert_eq!(budget.snapshot().current_bytes, 0);
        assert_eq!(pool.idle.lock().unwrap().as_ref().unwrap().len(), 1);
    }
    #[test]
    fn invalid_pool_configuration_fails_before_allocating_and_zero_slots_disables_gzip() {
        let mut config = config();
        let guards = ExecutionGuards::try_with_resident_slots(
            &rocketmq_proxy_core::RuntimeConfig {
                process_memory_limit_bytes: 16 * 1024 * 1024,
                ..Default::default()
            },
            1,
        )
        .unwrap();
        config.gzip_decode_slots = usize::MAX;
        assert!(GzipDecodePool::new(&config, &guards).is_err());
        config.gzip_decode_slots = 1;
        config.max_decompressed_request_bytes = 4;
        assert!(GzipDecodePool::new(&config, &guards).is_err());
        config.max_decompressed_request_bytes = 8;
        config.gzip_decode_slots = 0;
        let pool = GzipDecodePool::new(&config, &guards).unwrap();
        assert!(decode(&pool, &config, vec![message(b"hello", true)]).is_err());
        assert!(decode(&pool, &config, vec![message(b"hello", false)]).is_ok());
        config.max_send_messages_per_request = 0;
        assert!(GzipDecodePool::new(&config, &guards).is_err());
    }
}
