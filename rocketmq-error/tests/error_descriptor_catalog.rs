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

use std::collections::HashSet;

use rocketmq_error::descriptor_by_code;
use rocketmq_error::fields;
use rocketmq_error::BacktracePolicy;
use rocketmq_error::CanonicalCondition;
use rocketmq_error::ErrorClass;
use rocketmq_error::ErrorCode;
use rocketmq_error::ErrorDescriptor;
use rocketmq_error::ALL_DESCRIPTORS;
use rocketmq_error::STORAGE_BACKEND_UNAVAILABLE;
use rocketmq_error::STORAGE_CAPACITY_EXHAUSTED;
use rocketmq_error::STORAGE_INTERNAL_FAILURE;
use rocketmq_error::STORAGE_IO_FAILED;
use rocketmq_error::STORAGE_LIFECYCLE_NOT_STARTED;
use rocketmq_error::STORAGE_MAPPED_FILE_NOT_FOUND;
use rocketmq_error::STORAGE_OPERATION_TIMED_OUT;
use rocketmq_error::STORAGE_OPERATION_UNSUPPORTED;
use rocketmq_error::STORAGE_READ_FAILED;
use rocketmq_error::STORAGE_REQUEST_INVALID;
use rocketmq_error::STORAGE_STATE_CORRUPTED;
use rocketmq_error::STORAGE_WRITE_FAILED;

const EXPECTED_DESCRIPTOR_SNAPSHOTS: &[&str] = &[
    "protocol.header.invalid|validation|InvalidArgument|Caller|protocol|Request header is invalid|Info|never|Never|Public|29|BadRequest|InvalidArgument|400|64|operation:Diagnostic:Text:Some(64),invalid_value_present:SecretPresenceOnly:Presence:None,source_present:SecretPresenceOnly:Presence:None",
    "route.topic.not_found|routing|NotFound|RemotePeer|route|Topic route was not found|Warn|refresh_route|Never|Public|17|TopicNotFound|NotFound|404|66|topic:Public:Text:Some(127)",
    "auth.credentials.invalid|authentication|Unauthenticated|Caller|auth|Authentication credentials are invalid|Error|refresh_credentials|Never|Generic|16|Unauthorized|Unauthenticated|401|77|credentials_present:SecretPresenceOnly:Presence:None",
    "auth.permission.denied|authorization|PermissionDenied|Caller|auth|Permission was denied|Error|never|Never|Public|16|Forbidden|PermissionDenied|403|77|operation:Public:Text:Some(64)",
    "transport.admission.queue_saturated|capacity|ResourceExhausted|LocalResource|transport|Transport admission queue is saturated|Warn|backoff|Never|Public|2|TooManyRequests|ResourceExhausted|429|75|remote_addr:Diagnostic:Text:Some(256)",
    "controller.leadership.not_leader|routing|FailedPrecondition|LocalResource|controller|Controller is not the leader|Warn|refresh_leader|Never|Public|2007|InternalError|FailedPrecondition|409|65|leader_id:Diagnostic:U64:None",
    "transport.connection.timeout|timeout|DeadlineExceeded|Dependency|transport|Transport connection timed out|Warn|backoff|Never|Public|2|RequestTimeout|DeadlineExceeded|504|75|timeout_ms:Public:U64:None,remote_addr:Diagnostic:Text:Some(256),source_present:SecretPresenceOnly:Presence:None",
    "transport.start.failed|unavailable|Unavailable|Unknown|transport|Transport server could not be started|Error|operator_action|Never|Generic|1|InternalError|Unavailable|503|69|operation:Diagnostic:Text:Some(64),source_present:SecretPresenceOnly:Presence:None",
    "transport.dispatch.failed|internal|Internal|LocalResource|transport|Transport request dispatch failed|Error|operator_action|OnDemand|Generic|1|InternalError|Internal|500|70|operation:Diagnostic:Text:Some(64),source_present:SecretPresenceOnly:Presence:None",
    "transport.response.failed|internal|Internal|Dependency|transport|Transport response delivery failed|Error|operator_action|OnDemand|Generic|1|InternalError|Internal|500|70|operation:Diagnostic:Text:Some(64),source_present:SecretPresenceOnly:Presence:None",
    "transport.session.failed|unavailable|Unavailable|Dependency|transport|Transport session operation failed|Error|backoff|Never|Generic|1|InternalError|Unavailable|503|69|operation:Diagnostic:Text:Some(64),source_present:SecretPresenceOnly:Presence:None",
    "storage.lifecycle.not_started|validation|FailedPrecondition|Caller|storage|Storage service is not started|Warn|never|Never|Public|1|InternalError|FailedPrecondition|409|65|store_operation:Diagnostic:Text:Some(64),store_component:Diagnostic:Text:Some(64),store_detail:SecretPresenceOnly:Presence:None,source_present:SecretPresenceOnly:Presence:None",
    "storage.backend.unavailable|unavailable|Unavailable|Dependency|storage|Storage backend is unavailable|Error|backoff|Never|Generic|1|InternalError|Unavailable|503|69|store_operation:Diagnostic:Text:Some(64),store_component:Diagnostic:Text:Some(64),store_detail:SecretPresenceOnly:Presence:None,source_present:SecretPresenceOnly:Presence:None",
    "storage.request.invalid|validation|InvalidArgument|Caller|storage|Storage request is invalid|Info|never|Never|Public|29|BadRequest|InvalidArgument|400|64|store_operation:Diagnostic:Text:Some(64),store_component:Diagnostic:Text:Some(64),store_detail:SecretPresenceOnly:Presence:None,source_present:SecretPresenceOnly:Presence:None",
    "storage.mapped_file.not_found|io|NotFound|LocalResource|storage|Mapped file was not found|Warn|never|Never|Generic|22|NotFound|NotFound|404|66|store_operation:Diagnostic:Text:Some(64),store_component:Diagnostic:Text:Some(64),store_detail:SecretPresenceOnly:Presence:None,source_present:SecretPresenceOnly:Presence:None",
    "storage.capacity.exhausted|capacity|ResourceExhausted|LocalResource|storage|Storage capacity is exhausted|Critical|operator_action|Never|Generic|1|InternalError|ResourceExhausted|507|65|store_operation:Diagnostic:Text:Some(64),store_component:Diagnostic:Text:Some(64),store_detail:SecretPresenceOnly:Presence:None,source_present:SecretPresenceOnly:Presence:None",
    "storage.read.failed|io|Internal|LocalResource|storage|Storage read failed|Error|operator_action|Never|Generic|1|InternalError|Internal|500|70|store_operation:Diagnostic:Text:Some(64),store_component:Diagnostic:Text:Some(64),store_detail:SecretPresenceOnly:Presence:None,source_present:SecretPresenceOnly:Presence:None",
    "storage.write.failed|io|Internal|LocalResource|storage|Storage write failed|Error|operator_action|Never|Generic|1|InternalError|Internal|500|70|store_operation:Diagnostic:Text:Some(64),store_component:Diagnostic:Text:Some(64),store_detail:SecretPresenceOnly:Presence:None,source_present:SecretPresenceOnly:Presence:None",
    "storage.io.failed|io|Internal|LocalResource|storage|Storage I/O operation failed|Error|operator_action|Never|Generic|1|InternalError|Internal|500|70|store_operation:Diagnostic:Text:Some(64),store_component:Diagnostic:Text:Some(64),store_detail:SecretPresenceOnly:Presence:None,source_present:SecretPresenceOnly:Presence:None",
    "storage.state.corrupted|data_corruption|DataLoss|LocalResource|storage|Storage state is corrupted|Critical|operator_action|OnDemand|Generic|1|InternalError|DataLoss|500|65|store_operation:Diagnostic:Text:Some(64),store_component:Diagnostic:Text:Some(64),store_detail:SecretPresenceOnly:Presence:None,source_present:SecretPresenceOnly:Presence:None",
    "storage.operation.timed_out|timeout|DeadlineExceeded|LocalResource|storage|Storage operation timed out|Warn|backoff|Never|Generic|2|RequestTimeout|DeadlineExceeded|504|75|store_operation:Diagnostic:Text:Some(64),store_component:Diagnostic:Text:Some(64),store_detail:SecretPresenceOnly:Presence:None,source_present:SecretPresenceOnly:Presence:None",
    "storage.operation.unsupported|unsupported|Unimplemented|Configuration|storage|Storage operation is unsupported|Error|never|Never|Public|3|Unsupported|Unimplemented|400|64|store_operation:Diagnostic:Text:Some(64),store_component:Diagnostic:Text:Some(64),store_detail:SecretPresenceOnly:Presence:None,source_present:SecretPresenceOnly:Presence:None",
    "storage.internal.failure|internal|Internal|Unknown|storage|Internal storage failure|Error|operator_action|OnDemand|Generic|1|InternalError|Internal|500|70|store_operation:Diagnostic:Text:Some(64),store_component:Diagnostic:Text:Some(64),store_detail:SecretPresenceOnly:Presence:None,source_present:SecretPresenceOnly:Presence:None",
    "protocol.version.unsupported|unsupported|Unimplemented|Caller|protocol|Protocol version is unsupported|Error|never|Never|Public|3|Unsupported|Unimplemented|400|64|ordinal:Public:U64:None",
    "core.internal.failure|internal|Internal|Unknown|core|Internal error|Error|operator_action|OnDemand|Generic|1|InternalError|Internal|500|70|operation:Diagnostic:Text:Some(64),source_present:SecretPresenceOnly:Presence:None",
    "runtime.configuration.failed|validation|InvalidArgument|Configuration|runtime|Runtime configuration is invalid|Info|never|Never|Public|29|BadRequest|InvalidArgument|400|78|operation:Diagnostic:Text:Some(64),source_present:SecretPresenceOnly:Presence:None",
    "runtime.build.failed|unavailable|Unavailable|Configuration|runtime|Runtime could not be started|Error|operator_action|Never|Generic|1|InternalError|Unavailable|503|69|operation:Diagnostic:Text:Some(64),source_present:SecretPresenceOnly:Presence:None",
    "runtime.io.failed|io|Internal|LocalResource|runtime|Runtime I/O operation failed|Error|operator_action|Never|Generic|1|InternalError|Internal|500|70|operation:Diagnostic:Text:Some(64),source_present:SecretPresenceOnly:Presence:None",
    "runtime.context.unavailable|unavailable|Unavailable|LocalResource|runtime|Runtime context is unavailable|Error|never|Never|Generic|1|InternalError|Unavailable|503|69|operation:Diagnostic:Text:Some(64)",
    "runtime.capacity.exhausted|capacity|ResourceExhausted|LocalResource|runtime|Runtime capacity is exhausted|Warn|backoff|Never|Generic|2|TooManyRequests|ResourceExhausted|429|75|operation:Diagnostic:Text:Some(64)",
    "runtime.operation.timed_out|timeout|DeadlineExceeded|Dependency|runtime|Runtime operation timed out|Warn|backoff|Never|Generic|2|RequestTimeout|DeadlineExceeded|504|75|operation:Diagnostic:Text:Some(64)",
    "runtime.operation.unsupported|unsupported|Unimplemented|Caller|runtime|Runtime operation is unsupported|Error|never|Never|Public|3|Unsupported|Unimplemented|400|64|operation:Diagnostic:Text:Some(64)",
    "runtime.task.join_failed|bug|Internal|Bug|runtime|Runtime task failed|Error|operator_action|OnDemand|Generic|1|InternalError|Internal|500|70|operation:Diagnostic:Text:Some(64),source_present:SecretPresenceOnly:Presence:None",
    "runtime.internal.failure|internal|Internal|Unknown|runtime|Runtime operation failed|Error|operator_action|OnDemand|Generic|1|InternalError|Internal|500|70|operation:Diagnostic:Text:Some(64),source_present:SecretPresenceOnly:Presence:None",
    "transport.endpoint.invalid|validation|InvalidArgument|Caller|transport|Transport endpoint is invalid|Info|never|Never|Generic|2|BadRequest|InvalidArgument|400|64|remote_addr:SecretPresenceOnly:Presence:None",
    "transport.remote.rate_limited|capacity|ResourceExhausted|RemotePeer|transport|Remote transport peer rate limited the request|Warn|backoff|Never|Generic|2|TooManyRequests|ResourceExhausted|429|75|remote_addr:Diagnostic:Text:Some(256),limit:Diagnostic:U64:None",
    "transport.write.timeout|timeout|DeadlineExceeded|Dependency|transport|Transport write timed out|Warn|backoff|Never|Generic|2|RequestTimeout|DeadlineExceeded|504|75|phase:Diagnostic:Text:Some(32),timeout_ms:Public:U64:None,remote_addr:SecretPresenceOnly:Presence:None,source_present:SecretPresenceOnly:Presence:None",
    "transport.response.timeout|timeout|DeadlineExceeded|Dependency|transport|Transport response timed out|Warn|backoff|Never|Generic|2|RequestTimeout|DeadlineExceeded|504|75|phase:Diagnostic:Text:Some(32),timeout_ms:Public:U64:None,remote_addr:SecretPresenceOnly:Presence:None,source_present:SecretPresenceOnly:Presence:None",
    "transport.dns.failed|unavailable|Unavailable|Dependency|transport|Transport DNS resolution failed|Error|backoff|Never|Generic|2|InternalError|Unavailable|503|69|host:SecretPresenceOnly:Presence:None,source_present:SecretPresenceOnly:Presence:None",
    "transport.connection.failed|unavailable|Unavailable|Dependency|transport|Transport connection operation failed|Error|backoff|Never|Generic|2|InternalError|Unavailable|503|69|phase:Diagnostic:Text:Some(32),remote_addr:SecretPresenceOnly:Presence:None,source_present:SecretPresenceOnly:Presence:None",
    "core.serialization.failed|internal|Internal|Unknown|core|Serialization failed|Error|never|OnDemand|Generic|1|InternalError|Internal|500|70|operation:Diagnostic:Text:Some(64),format:Diagnostic:Text:Some(64),field:Public:Text:Some(64),source_present:SecretPresenceOnly:Presence:None,detail:SecretPresenceOnly:Presence:None",
    "protocol.body.invalid|validation|InvalidArgument|Caller|protocol|Request body is invalid|Info|never|Never|Generic|29|BadRequest|InvalidArgument|400|64|operation:Diagnostic:Text:Some(64),invalid_value_present:SecretPresenceOnly:Presence:None,source_present:SecretPresenceOnly:Presence:None",
    "protocol.encoding.unsupported|unsupported|Unimplemented|Caller|protocol|Protocol encoding is unsupported|Error|never|Never|Public|3|Unsupported|Unimplemented|400|64|serialization_type:Public:U64:None",
    "protocol.request.unsupported|unsupported|Unimplemented|Caller|protocol|Protocol request is unsupported|Error|never|Never|Public|3|Unsupported|Unimplemented|400|64|request_code:Public:I64:None",
    "proxy.broker_response.permission_denied|authorization|PermissionDenied|RemotePeer|proxy|Broker denied the Proxy operation|Warn|never|Never|Generic|1|Forbidden|PermissionDenied|403|77|operation:Diagnostic:Text:Some(64),broker_code:Diagnostic:I64:None,broker_addr:Diagnostic:Text:Some(256),message:SecretPresenceOnly:Presence:None,source_present:SecretPresenceOnly:Presence:None",
    "proxy.broker_response.topic_not_found|routing|NotFound|RemotePeer|proxy|Broker topic was not found|Warn|never|Never|Generic|1|TopicNotFound|NotFound|404|66|operation:Diagnostic:Text:Some(64),broker_code:Diagnostic:I64:None,broker_addr:Diagnostic:Text:Some(256),message:SecretPresenceOnly:Presence:None,source_present:SecretPresenceOnly:Presence:None",
    "proxy.broker_response.consumer_group_not_found|routing|NotFound|RemotePeer|proxy|Broker consumer group was not found|Warn|never|Never|Generic|1|ConsumerGroupNotFound|NotFound|404|66|operation:Diagnostic:Text:Some(64),broker_code:Diagnostic:I64:None,broker_addr:Diagnostic:Text:Some(256),message:SecretPresenceOnly:Presence:None,source_present:SecretPresenceOnly:Presence:None",
    "proxy.broker_response.resource_not_found|routing|NotFound|RemotePeer|proxy|Broker resource was not found|Warn|never|Never|Generic|1|NotFound|NotFound|404|66|operation:Diagnostic:Text:Some(64),broker_code:Diagnostic:I64:None,broker_addr:Diagnostic:Text:Some(256),message:SecretPresenceOnly:Presence:None,source_present:SecretPresenceOnly:Presence:None",
    "proxy.broker_response.offset_not_found|routing|NotFound|RemotePeer|proxy|Broker offset was not found|Warn|never|Never|Generic|1|OffsetNotFound|NotFound|404|66|operation:Diagnostic:Text:Some(64),broker_code:Diagnostic:I64:None,broker_addr:Diagnostic:Text:Some(256),message:SecretPresenceOnly:Presence:None,source_present:SecretPresenceOnly:Presence:None",
    "proxy.broker_response.offset_invalid|validation|InvalidArgument|Caller|proxy|Broker rejected an invalid offset|Info|never|Never|Generic|1|IllegalOffset|InvalidArgument|400|64|operation:Diagnostic:Text:Some(64),broker_code:Diagnostic:I64:None,broker_addr:Diagnostic:Text:Some(256),message:SecretPresenceOnly:Presence:None,source_present:SecretPresenceOnly:Presence:None",
    "proxy.broker_response.request_unsupported|unsupported|Unimplemented|Dependency|proxy|Broker does not support the Proxy request|Error|never|Never|Generic|1|Unsupported|Unimplemented|501|70|operation:Diagnostic:Text:Some(64),broker_code:Diagnostic:I64:None,broker_addr:Diagnostic:Text:Some(256),message:SecretPresenceOnly:Presence:None,source_present:SecretPresenceOnly:Presence:None",
    "proxy.broker_response.failed|internal|Internal|RemotePeer|proxy|Broker response failed|Error|switch_broker|Never|Generic|1|InternalError|Internal|500|70|operation:Diagnostic:Text:Some(64),broker_code:Diagnostic:I64:None,broker_addr:Diagnostic:Text:Some(256),message:SecretPresenceOnly:Presence:None,source_present:SecretPresenceOnly:Presence:None",
    "proxy.remoting.request.invalid|validation|InvalidArgument|Caller|proxy|Proxy remoting request is invalid|Info|never|Never|Generic|1|BadRequest|InvalidArgument|400|64|operation:Diagnostic:Text:Some(64),source_present:SecretPresenceOnly:Presence:None",
    "proxy.upstream.request.failed|unavailable|Unavailable|Dependency|proxy|Proxy upstream request failed|Error|backoff|Never|Generic|1|InternalError|Unavailable|503|69|operation:Diagnostic:Text:Some(64),source_present:SecretPresenceOnly:Presence:None",
    "proxy.drain.unavailable|unavailable|Unavailable|LocalResource|proxy|Proxy drain service is unavailable|Error|operator_action|Never|Generic|14|InternalError|Unavailable|503|69|operation:Diagnostic:Text:Some(64),source_present:SecretPresenceOnly:Presence:None",
    "rpc.broker_address.not_found|routing|NotFound|Dependency|client|RPC broker address was not found|Error|backoff|Never|Generic|1|NotFound|NotFound|404|66|broker:Diagnostic:Text:Some(127)",
    "rpc.request.unsupported|unsupported|Unimplemented|Caller|client|RPC request is unsupported|Error|never|Never|Generic|1|Unsupported|Unimplemented|400|64|request_code:Public:I64:None",
    "auth.operation.failed|internal|Internal|Dependency|auth|Authentication operation failed|Error|never|OnDemand|Generic|16|InternalError|Internal|500|70|operation:Diagnostic:Text:Some(64),reason:SecretPresenceOnly:Presence:None,source_present:SecretPresenceOnly:Presence:None",
    "controller.internal.failure|internal|Internal|Unknown|controller|Controller operation failed|Error|never|OnDemand|Generic|2015|InternalError|Internal|500|70|operation:Diagnostic:Text:Some(64),phase:Diagnostic:Text:Some(32),source_present:SecretPresenceOnly:Presence:None",
    "controller.request.invalid|validation|InvalidArgument|Caller|controller|Controller request is invalid|Info|never|Never|Generic|2015|BadRequest|InvalidArgument|400|64|operation:Diagnostic:Text:Some(64),reason:SecretPresenceOnly:Presence:None,source_present:SecretPresenceOnly:Presence:None",
    "controller.configuration.invalid|validation|InvalidArgument|Configuration|controller|Controller configuration is invalid|Error|never|Never|Generic|2015|BadRequest|InvalidArgument|400|78|key:Diagnostic:Text:Some(64),reason:SecretPresenceOnly:Presence:None,source_present:SecretPresenceOnly:Presence:None",
    "controller.lifecycle.not_initialized|validation|FailedPrecondition|LocalResource|controller|Controller is not initialized|Error|never|Never|Generic|2015|InternalError|FailedPrecondition|409|65|component:Diagnostic:Text:Some(64),reason:SecretPresenceOnly:Presence:None",
    "protocol.message.property.invalid|validation|InvalidArgument|Caller|protocol|Message property is invalid|Info|never|Never|Public|13|BadRequest|InvalidArgument|400|64|property:Public:Text:Some(127)",
    "broker.lookup.not_found|routing|NotFound|RemotePeer|broker|Broker was not found|Error|switch_broker|Never|Public|211|NotFound|NotFound|404|66|broker:Diagnostic:Text:Some(127)",
    "broker.registration.failed|unavailable|Unavailable|Dependency|broker|Broker registration failed|Error|switch_broker|Never|Generic|1|InternalError|Unavailable|503|69|broker:Diagnostic:Text:Some(127),reason:SecretPresenceOnly:Presence:None",
    "broker.operation.failed|internal|Internal|RemotePeer|broker|Broker operation failed|Error|switch_broker|OnDemand|Generic|1|InternalError|Internal|500|70|operation:Diagnostic:Text:Some(64),broker_code:Diagnostic:I64:None,broker_addr:Diagnostic:Text:Some(256),message:SecretPresenceOnly:Presence:None",
    "broker.topic.not_found|routing|NotFound|RemotePeer|broker|Topic does not exist|Warn|never|Never|Public|17|TopicNotFound|NotFound|404|66|topic:Public:Text:Some(127)",
    "broker.queue.not_found|routing|NotFound|RemotePeer|broker|Queue does not exist|Warn|switch_broker|Never|Public|22|NotFound|NotFound|404|66|topic:Public:Text:Some(127),queue_id:Public:I64:None",
    "broker.subscription_group.not_found|routing|NotFound|RemotePeer|broker|Subscription group does not exist|Warn|backoff|Never|Public|26|ConsumerGroupNotFound|NotFound|404|66|group:Public:Text:Some(127)",
    "broker.queue.id_out_of_range|validation|InvalidArgument|Caller|broker|Queue id is out of range|Error|never|Never|Public|1|BadRequest|InvalidArgument|400|64|topic:Public:Text:Some(127),queue_id:Public:I64:None,max_queue_id:Public:I64:None",
    "broker.message.too_large|capacity|ResourceExhausted|Caller|broker|Message body is too large|Error|never|Never|Public|13|MessageBodyTooLarge|ResourceExhausted|413|65|actual_bytes:Public:U64:None,limit_bytes:Public:U64:None",
    "broker.message.invalid|validation|InvalidArgument|Caller|broker|Message validation failed|Error|never|Never|Generic|13|BadRequest|InvalidArgument|400|64|reason:SecretPresenceOnly:Presence:None",
    "client.retry.budget_exhausted|capacity|ResourceExhausted|LocalResource|client|Retry budget was exhausted|Warn|never|Never|Public|2|TooManyRequests|ResourceExhausted|429|75|group:Public:Text:Some(127),current:Public:I64:None,max:Public:I64:None",
    "broker.transaction.rejected|internal|Aborted|RemotePeer|broker|Transaction message was rejected|Error|never|Never|Public|1|BadRequest|Aborted|409|65|",
    "broker.leadership.not_master|routing|FailedPrecondition|RemotePeer|broker|Broker is not the master|Warn|refresh_leader|Never|Generic|501|InternalError|FailedPrecondition|409|65|master_address:Diagnostic:Text:Some(256)",
    "broker.query.not_found|routing|NotFound|RemotePeer|broker|Broker query result was not found|Warn|backoff|Never|Public|22|NotFound|NotFound|404|66|resource:Public:Text:Some(127),offset:Diagnostic:I64:None",
    "broker.task.failed|internal|Internal|LocalResource|broker|Broker asynchronous task failed|Error|never|OnDemand|Generic|1|InternalError|Internal|500|70|task:Diagnostic:Text:Some(64),source_present:SecretPresenceOnly:Presence:None,context:SecretPresenceOnly:Presence:None",
    "protocol.response.failed|validation|InvalidArgument|RemotePeer|protocol|Response processing failed|Info|never|Never|Generic|29|BadRequest|InvalidArgument|400|64|operation:Diagnostic:Text:Some(64),reason:SecretPresenceOnly:Presence:None",
    "route.topic.inconsistent|internal|Internal|RemotePeer|route|Topic route data is inconsistent|Error|refresh_route|OnDemand|Public|1|InternalError|Internal|500|70|topic:Public:Text:Some(127),reason:SecretPresenceOnly:Presence:None",
    "route.registration.conflict|routing|Aborted|RemotePeer|route|Route registration conflict|Error|refresh_route|Never|Public|1|BadRequest|Aborted|409|65|broker:Diagnostic:Text:Some(127),reason:SecretPresenceOnly:Presence:None,expected:Diagnostic:U64:None,actual:Diagnostic:U64:None",
    "route.cluster.not_found|routing|NotFound|RemotePeer|route|Cluster was not found|Error|never|Never|Public|211|NotFound|NotFound|404|66|cluster:Public:Text:Some(127)",
    "client.lifecycle.not_started|validation|FailedPrecondition|Caller|client|Client is not started|Error|never|Never|Public|1|InternalError|FailedPrecondition|409|65|",
    "client.lifecycle.already_started|validation|AlreadyExists|Caller|client|Client is already started|Error|never|Never|Public|1|BadRequest|AlreadyExists|409|65|",
    "client.lifecycle.shutting_down|unavailable|Unavailable|LocalResource|client|Client is shutting down|Error|never|Never|Public|1|InternalError|Unavailable|503|69|",
    "client.lifecycle.invalid_state|validation|FailedPrecondition|Caller|client|Client state is invalid|Error|never|Never|Public|1|InternalError|FailedPrecondition|409|65|expected:Diagnostic:Text:Some(64),actual:Diagnostic:Text:Some(64)",
    "client.component.unavailable|unavailable|Unavailable|LocalResource|client|Client component is unavailable|Error|switch_broker|Never|Public|1|InternalError|Unavailable|503|69|client_role:Public:Text:Some(32)",
    "rpc.request.failed|unavailable|Unavailable|Dependency|client|RPC request failed|Error|backoff|Never|Generic|1|InternalError|Unavailable|503|69|remote_addr:Diagnostic:Text:Some(256),request_code:Public:I64:None,timeout_ms:Public:U64:None,source_present:SecretPresenceOnly:Presence:None",
    "rpc.response.failed|internal|Internal|RemotePeer|client|RPC response failed|Error|backoff|Never|Generic|1|InternalError|Internal|500|70|remote_code:Diagnostic:I64:None,message:SecretPresenceOnly:Presence:None",
    "tools.operation.failed|internal|Internal|Dependency|tools|Administrative operation failed|Error|backoff|OnDemand|Generic|1|InternalError|Internal|500|70|operation:Diagnostic:Text:Some(64),topic:Public:Text:Some(127),broker:Diagnostic:Text:Some(127),consumer:Diagnostic:Text:Some(127)",
    "protocol.filter.invalid|validation|InvalidArgument|Unknown|protocol|Filter operation failed|Info|never|Never|Generic|1|BadRequest|InvalidArgument|400|64|filter_kind:Diagnostic:Text:Some(64),filter_compile_kind:Diagnostic:Text:Some(64),filter_compile_stage:Diagnostic:Text:Some(64),filter_compile_position:Diagnostic:U64:None,filter_compile_source:Diagnostic:Text:Some(64),position:Diagnostic:U64:None,limit:Diagnostic:U64:None",
    "observability.feature.disabled|unsupported|FailedPrecondition|Configuration|observability|Observability feature is disabled|Info|never|Never|Public|29|InternalError|FailedPrecondition|409|78|feature:Public:Text:Some(64)",
    "observability.configuration.invalid|validation|InvalidArgument|Configuration|observability|Observability configuration is invalid|Info|never|Never|Generic|29|BadRequest|InvalidArgument|400|78|reason:SecretPresenceOnly:Presence:None",
    "observability.initialization.failed|internal|Internal|Dependency|observability|Observability initialization failed|Error|backoff|OnDemand|Generic|1|InternalError|Internal|500|70|observability_signal:Diagnostic:Text:Some(32),reason:SecretPresenceOnly:Presence:None",
    "observability.log_filter.invalid|validation|InvalidArgument|Configuration|observability|Observability log filter is invalid|Info|never|Never|Generic|29|BadRequest|InvalidArgument|400|78|filter:SecretPresenceOnly:Presence:None,error:SecretPresenceOnly:Presence:None",
    "observability.subscriber.installation_failed|internal|FailedPrecondition|LocalResource|observability|Observability subscriber installation failed|Error|never|OnDemand|Public|1|InternalError|FailedPrecondition|409|65|attempted:Diagnostic:Bool:None,installed:Diagnostic:Bool:None",
    "observability.shutdown.failed|internal|Internal|Dependency|observability|Observability shutdown failed|Error|backoff|OnDemand|Generic|1|InternalError|Internal|500|70|observability_signal:Diagnostic:Text:Some(32),reason:SecretPresenceOnly:Presence:None",
    "core.configuration.parse_failed|validation|InvalidArgument|Configuration|core|Configuration parsing failed|Error|never|Never|Generic|29|BadRequest|InvalidArgument|400|78|key:Diagnostic:Text:Some(64),reason:SecretPresenceOnly:Presence:None",
    "core.configuration.missing|validation|InvalidArgument|Configuration|core|Required configuration is missing|Info|never|Never|Generic|29|BadRequest|InvalidArgument|400|78|key:Diagnostic:Text:Some(64)",
    "core.configuration.invalid|validation|InvalidArgument|Configuration|core|Configuration value is invalid|Info|never|Never|Generic|29|BadRequest|InvalidArgument|400|78|key:Diagnostic:Text:Some(64),value:SecretPresenceOnly:Presence:None,reason:SecretPresenceOnly:Presence:None",
    "auth.configuration.invalid|validation|InvalidArgument|Configuration|auth|Authentication configuration is invalid|Error|never|Never|Generic|29|BadRequest|InvalidArgument|400|78|key:Diagnostic:Text:Some(64),reason:SecretPresenceOnly:Presence:None",
    "auth.configuration.reload_failed|internal|Internal|LocalResource|auth|Authentication configuration reload failed|Error|never|OnDemand|Generic|1|InternalError|Internal|500|70|path:SecretPresenceOnly:Presence:None,reason:SecretPresenceOnly:Presence:None",
    "controller.consensus.failed|internal|Internal|Dependency|controller|Controller consensus operation failed|Error|never|OnDemand|Generic|2015|InternalError|Internal|500|70|operation:Diagnostic:Text:Some(64),phase:Diagnostic:Text:Some(32),reason:SecretPresenceOnly:Presence:None,source_present:SecretPresenceOnly:Presence:None",
    "controller.consensus.timed_out|timeout|DeadlineExceeded|Dependency|controller|Controller consensus operation timed out|Error|never|Never|Generic|2015|RequestTimeout|DeadlineExceeded|504|75|operation:Diagnostic:Text:Some(64),timeout_ms:Public:U64:None",
    "core.io.failed|io|Internal|LocalResource|core|I/O operation failed|Error|never|Never|Generic|1|InternalError|Internal|500|70|operation:Diagnostic:Text:Some(64),source_present:SecretPresenceOnly:Presence:None",
    "core.argument.invalid|validation|InvalidArgument|Caller|core|Argument is invalid|Info|never|Never|Generic|29|BadRequest|InvalidArgument|400|64|message:SecretPresenceOnly:Presence:None",
    "core.operation.timed_out|timeout|DeadlineExceeded|Dependency|core|Operation timed out|Warn|backoff|Never|Public|2|RequestTimeout|DeadlineExceeded|504|75|operation:Diagnostic:Text:Some(64),timeout_ms:Public:U64:None",
    "core.service.failed|internal|Internal|LocalResource|core|Service lifecycle operation failed|Error|never|OnDemand|Generic|1|InternalError|Internal|500|70|operation:Diagnostic:Text:Some(64)",
    "core.lifecycle.not_initialized|validation|FailedPrecondition|Caller|core|Component is not initialized|Error|never|Never|Generic|1|InternalError|FailedPrecondition|409|65|component:Diagnostic:Text:Some(64),reason:SecretPresenceOnly:Presence:None",
];

fn descriptor_snapshot(descriptor: &ErrorDescriptor) -> String {
    let fields = descriptor
        .fields()
        .iter()
        .map(|field| {
            format!(
                "{}:{:?}:{:?}:{:?}",
                field.name(),
                field.visibility(),
                field.value_kind(),
                field.text_byte_limit()
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    let projection = descriptor.projection();

    format!(
        "{}|{}|{:?}|{:?}|{}|{}|{:?}|{}|{:?}|{:?}|{}|{:?}|{:?}|{}|{}|{}",
        descriptor.code(),
        descriptor.class(),
        descriptor.condition(),
        descriptor.fault(),
        descriptor.component(),
        descriptor.public_message(),
        descriptor.severity(),
        descriptor.recovery_hint().as_str(),
        descriptor.backtrace_policy(),
        descriptor.exposure(),
        projection.remoting().code.as_i32(),
        projection.grpc().payload,
        projection.grpc().status,
        projection.http().status.as_u16(),
        projection.cli().exit_code.as_i32(),
        fields
    )
}

#[test]
fn descriptor_catalog_snapshot_is_exact() {
    assert_eq!(EXPECTED_DESCRIPTOR_SNAPSHOTS.len(), 108);
    assert_eq!(ALL_DESCRIPTORS.len(), EXPECTED_DESCRIPTOR_SNAPSHOTS.len());

    for (descriptor, expected) in ALL_DESCRIPTORS.iter().zip(EXPECTED_DESCRIPTOR_SNAPSHOTS) {
        assert_eq!(descriptor_snapshot(descriptor), *expected, "{}", descriptor.code());
    }
}

#[test]
fn on_demand_backtraces_are_limited_to_internal_bug_or_data_loss_descriptors() {
    for descriptor in ALL_DESCRIPTORS {
        if descriptor.backtrace_policy() == BacktracePolicy::OnDemand {
            assert!(
                matches!(descriptor.class(), ErrorClass::INTERNAL | ErrorClass::BUG)
                    || descriptor.condition() == CanonicalCondition::DataLoss,
                "{}",
                descriptor.code()
            );
        }

        if matches!(
            descriptor.class(),
            ErrorClass::VALIDATION
                | ErrorClass::AUTHENTICATION
                | ErrorClass::AUTHORIZATION
                | ErrorClass::CAPACITY
                | ErrorClass::TIMEOUT
                | ErrorClass::UNAVAILABLE
                | ErrorClass::UNSUPPORTED
        ) {
            assert_eq!(
                descriptor.backtrace_policy(),
                BacktracePolicy::Never,
                "{}",
                descriptor.code()
            );
        }
    }
}

#[test]
fn transport_convergence_descriptors_are_exact() {
    for code in [
        "transport.start.failed",
        "transport.dispatch.failed",
        "transport.response.failed",
        "transport.session.failed",
    ] {
        let descriptor = descriptor_by_code(code).expect("transport descriptor");
        assert_eq!(
            descriptor.fields(),
            [fields::OPERATION_DIAGNOSTIC.schema(), fields::SOURCE_PRESENT.schema()],
            "{code}"
        );
    }
}

#[test]
fn catalog_codes_are_unique_valid_and_lookup_is_exact() {
    let mut codes = HashSet::new();

    for descriptor in ALL_DESCRIPTORS {
        let code = descriptor.code();
        assert_eq!(ErrorCode::try_new(code.as_str()), Some(code));
        assert!(codes.insert(code.as_str()), "duplicate catalog code: {code}");
        assert_eq!(descriptor_by_code(code.as_str()), Some(descriptor));
    }

    for unknown in [
        "route.topic.missing",
        "route.topic",
        "route..topic",
        "ROUTE_NOT_FOUND",
        "NETWORK_CONNECTION_FAILED",
        "",
    ] {
        assert_eq!(descriptor_by_code(unknown), None, "unexpected lookup for {unknown:?}");
    }
}

#[test]
fn public_messages_and_protocol_values_are_boundary_safe() {
    for descriptor in ALL_DESCRIPTORS {
        let code = descriptor.code();
        let message = descriptor.public_message();
        assert!(!message.is_empty(), "{code}");
        assert_eq!(message.trim(), message, "{code}");
        assert!(!message.chars().any(char::is_control), "{code}");

        let projection = descriptor.projection();
        assert_ne!(projection.remoting().code.as_i32(), 0, "{code}");
        assert_ne!(projection.http().status.as_u16(), 0, "{code}");
        assert_ne!(projection.cli().exit_code.as_i32(), 0, "{code}");
    }
}

#[test]
fn every_storage_descriptor_has_the_exact_allowed_fields() {
    let storage_descriptors = [
        STORAGE_LIFECYCLE_NOT_STARTED,
        STORAGE_BACKEND_UNAVAILABLE,
        STORAGE_REQUEST_INVALID,
        STORAGE_MAPPED_FILE_NOT_FOUND,
        STORAGE_CAPACITY_EXHAUSTED,
        STORAGE_READ_FAILED,
        STORAGE_WRITE_FAILED,
        STORAGE_IO_FAILED,
        STORAGE_STATE_CORRUPTED,
        STORAGE_OPERATION_TIMED_OUT,
        STORAGE_OPERATION_UNSUPPORTED,
        STORAGE_INTERNAL_FAILURE,
    ];
    let expected = [
        fields::STORE_OPERATION.schema(),
        fields::STORE_COMPONENT.schema(),
        fields::STORE_DETAIL_PRESENT.schema(),
        fields::SOURCE_PRESENT.schema(),
    ];

    for descriptor in storage_descriptors {
        assert_eq!(descriptor.fields(), expected, "{}", descriptor.code());
    }
}

#[test]
fn descriptor_construction_and_catalog_macro_remain_private() {
    let descriptor_source = include_str!("../src/descriptor.rs");
    let projection_source = include_str!("../src/projection.rs");
    let catalog_source = include_str!("../src/catalog.rs");
    let crate_root = include_str!("../src/lib.rs");

    assert!(descriptor_source.contains("pub(crate) const fn try_new("));
    assert!(projection_source.contains("pub(crate) const fn new("));
    assert!(!descriptor_source.contains("pub code: ErrorCode"));
    assert!(!descriptor_source.contains("pub class: ErrorClass"));
    assert!(!descriptor_source.contains("pub fault: FaultAttribution"));
    assert!(!descriptor_source.contains("pub component: ComponentId"));
    assert!(!descriptor_source.contains("pub exposure: Exposure"));
    assert!(!descriptor_source.contains("pub backtrace: BacktracePolicy"));
    assert!(descriptor_source.contains("pub enum ErrorSeverity"));
    assert!(!projection_source.contains("pub remoting: RemotingSpec"));
    assert!(catalog_source.contains("macro_rules! define_error_catalog"));
    for required in [
        "class: $class:path",
        "fault: $fault:path",
        "component: $component:path",
        "backtrace: $backtrace:path",
        "exposure: $exposure:path",
    ] {
        assert!(
            catalog_source.contains(required),
            "missing required macro field {required}"
        );
    }
    assert!(!catalog_source.contains("#[macro_export]"));
    assert!(!crate_root.contains("pub mod catalog;"));
    assert!(!crate_root.contains("pub mod descriptor;"));
    assert!(!crate_root.contains("pub mod projection;"));
}
