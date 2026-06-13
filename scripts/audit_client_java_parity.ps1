[CmdletBinding()]
param(
    [string]$JavaRocketMQRoot = "D:\Github\Java\rocketmq",
    [switch]$FailOnMissing,
    [string]$OutputFile = "",
    [switch]$RustOnly
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Add-Missing {
    param(
        [System.Collections.Generic.List[string]]$Missing,
        [Parameter(Mandatory = $true)]
        [string]$Message
    )

    $Missing.Add($Message) | Out-Null
}

function Test-RequiredFile {
    param(
        [System.Collections.Generic.List[string]]$Missing,
        [Parameter(Mandatory = $true)]
        [string]$Root,
        [Parameter(Mandatory = $true)]
        [string]$RelativePath,
        [Parameter(Mandatory = $true)]
        [string]$Label
    )

    $path = Join-Path $Root $RelativePath
    if (-not (Test-Path $path)) {
        Add-Missing -Missing $Missing -Message "$Label missing file: $RelativePath"
    }
}

function Test-FileContains {
    param(
        [System.Collections.Generic.List[string]]$Missing,
        [Parameter(Mandatory = $true)]
        [string]$Root,
        [Parameter(Mandatory = $true)]
        [string]$RelativePath,
        [Parameter(Mandatory = $true)]
        [string]$Pattern,
        [Parameter(Mandatory = $true)]
        [string]$Capability
    )

    $path = Join-Path $Root $RelativePath
    if (-not (Test-Path $path)) {
        Add-Missing -Missing $Missing -Message "$Capability missing file: $RelativePath"
        return
    }

    if (-not (Select-String -Path $path -Pattern $Pattern -Quiet)) {
        Add-Missing -Missing $Missing -Message "$Capability missing pattern '$Pattern' in $RelativePath"
    }
}

function Get-PlaceholderHits {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Root,
        [Parameter(Mandatory = $true)]
        [string[]]$RelativeDirectories
    )

    $hits = @()
    foreach ($relativeDirectory in $RelativeDirectories) {
        $directory = Join-Path $Root $relativeDirectory
        if (-not (Test-Path $directory)) {
            continue
        }

        $files = Get-ChildItem -Path $directory -Recurse -File -Include *.rs
        foreach ($file in $files) {
            $matches = Select-String -Path $file.FullName -Pattern '\b(todo!|unimplemented!)|not implemented yet'
            foreach ($match in $matches) {
                $relative = $file.FullName.Substring($Root.Length).TrimStart('\', '/')
                $hits += "${relative}:$($match.LineNumber):$($match.Line.Trim())"
            }
        }
    }

    return $hits
}

$rustRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$missing = [System.Collections.Generic.List[string]]::new()
$javaClientRoot = Join-Path $JavaRocketMQRoot "client\src\main\java\org\apache\rocketmq\client"

if (-not $RustOnly -and -not (Test-Path $javaClientRoot)) {
    Add-Missing -Missing $missing -Message "Java client root missing: $javaClientRoot"
}

$javaOracleFiles = @(
    "MQAdmin.java",
    "common\NameserverAccessConfig.java",
    "consumer\DefaultLitePullConsumer.java",
    "consumer\DefaultMQPushConsumer.java",
    "consumer\LitePullConsumer.java",
    "consumer\MQConsumer.java",
    "consumer\MQPushConsumer.java",
    "consumer\NotifyResult.java",
    "consumer\rebalance\AllocateMessageQueueConsistentHash.java",
    "impl\MQAdminImpl.java",
    "impl\consumer\DefaultLitePullConsumerImpl.java",
    "impl\consumer\DefaultMQPushConsumerImpl.java",
    "impl\producer\DefaultMQProducerImpl.java",
    "producer\DefaultMQProducer.java",
    "producer\MQProducer.java",
    "producer\TransactionMQProducer.java"
)

$rustPeerFiles = @(
    "rocketmq-client\src\admin\default_mq_admin_ext_impl.rs",
    "rocketmq-client\src\admin\mq_admin_ext_async.rs",
    "rocketmq-client\src\base\client_config.rs",
    "rocketmq-client\src\base\mq_admin.rs",
    "rocketmq-client\src\lib.rs",
    "rocketmq-client\src\common\acl.rs",
    "rocketmq-client\src\common\acl_client_rpc_hook.rs",
    "rocketmq-client\src\common\nameserver_access_config.rs",
    "rocketmq-client\src\consumer\default_lite_pull_consumer.rs",
    "rocketmq-client\src\consumer\default_mq_push_consumer.rs",
    "rocketmq-client\src\consumer\lite_pull_consumer.rs",
    "rocketmq-client\src\consumer\mq_consumer.rs",
    "rocketmq-client\src\consumer\mq_push_consumer.rs",
    "rocketmq-client\src\consumer\notify_result.rs",
    "rocketmq-client\src\consumer\consumer_impl\default_lite_pull_consumer_impl.rs",
    "rocketmq-client\src\consumer\consumer_impl\default_mq_push_consumer_impl.rs",
    "rocketmq-client\src\consumer\rebalance_strategy\allocate_message_queue_consistent_hash.rs",
    "rocketmq-client\src\producer\default_mq_producer.rs",
    "rocketmq-client\src\producer\mq_producer.rs",
    "rocketmq-client\src\producer\transaction_mq_producer.rs",
    "rocketmq-client\src\producer\producer_impl\default_mq_producer_impl.rs",
    "rocketmq-remoting\Cargo.toml",
    "rocketmq-remoting\src\clients\client.rs",
    "rocketmq-tools\rocketmq-admin\rocketmq-admin-core\src\admin\default_mq_admin_ext.rs",
    "rocketmq-client\examples\benchmark\client_production_benchmark.rs",
    "rocketmq-client\tests\public_api_exports_test.rs"
)

$javaSpecificRustPeerFiles = @(
    "scripts\java\ClientCompatibilitySmoke.java",
    "scripts\run_client_java_gate.ps1"
)

if (-not $RustOnly) {
    $rustPeerFiles += $javaSpecificRustPeerFiles
}

if (-not $RustOnly) {
    foreach ($file in $javaOracleFiles) {
        Test-RequiredFile -Missing $missing -Root $javaClientRoot -RelativePath $file -Label "Java oracle"
    }
}

foreach ($file in $rustPeerFiles) {
    Test-RequiredFile -Missing $missing -Root $rustRoot -RelativePath $file -Label "Rust peer"
}

$capabilityChecks = @(
    @{ File = "rocketmq-client\src\producer\default_mq_producer.rs"; Pattern = "pub struct DefaultMQProducer"; Capability = "DefaultMQProducer facade" },
    @{ File = "rocketmq-client\src\producer\default_mq_producer.rs"; Pattern = "pub async fn send<"; Capability = "producer sync send facade" },
    @{ File = "rocketmq-client\src\producer\default_mq_producer.rs"; Pattern = "pub async fn send_with_callback"; Capability = "producer async callback send facade" },
    @{ File = "rocketmq-client\src\producer\default_mq_producer.rs"; Pattern = "pub async fn send_oneway"; Capability = "producer oneway send facade" },
    @{ File = "rocketmq-client\src\producer\default_mq_producer.rs"; Pattern = "pub async fn send_batch"; Capability = "producer batch send facade" },
    @{ File = "rocketmq-client\src\producer\default_mq_producer.rs"; Pattern = "pub async fn request<"; Capability = "producer request-reply facade" },
    @{ File = "rocketmq-client\src\producer\default_mq_producer.rs"; Pattern = "pub async fn recall_message"; Capability = "producer recall facade" },
    @{ File = "rocketmq-client\src\producer\default_mq_producer.rs"; Pattern = "pub async fn fetch_publish_message_queues"; Capability = "producer fetchPublishMessageQueues facade" },
    @{ File = "rocketmq-client\src\producer\default_mq_producer.rs"; Pattern = "pub fn get_default_mq_producer_impl"; Capability = "producer getDefaultMQProducerImpl Java-style getter" },
    @{ File = "rocketmq-client\src\producer\default_mq_producer.rs"; Pattern = "pub async fn send_to_queue"; Capability = "producer queue send facade" },
    @{ File = "rocketmq-client\src\producer\default_mq_producer.rs"; Pattern = "pub async fn send_with_selector"; Capability = "producer selector send facade" },
    @{ File = "rocketmq-client\src\producer\default_mq_producer.rs"; Pattern = "pub async fn send_oneway_with_selector"; Capability = "producer selector oneway facade" },
    @{ File = "rocketmq-client\src\producer\default_mq_producer.rs"; Pattern = "pub async fn send_batch_to_queue"; Capability = "producer batch queue send facade" },
    @{ File = "rocketmq-client\src\producer\default_mq_producer.rs"; Pattern = "pub async fn request_with_callback"; Capability = "producer async request facade" },
    @{ File = "rocketmq-client\src\producer\default_mq_producer.rs"; Pattern = "pub async fn request_to_queue"; Capability = "producer queue request facade" },
    @{ File = "rocketmq-client\src\producer\default_mq_producer.rs"; Pattern = "pub async fn send_direct"; Capability = "producer direct send facade" },
    @{ File = "rocketmq-client\src\producer\default_mq_producer.rs"; Pattern = "pub async fn send_by_accumulator"; Capability = "producer accumulator send facade" },
    @{ File = "rocketmq-client\src\producer\default_mq_producer.rs"; Pattern = "pub fn add_retry_response_code"; Capability = "producer retry response code config" },
    @{ File = "rocketmq-client\src\producer\default_mq_producer.rs"; Pattern = "pub fn set_auto_batch"; Capability = "producer auto-batch config" },
    @{ File = "rocketmq-client\src\producer\default_mq_producer.rs"; Pattern = "pub fn get_auto_batch"; Capability = "producer auto-batch getter" },
    @{ File = "rocketmq-client\src\producer\default_mq_producer.rs"; Pattern = "pub fn set_batch_max_delay_ms"; Capability = "producer batchMaxDelayMs config" },
    @{ File = "rocketmq-client\src\producer\default_mq_producer.rs"; Pattern = "pub fn set_batch_max_bytes"; Capability = "producer batchMaxBytes config" },
    @{ File = "rocketmq-client\src\producer\default_mq_producer.rs"; Pattern = "pub fn set_total_batch_max_bytes"; Capability = "producer totalBatchMaxBytes config" },
    @{ File = "rocketmq-client\src\producer\default_mq_producer.rs"; Pattern = "pub fn set_enable_backpressure_for_async_mode"; Capability = "producer async backpressure config" },
    @{ File = "rocketmq-client\src\producer\default_mq_producer.rs"; Pattern = "pub fn set_send_latency_fault_enable"; Capability = "producer latency fault config" },
    @{ File = "rocketmq-client\src\producer\default_mq_producer.rs"; Pattern = "pub fn set_compress_type"; Capability = "producer compression type config" },
    @{ File = "rocketmq-client\src\producer\default_mq_producer.rs"; Pattern = "pub fn set_compress_level"; Capability = "producer compression level config" },
    @{ File = "rocketmq-client\src\producer\default_mq_producer.rs"; Pattern = "pub fn init_produce_accumulator"; Capability = "producer accumulator initialization" },
    @{ File = "rocketmq-client\src\producer\transaction_mq_producer.rs"; Pattern = "pub struct TransactionMQProducer"; Capability = "TransactionMQProducer type" },
    @{ File = "rocketmq-client\src\producer\transaction_mq_producer.rs"; Pattern = "pub async fn send_message_in_transaction"; Capability = "transaction producer send" },
    @{ File = "rocketmq-client\src\producer\transaction_mq_producer.rs"; Pattern = "pub fn get_transaction_check_listener"; Capability = "transaction check listener getter" },
    @{ File = "rocketmq-client\src\producer\transaction_mq_producer.rs"; Pattern = "pub fn set_transaction_check_listener"; Capability = "transaction check listener setter" },
    @{ File = "rocketmq-client\src\producer\transaction_mq_producer.rs"; Pattern = "pub fn set_check_thread_pool_min_size"; Capability = "transaction check thread-pool min config" },
    @{ File = "rocketmq-client\src\producer\transaction_mq_producer.rs"; Pattern = "pub fn set_check_thread_pool_max_size"; Capability = "transaction check thread-pool max config" },
    @{ File = "rocketmq-client\src\producer\transaction_mq_producer.rs"; Pattern = "pub fn set_check_request_hold_max"; Capability = "transaction check request hold config" },
    @{ File = "rocketmq-client\src\producer\transaction_mq_producer.rs"; Pattern = "pub fn set_executor_service"; Capability = "transaction check executor config" },
    @{ File = "rocketmq-client\src\producer\queue_selector\select_message_queue_by_machine_room.rs"; Pattern = "SelectMessageQueueByMachineRoom"; Capability = "producer machine-room queue selector" },
    @{ File = "rocketmq-client\src\consumer\default_mq_push_consumer.rs"; Pattern = "pub struct DefaultMQPushConsumer"; Capability = "DefaultMQPushConsumer facade" },
    @{ File = "rocketmq-client\src\consumer\default_mq_push_consumer.rs"; Pattern = "pub async fn shutdown"; Capability = "push shutdown facade" },
    @{ File = "rocketmq-client\src\consumer\default_mq_push_consumer.rs"; Pattern = "pub async fn send_message_back\("; Capability = "push sendMessageBack facade" },
    @{ File = "rocketmq-client\src\consumer\default_mq_push_consumer.rs"; Pattern = "pub async fn send_message_back_to_origin_broker"; Capability = "push sendMessageBack origin broker facade" },
    @{ File = "rocketmq-client\src\consumer\default_mq_push_consumer.rs"; Pattern = "pub async fn fetch_subscribe_message_queues"; Capability = "push fetchSubscribeMessageQueues facade" },
    @{ File = "rocketmq-client\src\consumer\default_mq_push_consumer.rs"; Pattern = "pub async fn subscribe_with_selector"; Capability = "push selector subscribe facade" },
    @{ File = "rocketmq-client\src\consumer\default_mq_push_consumer.rs"; Pattern = "pub async fn unsubscribe"; Capability = "push unsubscribe facade" },
    @{ File = "rocketmq-client\src\consumer\default_mq_push_consumer.rs"; Pattern = "pub async fn suspend"; Capability = "push suspend facade" },
    @{ File = "rocketmq-client\src\consumer\default_mq_push_consumer.rs"; Pattern = "pub async fn resume"; Capability = "push resume facade" },
    @{ File = "rocketmq-client\src\consumer\default_mq_push_consumer.rs"; Pattern = "pub fn update_core_pool_size"; Capability = "push thread-pool update facade" },
    @{ File = "rocketmq-client\src\consumer\default_mq_push_consumer.rs"; Pattern = "pub async fn consumer_running_info"; Capability = "push consumerRunningInfo facade" },
    @{ File = "rocketmq-client\src\consumer\default_mq_push_consumer.rs"; Pattern = "pub fn is_pause"; Capability = "push pause state getter" },
    @{ File = "rocketmq-client\src\consumer\default_mq_push_consumer.rs"; Pattern = "pub fn is_consume_orderly"; Capability = "push orderly mode getter" },
    @{ File = "rocketmq-client\src\consumer\default_mq_push_consumer.rs"; Pattern = "pub fn register_consume_message_hook"; Capability = "push consume trace hook registration" },
    @{ File = "rocketmq-client\src\consumer\default_mq_push_consumer.rs"; Pattern = "pub fn register_message_listener"; Capability = "push message listener registration" },
    @{ File = "rocketmq-client\src\consumer\default_mq_push_consumer.rs"; Pattern = "pub fn set_message_queue_listener"; Capability = "push message queue listener config" },
    @{ File = "rocketmq-client\src\consumer\default_mq_push_consumer.rs"; Pattern = "pub fn set_consume_thread_min"; Capability = "push consume thread min config" },
    @{ File = "rocketmq-client\src\consumer\default_mq_push_consumer.rs"; Pattern = "pub fn set_consume_thread_max"; Capability = "push consume thread max config" },
    @{ File = "rocketmq-client\src\consumer\default_mq_push_consumer.rs"; Pattern = "pub fn set_max_reconsume_times"; Capability = "push max reconsume times config" },
    @{ File = "rocketmq-client\src\consumer\default_mq_push_consumer.rs"; Pattern = "pub fn set_message_model"; Capability = "push message model config" },
    @{ File = "rocketmq-client\src\consumer\consumer_impl\default_mq_push_consumer_impl.rs"; Pattern = "pub async fn send_message_back"; Capability = "push sendMessageBack implementation" },
    @{ File = "rocketmq-client\src\consumer\default_lite_pull_consumer.rs"; Pattern = "pub struct DefaultLitePullConsumer"; Capability = "DefaultLitePullConsumer facade" },
    @{ File = "rocketmq-client\src\consumer\default_lite_pull_consumer.rs"; Pattern = "pub async fn subscribe"; Capability = "LitePull subscribe facade" },
    @{ File = "rocketmq-client\src\consumer\default_lite_pull_consumer.rs"; Pattern = "pub async fn unsubscribe"; Capability = "LitePull unsubscribe facade" },
    @{ File = "rocketmq-client\src\consumer\default_lite_pull_consumer.rs"; Pattern = "pub async fn assign"; Capability = "LitePull assign facade" },
    @{ File = "rocketmq-client\src\consumer\default_lite_pull_consumer.rs"; Pattern = "pub async fn assignment"; Capability = "LitePull assignment facade" },
    @{ File = "rocketmq-client\src\consumer\default_lite_pull_consumer.rs"; Pattern = "pub async fn poll"; Capability = "LitePull owned poll facade" },
    @{ File = "rocketmq-client\src\consumer\default_lite_pull_consumer.rs"; Pattern = "pub async fn poll_zero_copy"; Capability = "LitePull zero-copy poll facade" },
    @{ File = "rocketmq-client\src\consumer\default_lite_pull_consumer.rs"; Pattern = "pub async fn seek"; Capability = "LitePull seek facade" },
    @{ File = "rocketmq-client\src\consumer\default_lite_pull_consumer.rs"; Pattern = "pub async fn seek_to_begin"; Capability = "LitePull seekToBegin facade" },
    @{ File = "rocketmq-client\src\consumer\default_lite_pull_consumer.rs"; Pattern = "pub async fn seek_to_end"; Capability = "LitePull seekToEnd facade" },
    @{ File = "rocketmq-client\src\consumer\default_lite_pull_consumer.rs"; Pattern = "pub async fn pause"; Capability = "LitePull pause facade" },
    @{ File = "rocketmq-client\src\consumer\default_lite_pull_consumer.rs"; Pattern = "pub async fn resume"; Capability = "LitePull resume facade" },
    @{ File = "rocketmq-client\src\consumer\default_lite_pull_consumer.rs"; Pattern = "pub async fn commit"; Capability = "LitePull commit facade" },
    @{ File = "rocketmq-client\src\consumer\default_lite_pull_consumer.rs"; Pattern = "pub async fn commit_sync"; Capability = "LitePull commitSync facade" },
    @{ File = "rocketmq-client\src\consumer\default_lite_pull_consumer.rs"; Pattern = "pub async fn commit_with_map"; Capability = "LitePull commit map facade" },
    @{ File = "rocketmq-client\src\consumer\default_lite_pull_consumer.rs"; Pattern = "pub async fn commit_with_set"; Capability = "LitePull commit messageQueues facade" },
    @{ File = "rocketmq-client\src\consumer\default_lite_pull_consumer.rs"; Pattern = "pub async fn committed"; Capability = "LitePull committed facade" },
    @{ File = "rocketmq-client\src\consumer\default_lite_pull_consumer.rs"; Pattern = "pub async fn fetch_message_queues"; Capability = "LitePull fetchMessageQueues facade" },
    @{ File = "rocketmq-client\src\consumer\default_lite_pull_consumer.rs"; Pattern = "pub async fn offset_for_timestamp"; Capability = "LitePull offsetForTimestamp facade" },
    @{ File = "rocketmq-client\src\consumer\default_lite_pull_consumer.rs"; Pattern = "pub async fn min_offset"; Capability = "LitePull minOffset facade" },
    @{ File = "rocketmq-client\src\consumer\default_lite_pull_consumer.rs"; Pattern = "pub async fn max_offset"; Capability = "LitePull maxOffset facade" },
    @{ File = "rocketmq-client\src\consumer\default_lite_pull_consumer.rs"; Pattern = "pub async fn earliest_msg_store_time"; Capability = "LitePull earliestMsgStoreTime facade" },
    @{ File = "rocketmq-client\src\consumer\default_lite_pull_consumer.rs"; Pattern = "pub async fn is_auto_commit"; Capability = "LitePull isAutoCommit facade" },
    @{ File = "rocketmq-client\src\consumer\default_lite_pull_consumer.rs"; Pattern = "pub async fn set_auto_commit"; Capability = "LitePull autoCommit config" },
    @{ File = "rocketmq-client\src\consumer\default_lite_pull_consumer.rs"; Pattern = "pub async fn update_name_server_address"; Capability = "LitePull nameserver update facade" },
    @{ File = "rocketmq-client\src\consumer\default_lite_pull_consumer.rs"; Pattern = "pub async fn register_topic_message_queue_change_listener"; Capability = "LitePull topic queue change listener registration" },
    @{ File = "rocketmq-client\src\consumer\default_lite_pull_consumer.rs"; Pattern = "pub async fn set_message_queue_listener"; Capability = "LitePull message queue listener config" },
    @{ File = "rocketmq-client\src\consumer\default_lite_pull_consumer.rs"; Pattern = "pub async fn set_allocate_message_queue_strategy"; Capability = "LitePull allocate strategy config" },
    @{ File = "rocketmq-client\src\consumer\default_lite_pull_consumer.rs"; Pattern = "pub async fn set_offset_store"; Capability = "LitePull offset store config" },
    @{ File = "rocketmq-client\src\consumer\default_lite_pull_consumer.rs"; Pattern = "pub async fn set_pull_batch_size"; Capability = "LitePull pull batch size config" },
    @{ File = "rocketmq-client\src\consumer\default_lite_pull_consumer.rs"; Pattern = "pub async fn set_pull_thread_nums"; Capability = "LitePull pull thread config" },
    @{ File = "rocketmq-client\src\consumer\default_lite_pull_consumer.rs"; Pattern = "pub async fn set_consume_from_where"; Capability = "LitePull consumeFromWhere config" },
    @{ File = "rocketmq-client\src\consumer\default_lite_pull_consumer.rs"; Pattern = "pub async fn build_subscriptions_for_heartbeat"; Capability = "LitePull heartbeat subscription build" },
    @{ File = "rocketmq-client\src\consumer\default_lite_pull_consumer.rs"; Pattern = "pub fn set_enable_msg_trace"; Capability = "LitePull trace enable config" },
    @{ File = "rocketmq-client\src\consumer\default_lite_pull_consumer.rs"; Pattern = "pub fn set_customized_trace_topic"; Capability = "LitePull custom trace topic config" },
    @{ File = "rocketmq-client\src\consumer\consumer_impl\default_lite_pull_consumer_impl.rs"; Pattern = "pull_inner"; Capability = "LitePull main pull chain" },
    @{ File = "rocketmq-client\src\consumer\rebalance_strategy\allocate_message_queue_consistent_hash.rs"; Pattern = "AllocateMessageQueueConsistentHash"; Capability = "consistent-hash rebalance strategy" },
    @{ File = "rocketmq-client\src\consumer\notify_result.rs"; Pattern = "pub struct NotifyResult"; Capability = "NotifyResult modern type" },
    @{ File = "rocketmq-client\tests\public_api_exports_test.rs"; Pattern = "NotifyResult\{hasMsg=true, pollingFull=true\}"; Capability = "NotifyResult public API test" },
    @{ File = "rocketmq-client\src\common\nameserver_access_config.rs"; Pattern = "pub struct NameserverAccessConfig"; Capability = "NameserverAccessConfig modern type" },
    @{ File = "rocketmq-client\src\common\acl_client_rpc_hook.rs"; Pattern = "pub struct AclClientRPCHook"; Capability = "ACL RPC hook public type" },
    @{ File = "rocketmq-client\src\common\acl_client_rpc_hook.rs"; Pattern = "impl RPCHook for AclClientRPCHook"; Capability = "ACL RPCHook implementation" },
    @{ File = "rocketmq-client\src\common\acl.rs"; Pattern = "combine_request_content"; Capability = "ACL Java-compatible request content combiner" },
    @{ File = "rocketmq-client\src\base\client_config.rs"; Pattern = "use_tls"; Capability = "ClientConfig TLS switch" },
    @{ File = "rocketmq-remoting\Cargo.toml"; Pattern = 'default = \["tls"\]'; Capability = "remoting default TLS feature" },
    @{ File = "rocketmq-remoting\src\clients\client.rs"; Pattern = "rocketmq-remoting was compiled without the tls feature"; Capability = "typed TLS config error without feature" },
    @{ File = "rocketmq-remoting\src\clients\client.rs"; Pattern = "let _ = rx\.await"; Capability = "remoting client callback invoke waits for response completion" },
    @{ File = "rocketmq-client\src\trace\async_trace_dispatcher.rs"; Pattern = "AsyncTraceDispatcher"; Capability = "trace dispatcher" },
    @{ File = "rocketmq-client\src\lib.rs"; Pattern = "pub use crate::trace::async_trace_dispatcher::AsyncTraceDispatcher"; Capability = "trace dispatcher public root export" },
    @{ File = "rocketmq-client\src\lib.rs"; Pattern = "pub use crate::trace::trace_dispatcher::TraceDispatcher"; Capability = "trace dispatcher trait public root export" },
    @{ File = "rocketmq-client\src\lib.rs"; Pattern = "pub use crate::trace::trace_dispatcher::ArcTraceDispatcher"; Capability = "trace dispatcher shared type public root export" },
    @{ File = "rocketmq-client\src\lib.rs"; Pattern = "pub use crate::trace::trace_dispatcher::Type as TraceDispatcherOperation"; Capability = "trace dispatcher operation public root export" },
    @{ File = "rocketmq-client\src\lib.rs"; Pattern = "pub use crate::trace::trace_dispatcher_type::TraceDispatcherType"; Capability = "trace dispatcher type public root export" },
    @{ File = "rocketmq-client\src\lib.rs"; Pattern = "pub use crate::trace::trace_type::TraceType"; Capability = "trace type public root export" },
    @{ File = "rocketmq-client\tests\public_api_exports_test.rs"; Pattern = "crate_root_exports_trace_dispatcher_api_for_custom_trace_wiring"; Capability = "trace dispatcher public API wiring test" },
    @{ File = "rocketmq-client\src\trace\hook\send_message_trace_hook_impl.rs"; Pattern = "SendMessageTraceHookImpl"; Capability = "send trace hook" },
    @{ File = "rocketmq-client\src\trace\hook\end_transaction_trace_hook_impl.rs"; Pattern = "EndTransactionTraceHookImpl"; Capability = "transaction trace hook" },
    @{ File = "rocketmq-client\src\trace\hook\default_recall_message_trace_hook.rs"; Pattern = "DefaultRecallMessageTraceHook"; Capability = "recall trace hook" },
    @{ File = "rocketmq-client\src\admin\mq_admin_ext_async.rs"; Pattern = "clean_expired_consumer_queue_by_addr"; Capability = "admin cleanExpiredConsumerQueue by addr" },
    @{ File = "rocketmq-client\src\admin\mq_admin_ext_async.rs"; Pattern = "delete_expired_commit_log_by_addr"; Capability = "admin deleteExpiredCommitLog by addr" },
    @{ File = "rocketmq-client\src\admin\mq_admin_ext_async.rs"; Pattern = "clean_unused_topic_by_addr"; Capability = "admin cleanUnusedTopic by addr" },
    @{ File = "rocketmq-client\src\admin\mq_admin_ext_async.rs"; Pattern = "export_pop_records"; Capability = "admin exportPopRecords" },
    @{ File = "rocketmq-client\src\admin\mq_admin_ext_async.rs"; Pattern = "async fn list_user"; Capability = "admin Java-style listUser alias" },
    @{ File = "rocketmq-client\src\admin\default_mq_admin_ext_impl.rs"; Pattern = "self\.get_topic_cluster_list\(topic\)\.await"; Capability = "admin getClusterList delegates to real topic cluster lookup" },
    @{ File = "rocketmq-client\src\admin\default_mq_admin_ext_impl.rs"; Pattern = "merge_consume_status_result"; Capability = "admin getConsumeStatus merges all broker responses" },
    @{ File = "rocketmq-client\src\implementation\mq_client_api_impl.rs"; Pattern = "cluster_names_for_topic_route"; Capability = "client API getClusterList maps topic brokers to clusters" },
    @{ File = "rocketmq-client\tests\broker_backed_smoke.rs"; Pattern = "ROCKETMQ_NAMESRV_ADDR"; Capability = "broker-backed smoke harness" },
    @{ File = "rocketmq-client\tests\broker_backed_smoke.rs"; Pattern = "broker_backed_producer_sync_oneway_batch_smoke"; Capability = "producer sync/oneway/batch broker-backed smoke" },
    @{ File = "rocketmq-client\tests\broker_backed_smoke.rs"; Pattern = "broker_backed_producer_async_callback_smoke"; Capability = "producer async callback broker-backed smoke" },
    @{ File = "rocketmq-client\tests\broker_backed_smoke.rs"; Pattern = "broker_backed_producer_mq_admin_offsets_smoke"; Capability = "producer MQAdmin offsets broker-backed smoke" },
    @{ File = "rocketmq-client\tests\broker_backed_smoke.rs"; Pattern = "broker_backed_producer_create_topic_smoke"; Capability = "producer create-topic broker-backed smoke" },
    @{ File = "rocketmq-client\tests\broker_backed_smoke.rs"; Pattern = "broker_backed_acl_producer_send_smoke"; Capability = "ACL producer broker-backed smoke" },
    @{ File = "rocketmq-client\tests\broker_backed_smoke.rs"; Pattern = "broker_backed_tls_producer_send_smoke"; Capability = "TLS producer broker-backed smoke" },
    @{ File = "rocketmq-client\tests\broker_backed_smoke.rs"; Pattern = "broker_backed_trace_producer_send_smoke"; Capability = "trace producer broker-backed smoke" },
    @{ File = "rocketmq-client\tests\broker_backed_smoke.rs"; Pattern = "broker_backed_producer_request_reply_smoke"; Capability = "producer request-reply broker-backed smoke" },
    @{ File = "rocketmq-client\tests\broker_backed_smoke.rs"; Pattern = "broker_backed_producer_recall_smoke"; Capability = "producer recall broker-backed smoke" },
    @{ File = "rocketmq-client\tests\broker_backed_smoke.rs"; Pattern = "broker_backed_transaction_producer_smoke"; Capability = "transaction broker-backed smoke" },
    @{ File = "rocketmq-client\tests\broker_backed_smoke.rs"; Pattern = "broker_backed_lite_pull_assign_offset_smoke"; Capability = "LitePull assign/offset broker-backed smoke" },
    @{ File = "rocketmq-client\tests\broker_backed_smoke.rs"; Pattern = "broker_backed_lite_pull_subscribe_poll_smoke"; Capability = "LitePull broker-backed smoke" },
    @{ File = "rocketmq-client\tests\broker_backed_smoke.rs"; Pattern = "broker_backed_push_consumer_concurrent_smoke"; Capability = "push broker-backed smoke" },
    @{ File = "rocketmq-client\tests\broker_backed_smoke.rs"; Pattern = "broker_backed_push_consumer_retry_smoke"; Capability = "push retry broker-backed smoke" },
    @{ File = "rocketmq-client\tests\broker_backed_smoke.rs"; Pattern = "broker_backed_push_consumer_orderly_smoke"; Capability = "push orderly broker-backed smoke" },
    @{ File = "scripts\java\ClientCompatibilitySmoke.java"; Pattern = "ProducerOneway"; Capability = "Java compatibility oneway smoke" },
    @{ File = "scripts\java\ClientCompatibilitySmoke.java"; Pattern = "ProducerBatch"; Capability = "Java compatibility batch smoke" },
    @{ File = "scripts\java\ClientCompatibilitySmoke.java"; Pattern = "ProducerBatchBenchmark"; Capability = "Java producer batch performance harness" },
    @{ File = "scripts\java\ClientCompatibilitySmoke.java"; Pattern = "TransactionProducer"; Capability = "Java compatibility transaction smoke" },
    @{ File = "scripts\java\ClientCompatibilitySmoke.java"; Pattern = "PushConcurrent"; Capability = "Java compatibility push smoke" },
    @{ File = "scripts\java\ClientCompatibilitySmoke.java"; Pattern = "LitePullAssign"; Capability = "Java compatibility LitePull assign smoke" },
    @{ File = "scripts\java\ClientCompatibilitySmoke.java"; Pattern = "LitePullSubscribe"; Capability = "Java compatibility LitePull subscribe smoke" },
    @{ File = "scripts\java\ClientCompatibilitySmoke.java"; Pattern = "LitePullBenchmark"; Capability = "Java LitePull performance harness" },
    @{ File = "scripts\java\ClientCompatibilitySmoke.java"; Pattern = "RequestReply"; Capability = "Java compatibility request-reply smoke" },
    @{ File = "scripts\run_client_java_gate.ps1"; Pattern = "java-compatibility-harness-run"; Capability = "Java compatibility harness gate command" },
    @{ File = "scripts\run_client_production_readiness.ps1"; Pattern = "java-compatibility-harness-compile"; Capability = "readiness Java harness compile gate" },
    @{ File = "rocketmq-client\examples\benchmark\client_production_benchmark.rs"; Pattern = "RocketMQ Rust client production benchmark"; Capability = "Rust broker producer performance harness" },
    @{ File = "rocketmq-client\examples\benchmark\client_production_benchmark.rs"; Pattern = "Operation::Send"; Capability = "Rust benchmark Java-compatible TPS summary" },
    @{ File = "rocketmq-client\examples\benchmark\client_production_benchmark.rs"; Pattern = "Operation::Consume"; Capability = "Rust LitePull benchmark Java-compatible TPS summary" },
    @{ File = "scripts\run_client_production_bench.ps1"; Pattern = "Assert-BenchmarkComparison"; Capability = "Rust/Java performance comparison gate" },
    @{ File = "scripts\run_client_production_bench.ps1"; Pattern = "MinRustToJavaTpsRatio"; Capability = "Rust/Java TPS threshold" },
    @{ File = "scripts\run_client_production_readiness.ps1"; Pattern = "RustBrokerBenchScenario"; Capability = "readiness selects Rust/Java broker benchmark scenario" },
    @{ File = "scripts\run_client_production_readiness.ps1"; Pattern = '\[switch\]\$RustOnly'; Capability = "readiness Rust-only gate mode" },
    @{ File = "scripts\run_client_production_readiness.ps1"; Pattern = "skipped-rust-only"; Capability = "readiness skips Java gates in Rust-only mode" },
    @{ File = "rocketmq-client\benches\client_hot_path_benchmark.rs"; Pattern = "bench_acl_signing"; Capability = "ACL signing benchmark" },
    @{ File = "rocketmq-client\benches\client_hot_path_benchmark.rs"; Pattern = "bench_lite_pull_batch_clone"; Capability = "LitePull owned vs zero-copy benchmark" },
    @{ File = "rocketmq-remoting\benches\encode_decode_bench.rs"; Pattern = "bench_encode_json_simple"; Capability = "remoting encode/decode benchmark" },
    @{ File = "rocketmq-auth\benches\auth_hot_path_bench.rs"; Pattern = "criterion_group"; Capability = "auth hot-path benchmark" },
    @{ File = "rocketmq-client\tests\public_api_exports_test.rs"; Pattern = "crate_root_legacy_java_apis_return_typed_unsupported_errors"; Capability = "deprecated Java API typed unsupported public API test" }
)

if ($RustOnly) {
    $capabilityChecks = @($capabilityChecks | Where-Object {
            $_.File -ne "scripts\java\ClientCompatibilitySmoke.java" -and
            $_.File -ne "scripts\run_client_java_gate.ps1" -and
            $_.File -ne "scripts\run_client_production_bench.ps1" -and
            $_.Capability -ne "readiness Java harness compile gate" -and
            $_.Capability -ne "readiness selects Rust/Java broker benchmark scenario"
        })
}

foreach ($check in $capabilityChecks) {
    Test-FileContains `
        -Missing $missing `
        -Root $rustRoot `
        -RelativePath $check.File `
        -Pattern $check.Pattern `
        -Capability $check.Capability
}

$placeholderHits = @(Get-PlaceholderHits `
    -Root $rustRoot `
    -RelativeDirectories @(
        "rocketmq-client\src",
        "rocketmq-remoting\src",
        "rocketmq-common\src",
        "rocketmq-tools\rocketmq-admin\rocketmq-admin-core\src"
    ))

foreach ($hit in $placeholderHits) {
    Add-Missing -Missing $missing -Message "placeholder implementation remains: $hit"
}

$intentionalExclusions = @(
    "deprecated DefaultMQPullConsumer and MQPullConsumerScheduleService",
    "filter-class subscribe API",
    "OpenTracing hooks",
    "ACL 1.x acl_file_full_path global white-address config",
    "syncBrokerMemberGroup controllerAddr path"
)

$lines = [System.Collections.Generic.List[string]]::new()
if ($RustOnly) {
    $lines.Add("RocketMQ client Rust production audit") | Out-Null
} else {
    $lines.Add("RocketMQ client Java parity audit") | Out-Null
}
$lines.Add("rustRoot=$rustRoot") | Out-Null
$lines.Add("rustOnly=$($RustOnly.IsPresent)") | Out-Null
if ($RustOnly) {
    $lines.Add("javaRoot=skipped") | Out-Null
    $lines.Add("javaOracleFiles=0 (skipped $($javaOracleFiles.Count))") | Out-Null
} else {
    $lines.Add("javaRoot=$JavaRocketMQRoot") | Out-Null
    $lines.Add("javaOracleFiles=$($javaOracleFiles.Count)") | Out-Null
}
$lines.Add("rustPeerFiles=$($rustPeerFiles.Count)") | Out-Null
$lines.Add("capabilityChecks=$($capabilityChecks.Count)") | Out-Null
$lines.Add("placeholderHits=$($placeholderHits.Count)") | Out-Null
$lines.Add("missing=$($missing.Count)") | Out-Null
$lines.Add("") | Out-Null
$lines.Add("Intentional exclusions:") | Out-Null
foreach ($exclusion in $intentionalExclusions) {
    $lines.Add("- $exclusion") | Out-Null
}

if ($missing.Count -gt 0) {
    $lines.Add("") | Out-Null
    $lines.Add("Missing or incomplete checks:") | Out-Null
    foreach ($item in $missing) {
        $lines.Add("- $item") | Out-Null
    }
} else {
    $lines.Add("") | Out-Null
    $lines.Add("Result=PASS") | Out-Null
}

$text = $lines -join [Environment]::NewLine
Write-Output $text

if (-not [string]::IsNullOrWhiteSpace($OutputFile)) {
    $outputDirectory = Split-Path -Path $OutputFile -Parent
    if (-not [string]::IsNullOrWhiteSpace($outputDirectory) -and -not (Test-Path $outputDirectory)) {
        New-Item -ItemType Directory -Path $outputDirectory | Out-Null
    }

    $text | Set-Content -Path $OutputFile
}

if ($FailOnMissing -and $missing.Count -gt 0) {
    throw "RocketMQ client Java parity audit failed with $($missing.Count) missing check(s)."
}
