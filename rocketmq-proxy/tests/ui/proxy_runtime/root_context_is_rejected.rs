use rocketmq_proxy::ProxyConfig;
use rocketmq_proxy::ProxyRuntime;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;

fn main() {
    let owner = RuntimeOwner::new(RuntimeConfig::server_default("compile-fail")).unwrap();
    let _ = ProxyRuntime::builder(
        ProxyConfig::default(),
        owner.root_context(),
        rocketmq_observability::TelemetryHandle::noop(),
    );
}
