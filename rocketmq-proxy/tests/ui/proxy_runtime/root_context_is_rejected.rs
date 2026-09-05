use rocketmq_proxy::ProxyConfig;
use rocketmq_proxy::ProxyRuntime;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;

fn main() {
    let owner = RuntimeOwner::plan(RuntimeConfig::server_default("compile-fail")).expect("test runtime configuration is valid").build().unwrap();
    let _ = ProxyRuntime::builder(
        ProxyConfig::default(),
        owner.root_context(),
        rocketmq_observability::TelemetryHandle::noop(),
    );
}
