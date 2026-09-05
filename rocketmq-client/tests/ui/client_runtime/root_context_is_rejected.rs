use rocketmq_client_rust::ClientRuntime;
use rocketmq_client_rust::ClientRuntimeConfig;
use rocketmq_client_rust::TelemetryHandle;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;

fn main() {
    let owner = RuntimeOwner::plan(RuntimeConfig::server_default("compile-fail")).expect("test runtime configuration is valid").build().unwrap();
    let _ = ClientRuntime::try_new(
        owner.root_context(),
        ClientRuntimeConfig::default(),
        TelemetryHandle::noop(),
    );
}
