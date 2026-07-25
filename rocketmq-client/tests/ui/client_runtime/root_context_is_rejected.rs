use rocketmq_client_rust::ClientRuntime;
use rocketmq_client_rust::ClientRuntimeConfig;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;

fn main() {
    let owner = RuntimeOwner::new(RuntimeConfig::server_default("compile-fail")).unwrap();
    let _ = ClientRuntime::new(owner.root_context(), ClientRuntimeConfig::default());
}
