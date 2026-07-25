use rocketmq_proxy::ProxyConfig;
use rocketmq_proxy::ProxyRuntime;

fn main() {
    let _ = ProxyRuntime::builder(ProxyConfig::default());
}
