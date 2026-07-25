use rocketmq_runtime::RootServiceContext;
use rocketmq_store::factory::StoreFactory;
use rocketmq_store::factory::StoreFactoryConfig;

fn open_with_root(config: StoreFactoryConfig, root: RootServiceContext) {
    let _ = StoreFactory::open(config, root);
}

fn main() {}
