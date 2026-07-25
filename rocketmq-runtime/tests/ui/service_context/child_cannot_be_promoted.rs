use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::RootServiceContext;

fn promote(child: ChildServiceContext) -> RootServiceContext {
    child.root_context()
}

fn main() {}
