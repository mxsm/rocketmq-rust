use criterion::criterion_group;
use criterion::criterion_main;
use criterion::Criterion;
use rocketmq_client_rust::producer::message_queue_selector::MessageQueueSelector;
use rocketmq_client_rust::producer::queue_selector::SelectMessageQueueByHash;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::message_single::Message;
use std::cell::RefCell;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::hint::black_box;

thread_local! {
    static HASHER: RefCell<DefaultHasher> = RefCell::new(DefaultHasher::new());
}

/// Queue selector using thread-local hasher for performance comparison.
struct SelectMessageQueueByHashCached;

impl<M, A> MessageQueueSelector<M, A> for SelectMessageQueueByHashCached
where
    M: rocketmq_common::common::message::MessageTrait,
    A: Hash,
{
    #[inline]
    fn select(&self, mqs: &[MessageQueue], _msg: &M, arg: &A) -> Option<MessageQueue> {
        if mqs.is_empty() {
            return None;
        }

        let hash_code = HASHER.with(|hasher| {
            let mut h = hasher.borrow_mut();
            arg.hash(&mut *h);
            let code = h.finish();
            *h = DefaultHasher::new();
            code
        });

        let index = (hash_code % mqs.len() as u64) as usize;
        mqs.get(index).cloned()
    }
}

fn bench_hasher_creation(c: &mut Criterion) {
    c.bench_function("hasher_new", |b| {
        b.iter(|| {
            let hasher = black_box(DefaultHasher::new());
            black_box(hasher);
        })
    });
}

fn bench_select_original(c: &mut Criterion) {
    let selector = SelectMessageQueueByHash;
    let queues = vec![
        MessageQueue::from_parts("test_topic", "broker-a", 0),
        MessageQueue::from_parts("test_topic", "broker-a", 1),
        MessageQueue::from_parts("test_topic", "broker-a", 2),
        MessageQueue::from_parts("test_topic", "broker-a", 3),
    ];
    let msg = Message::builder().build().unwrap();

    c.bench_function("select_original", |b| {
        b.iter(|| {
            let order_id = black_box(12345);
            black_box(selector.select(&queues, &msg, &order_id))
        })
    });
}

fn bench_select_cached(c: &mut Criterion) {
    let selector = SelectMessageQueueByHashCached;
    let queues = vec![
        MessageQueue::from_parts("test_topic", "broker-a", 0),
        MessageQueue::from_parts("test_topic", "broker-a", 1),
        MessageQueue::from_parts("test_topic", "broker-a", 2),
        MessageQueue::from_parts("test_topic", "broker-a", 3),
    ];
    let msg = Message::builder().build().unwrap();

    c.bench_function("select_cached", |b| {
        b.iter(|| {
            let order_id = black_box(12345);
            black_box(selector.select(&queues, &msg, &order_id))
        })
    });
}

criterion_group!(
    benches,
    bench_hasher_creation,
    bench_select_original,
    bench_select_cached
);
criterion_main!(benches);
