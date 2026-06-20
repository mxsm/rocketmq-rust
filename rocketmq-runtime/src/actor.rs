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

use std::future::Future;
use std::io;
use std::thread;

pub struct ActorRuntime;

impl ActorRuntime {
    pub fn spawn_current_thread<F>(thread_name: impl Into<String>, future: F) -> io::Result<thread::JoinHandle<()>>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let thread_name = thread_name.into();
        thread::Builder::new().name(thread_name.clone()).spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap_or_else(|error| panic!("failed to build actor runtime '{thread_name}': {error}"));
            runtime.block_on(future);
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::time::Duration;

    use super::ActorRuntime;

    #[test]
    fn actor_runtime_runs_future_on_named_current_thread_runtime() {
        let (tx, rx) = mpsc::channel();
        let handle = ActorRuntime::spawn_current_thread("rocketmq-actor-runtime-test", async move {
            tokio::time::sleep(Duration::from_millis(1)).await;
            tx.send(std::thread::current().name().unwrap_or_default().to_string())
                .expect("test receiver should be alive");
        })
        .expect("actor runtime thread should spawn");

        assert_eq!(
            rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            "rocketmq-actor-runtime-test"
        );
        handle.join().expect("actor runtime thread should finish");
    }
}
