// Copyright 2026 The RocketMQ Rust Authors
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

use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;

use super::*;
use crate::admin_facade::test_client_runtime;

fn run_local_test(test: impl Future<Output = ()>) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    tokio::task::LocalSet::new().block_on(&runtime, async {
        tokio::time::timeout(Duration::from_secs(20), test)
            .await
            .expect("command and cleanup must finish within the test deadline");
    });
}

async fn start_blocked_query(app: &mut RocketmqTuiApp, execution_id: u64) -> (Arc<ClientRuntime>, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let namesrv_addr = listener.local_addr().unwrap().to_string();
    app.apply_action(Action::NamesrvChanged(namesrv_addr.clone()));
    let facade = app.command_facade(execution_id).unwrap();
    assert_eq!(facade.namesrv_addr(), Some(namesrv_addr.as_str()));
    let client_runtime = facade.client_runtime();
    app.apply_action(Action::CommandStarted {
        execution_id,
        command_id: "topic.list".to_string(),
    });
    app.spawn_command_task(
        execution_id,
        "topic.list".to_string(),
        client_runtime.clone(),
        async move {
            let topics = facade.query_topic_list(None).await?;
            Ok(CommandResultViewModel::from_debug("Topics", &topics))
        },
    );

    // The request has reached a real connection. Keeping the peer silent leaves
    // the command suspended inside its RPC rather than before session startup.
    let (mut peer, _) = listener.accept().await.unwrap();
    peer.read_u8().await.unwrap();
    assert_eq!(client_runtime.pool().instance_count(), 1);
    (client_runtime, peer)
}

async fn assert_runtime_closed(client_runtime: &ClientRuntime) {
    assert!(client_runtime.is_shutdown());
    assert_eq!(client_runtime.pool().instance_count(), 0);
    assert_eq!(client_runtime.service_context().task_group().task_count(), 0);
    let report = client_runtime.shutdown().await;
    assert!(report.is_healthy(), "{}", report.to_json());
}

#[test]
fn cancelling_query_releases_its_pool_without_stopping_the_next_command() {
    run_local_test(async {
        let parent = test_client_runtime();
        let mut app = RocketmqTuiApp::new(parent.clone());
        let (cancelled_runtime, _cancelled_peer) = start_blocked_query(&mut app, 1).await;
        app.apply_action(Action::CancelExecution {
            execution_id: 1,
            command_id: "topic.list".to_string(),
        });

        // A new command may start while the previous command's cleanup is still
        // queued. It must keep an independent pool and cancellation scope.
        let (next_runtime, _next_peer) = start_blocked_query(&mut app, 2).await;
        let completion = app.command_tasks.join_next().await.unwrap();
        app.complete_command_task(completion);
        assert_runtime_closed(&cancelled_runtime).await;
        assert!(!next_runtime.is_shutdown());
        assert_eq!(next_runtime.pool().instance_count(), 1);
        assert!(matches!(
            app.state.execution,
            CommandExecutionState::Running { execution_id: 2, .. }
        ));

        app.apply_action(Action::CancelExecution {
            execution_id: 2,
            command_id: "topic.list".to_string(),
        });
        app.shutdown_commands().await;
        assert_runtime_closed(&next_runtime).await;
        assert!(app.command_tasks.is_empty());
        assert!(!parent.is_shutdown());
        assert_eq!(parent.pool().instance_count(), 0);
        assert!(parent.shutdown().await.is_healthy());
    });
}

#[test]
fn quitting_awaits_cancelled_command_cleanup_even_with_a_full_action_queue() {
    run_local_test(async {
        let parent = test_client_runtime();
        let mut app = RocketmqTuiApp::new(parent.clone());
        let (client_runtime, _peer) = start_blocked_query(&mut app, 1).await;
        for _ in 0..ACTION_QUEUE_CAPACITY {
            try_send_progress(&app.action_tx, &app.action_queue_diagnostics, 1, "waiting".to_string());
        }
        assert_eq!(app.action_queue_snapshot().queued, ACTION_QUEUE_CAPACITY);

        app.quit();
        app.shutdown_commands().await;
        assert!(app.should_quit());
        assert!(app.running_task.is_none());
        assert!(app.command_tasks.is_empty());
        assert_runtime_closed(&client_runtime).await;
        assert!(parent.shutdown().await.is_healthy());
    });
}

#[test]
fn a_closed_parent_rejects_execution_without_spawning_tasks() {
    run_local_test(async {
        let parent = test_client_runtime();
        let mut app = RocketmqTuiApp::new(parent.clone());
        assert!(parent.shutdown().await.is_healthy());

        let error = app.command_facade(1).unwrap_err();
        assert_eq!(error.descriptor(), &rocketmq_error::RUNTIME_CONTEXT_UNAVAILABLE);
        app.start_execution(1, "topic.list".to_string());
        assert!(matches!(app.state.execution, CommandExecutionState::Failed { .. }));
        assert_eq!(app.state.last_error.as_deref(), Some(error.to_string().as_str()));
        assert!(app.running_task.is_none());
        assert!(app.command_tasks.is_empty());
    });
}

#[test]
fn command_completion_error_and_panic_all_release_owned_clients() {
    run_local_test(async {
        let parent = test_client_runtime();
        let mut app = RocketmqTuiApp::new(parent.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        app.apply_action(Action::NamesrvChanged(listener.local_addr().unwrap().to_string()));

        for execution_id in 1..=3 {
            let facade = app.command_facade(execution_id).unwrap();
            let client_runtime = facade.client_runtime();
            let observed_runtime = client_runtime.clone();
            app.apply_action(Action::CommandStarted {
                execution_id,
                command_id: "test".to_string(),
            });
            app.spawn_command_task(execution_id, "test".to_string(), client_runtime.clone(), async move {
                let _session = facade.admin_builder().build_and_start().await.unwrap();
                assert_eq!(observed_runtime.pool().instance_count(), 1);
                match execution_id {
                    1 => Ok(CommandResultViewModel::operation_success("Done", Vec::new())),
                    2 => Err(crate::errors::argument_invalid("test operation failed")),
                    _ => panic!("test command panic"),
                }
            });

            let completion = app.command_tasks.join_next().await.unwrap();
            app.complete_command_task(completion);
            assert_runtime_closed(&client_runtime).await;
            assert!(app.running_task.is_none());
            if execution_id == 1 {
                assert!(matches!(app.state.execution, CommandExecutionState::Succeeded { .. }));
            } else {
                assert!(matches!(app.state.execution, CommandExecutionState::Failed { .. }));
            }
        }
        assert!(parent.shutdown().await.is_healthy());
    });
}
