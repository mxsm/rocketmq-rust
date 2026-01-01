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

use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_rust::DelayedIntervalTrigger;
use rocketmq_rust::IntervalTrigger;
use rocketmq_rust::SchedulerConfig;
use rocketmq_rust::Task;
use rocketmq_rust::TaskResult;
use rocketmq_rust::TaskScheduler;
use tokio::time::sleep;
use tracing::info;
use tracing::Level;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    // Create scheduler with custom config
    let config = SchedulerConfig {
        executor_config: rocketmq_rust::ExecutorConfig {
            max_concurrent_tasks: 10,
            default_timeout: Duration::from_secs(30),
            enable_metrics: true,
        },
        executor_pool_size: 2,
        check_interval: Duration::from_millis(100), // Faster checking for demo
        max_scheduler_threads: 1,
        enable_persistence: false,
        persistence_interval: Duration::from_secs(10),
    };

    let scheduler = Arc::new(TaskScheduler::new(config));

    // Shared counter for demonstration
    let counter = Arc::new(AtomicU32::new(0));

    // 1. Simple delayed task (execute once after 3 seconds)
    let counter_clone = counter.clone();
    let delayed_task = Arc::new(
        Task::new("delayed_task", "Simple Delayed Task", move |ctx| {
            let counter = counter_clone.clone();
            async move {
                let count = counter.fetch_add(1, Ordering::Relaxed);
                info!(
                    "Execute delayed tasks #{}: {} at {:?}",
                    count + 1,
                    ctx.execution_id,
                    ctx.scheduled_time
                );
                TaskResult::Success(Some("Delay task completion".to_string()))
            }
        })
        .with_description("A delayed task that executes once after 3 seconds"),
    );

    // 2. Interval task with initial delay (Start after 5 seconds, and then execute once every 2
    //    seconds.)
    let counter_clone = counter.clone();
    let interval_delayed_task = Arc::new(
        Task::new("interval_delayed", "Interval with Initial Delay", move |ctx| {
            let counter = counter_clone.clone();
            async move {
                let count = counter.fetch_add(1, Ordering::Relaxed);
                info!(
                    "Execute interval delayed tasks #{}: {} at {:?}",
                    count + 1,
                    ctx.execution_id,
                    ctx.scheduled_time
                );
                TaskResult::Success(Some("Interval delay task completed".to_string()))
            }
        })
        .with_description("Interval task with initial delay"),
    );

    // 3. Regular task with execution delay (每次执行前等待500ms)
    let counter_clone = counter.clone();
    let execution_delay_task = Arc::new(
        Task::new("execution_delay", "Task with Execution Delay", move |ctx| {
            let counter = counter_clone.clone();
            async move {
                let count = counter.fetch_add(1, Ordering::Relaxed);
                info!(
                    "Execute tasks with execution delay #{}: {} at {:?}",
                    count + 1,
                    ctx.execution_id,
                    ctx.scheduled_time
                );
                TaskResult::Success(Some("Execution of the delayed task is completed".to_string()))
            }
        })
        .with_description("Delay 500ms before each execution")
        .with_execution_delay(Duration::from_millis(500)),
    );

    // 4. Limited interval task with delay (最多执行3次)
    let counter_clone = counter.clone();
    let limited_task = Arc::new(
        Task::new("limited_interval", "Limited Interval Task", move |ctx| {
            let counter = counter_clone.clone();
            async move {
                let count = counter.fetch_add(1, Ordering::Relaxed);
                info!(
                    "Execute tasks with limited number of times #{}: {} at {:?}",
                    count + 1,
                    ctx.execution_id,
                    ctx.scheduled_time
                );
                TaskResult::Success(Some("Limit the number of times to complete the task".to_string()))
            }
        })
        .with_description("Interval task that executes at most 3 times"),
    );

    // Start the scheduler
    scheduler.start().await?;

    // Schedule different types of delayed tasks

    // 1. Simple delay task
    let delayed_job_id = scheduler
        .schedule_delayed_job(delayed_task, Duration::from_secs(3))
        .await?;
    info!("Scheduled delayed job: {}", delayed_job_id);

    // 2. Interval with initial delay
    let interval_job_id = scheduler
        .schedule_interval_job_with_delay(interval_delayed_task, Duration::from_secs(2), Duration::from_secs(5))
        .await?;
    info!("Scheduled interval job with delay: {}", interval_job_id);

    // 3. Regular interval with execution delay
    let execution_delay_job_id = scheduler
        .schedule_job(execution_delay_task, Arc::new(IntervalTrigger::every_seconds(3)))
        .await?;
    info!("Scheduled execution delay job: {}", execution_delay_job_id);

    // 4. Limited interval with delay
    let limited_job_id = scheduler
        .schedule_job(
            limited_task,
            Arc::new(DelayedIntervalTrigger::every_seconds_with_delay(1, 2).repeat(3)),
        )
        .await?;
    info!("Scheduled limited job: {}", limited_job_id);

    // Demonstrate immediate execution with delay
    tokio::spawn({
        let scheduler = scheduler.clone();
        let execution_delay_job_id = execution_delay_job_id.clone();
        async move {
            sleep(Duration::from_secs(8)).await;

            // Execute immediately with 2-second delay
            match scheduler
                .execute_job_now_with_delay(&execution_delay_job_id, Some(Duration::from_secs(2)))
                .await
            {
                Ok(execution_id) => info!("Immediate execution scheduled: {}", execution_id),
                Err(e) => info!("Failed to schedule immediate execution: {}", e),
            }
        }
    });

    // Monitor scheduler status
    tokio::spawn({
        let scheduler = scheduler.clone();
        async move {
            loop {
                sleep(Duration::from_secs(5)).await;
                let status = scheduler.get_status().await;
                info!(
                    "Scheduler Status - Total Jobs: {}, Enabled: {}, Running Tasks: {}",
                    status.total_jobs, status.enabled_jobs, status.running_tasks
                );
            }
        }
    });

    // Run for 30 seconds to see all tasks execute
    sleep(Duration::from_secs(30)).await;

    // Get final status
    let status = scheduler.get_status().await;
    info!("Final scheduler status: {:?}", status);
    info!("Final counter value: {}", counter.load(Ordering::Acquire));

    // Stop the scheduler
    scheduler.stop().await?;

    Ok(())
}
