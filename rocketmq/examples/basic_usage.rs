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

use std::sync::Arc;
use std::time::Duration;

use rocketmq_rust::CronTrigger;
use rocketmq_rust::DelayTrigger;
use rocketmq_rust::IntervalTrigger;
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

    // Create scheduler
    let scheduler = TaskScheduler::default();

    // Create a simple task
    let simple_task = Arc::new(
        Task::new("simple_task", "Simple Task", |ctx| async move {
            info!("Executing simple task: {}", ctx.task_id);
            sleep(Duration::from_millis(100)).await;
            TaskResult::Success(Some("Task completed successfully".to_string()))
        })
        .with_description("A simple demonstration task")
        .with_priority(1),
    );

    // Create a cron trigger (every minute)
    let cron_trigger = Arc::new(CronTrigger::every_minute()?);

    // Create an interval trigger (every 5 seconds)
    let interval_trigger = Arc::new(IntervalTrigger::every_seconds(5));

    // Create a delay trigger (execute once after 3 seconds)
    let delay_trigger = Arc::new(DelayTrigger::after_seconds(3));

    // Start the scheduler
    scheduler.start().await?;

    // Schedule jobs
    let job1_id = scheduler.schedule_job(simple_task.clone(), cron_trigger).await?;
    let job2_id = scheduler.schedule_job(simple_task.clone(), interval_trigger).await?;
    let job3_id = scheduler.schedule_job(simple_task.clone(), delay_trigger).await?;

    info!("Scheduled jobs: {}, {}, {}", job1_id, job2_id, job3_id);

    // Let it run for a while
    sleep(Duration::from_secs(30)).await;

    // Get scheduler status
    let status = scheduler.get_status().await;
    info!("Scheduler status: {:?}", status);

    // Stop the scheduler
    scheduler.stop().await?;

    Ok(())
}
