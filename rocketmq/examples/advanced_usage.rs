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

use rocketmq_rust::schedule::executor::ExecutorConfig;
use rocketmq_rust::CronTrigger;
use rocketmq_rust::IntervalTrigger;
use rocketmq_rust::SchedulerConfig;
use rocketmq_rust::Task;
use rocketmq_rust::TaskResult;
use rocketmq_rust::TaskScheduler;
use tokio::time::sleep;
use tracing::error;
use tracing::info;
use tracing::Level;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    // Create custom scheduler configuration
    let config = SchedulerConfig {
        executor_config: ExecutorConfig {
            max_concurrent_tasks: 5,
            default_timeout: Duration::from_secs(30),
            enable_metrics: true,
        },
        executor_pool_size: 2,
        check_interval: Duration::from_millis(500),
        max_scheduler_threads: 1,
        enable_persistence: true,
        persistence_interval: Duration::from_secs(10),
    };

    let scheduler = Arc::new(TaskScheduler::new(config));

    // Shared counter for demonstration
    let counter = Arc::new(AtomicU32::new(0));

    // Create different types of tasks

    // 1. Data processing task
    let counter_clone = counter.clone();
    let data_task = Arc::new(
        Task::new("data_processor", "Data Processor", move |ctx| {
            let counter = counter_clone.clone();
            async move {
                let count = counter.fetch_add(1, Ordering::Relaxed);
                info!("Processing data batch #{} at {:?}", count, ctx.scheduled_time);

                // Simulate data processing
                sleep(Duration::from_millis(200)).await;

                // Simulate occasional failures
                if count % 7 == 6 {
                    TaskResult::Failed("Simulated processing error".to_string())
                } else {
                    TaskResult::Success(Some(format!("Processed batch #{}", count)))
                }
            }
        })
        .with_description("Processes data batches periodically")
        .with_group("data")
        .with_priority(2)
        .with_max_retry(3)
        .with_timeout(Duration::from_secs(10)),
    );

    // 2. Health check task
    let health_task = Arc::new(
        Task::new("health_check", "Health Check", |ctx| async move {
            info!("Performing health check: {}", ctx.execution_id);

            // Simulate health check
            sleep(Duration::from_millis(50)).await;

            TaskResult::Success(Some("All systems healthy".to_string()))
        })
        .with_description("Checks system health")
        .with_group("monitoring")
        .with_priority(1),
    );

    // 3. Cleanup task
    let cleanup_task = Arc::new(
        Task::new("cleanup", "Cleanup Task", |ctx| async move {
            info!("Running cleanup: {}", ctx.execution_id);

            // Simulate cleanup work
            sleep(Duration::from_millis(100)).await;

            TaskResult::Success(Some("Cleanup completed".to_string()))
        })
        .with_description("Cleans up temporary files")
        .with_group("maintenance")
        .with_priority(0),
    );

    let counter_c = counter.clone();
    // 4. Report generation task
    let report_task = Arc::new(
        Task::new("report_generator", "Report Generator", move |_| {
            let value = counter_c.clone();
            async move {
                let count = value.load(Ordering::Relaxed);
                info!("Generating report with {} processed items", count);

                // Simulate report generation
                sleep(Duration::from_millis(300)).await;

                TaskResult::Success(Some(format!("Report generated with {} items", count)))
            }
        })
        .with_description("Generates periodic reports")
        .with_group("reporting")
        .with_priority(1),
    );

    // Start the scheduler
    scheduler.start().await?;

    // Schedule jobs with different triggers

    // Data processing every 5 seconds
    let data_job_id = scheduler
        .schedule_job(data_task, Arc::new(IntervalTrigger::every_seconds(5)))
        .await?;

    // Health check every 10 seconds
    let _health_job_id = scheduler
        .schedule_job(health_task, Arc::new(IntervalTrigger::every_seconds(10)))
        .await?;

    // Cleanup task using cron (every minute at second 0)
    let _cleanup_job_id = scheduler
        .schedule_job(cleanup_task, Arc::new(CronTrigger::new("0 * * * * *")?))
        .await?;

    // Report generation every 30 seconds, starting after 10 seconds
    let report_job_id = scheduler
        .schedule_job(
            report_task,
            Arc::new(
                IntervalTrigger::every_seconds(30)
                    .with_start_time(std::time::SystemTime::now() + Duration::from_secs(10)),
            ),
        )
        .await?;

    info!("All jobs scheduled successfully");

    let arc = scheduler.clone();
    // Demonstrate scheduler management
    tokio::spawn(async move {
        sleep(Duration::from_secs(20)).await;

        // Temporarily disable data processing
        if let Err(e) = arc.set_job_enabled(&data_job_id, false).await {
            error!("Failed to disable job: {}", e);
        } else {
            info!("Data processing job disabled");
        }

        sleep(Duration::from_secs(10)).await;

        // Re-enable data processing
        if let Err(e) = arc.set_job_enabled(&data_job_id, true).await {
            error!("Failed to enable job: {}", e);
        } else {
            info!("Data processing job re-enabled");
        }

        sleep(Duration::from_secs(10)).await;

        // Execute report generation immediately
        match arc.execute_job_now(&report_job_id).await {
            Ok(execution_id) => info!("Report job executed immediately: {}", execution_id),
            Err(e) => error!("Failed to execute report job: {}", e),
        }
    });

    let clone = scheduler.clone();
    // Monitor scheduler status
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(15)).await;

            let status = clone.get_status().await;
            info!(
                "Scheduler Status - Running: {}, Total Jobs: {}, Enabled: {}, Running Tasks: {}",
                status.running, status.total_jobs, status.enabled_jobs, status.running_tasks
            );

            // Get jobs by group
            let monitoring_jobs = clone.get_jobs_by_group("monitoring").await;
            info!("Monitoring jobs count: {}", monitoring_jobs.len());
        }
    });

    // Let the scheduler run
    sleep(Duration::from_secs(60)).await;

    // Stop the scheduler
    info!("Stopping scheduler...");
    scheduler.stop().await?;

    info!("Final counter value: {}", counter.load(Ordering::Relaxed));

    Ok(())
}
