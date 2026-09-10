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

#![recursion_limit = "512"]

mod acl;
mod audit;
mod auth;
mod cluster;
mod connection;
mod consumer;
mod dashboard;
mod error;
mod history;
mod message;
mod nameserver;
mod persistence;
mod producer;
mod proxy;
mod topic;

use rocketmq_admin_core::client_adapter::ClientRuntime;
use rocketmq_admin_core::client_adapter::ClientRuntimeConfig;
use rocketmq_admin_core::client_adapter::TelemetryHandle;
use rocketmq_dashboard_common::NameServerConfigStore;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;
use tauri::Manager;

const ADMIN_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
const STARTUP_FAILURE_EXIT_CODE: i32 = 70;
const CLEANUP_FAILURE_EXIT_CODE: i32 = 71;

#[derive(Clone)]
struct DashboardAdminLifecycle {
    audit: audit::AuditManager,
    storage: persistence::StorageManager,
    cluster_manager: cluster::ClusterManager,
    consumer_manager: consumer::ConsumerManager,
    message_manager: message::MessageManager,
    producer_manager: producer::ProducerManager,
    acl_manager: acl::AclManager,
    topic_manager: topic::TopicManager,
}

struct DashboardApplication {
    app: tauri::App<tauri::Wry>,
    client_runtime_owner: RuntimeOwner,
    client_runtime: Arc<ClientRuntime>,
    admin_lifecycle: Arc<OnceLock<DashboardAdminLifecycle>>,
}

impl DashboardAdminLifecycle {
    async fn shutdown(&self) -> bool {
        let audit_healthy = self.audit.shutdown(ADMIN_SHUTDOWN_TIMEOUT).await;
        let storage_healthy = self.storage.shutdown(ADMIN_SHUTDOWN_TIMEOUT).await;
        tokio::join!(
            self.cluster_manager.shutdown(),
            self.consumer_manager.shutdown(),
            self.message_manager.shutdown(),
            self.producer_manager.shutdown(),
            self.acl_manager.shutdown(),
            self.topic_manager.shutdown(),
        );
        audit_healthy && storage_healthy
    }
}

fn final_exit_code(application_exit_code: i32, cleanup_healthy: bool) -> i32 {
    if application_exit_code != 0 {
        application_exit_code
    } else if cleanup_healthy {
        0
    } else {
        CLEANUP_FAILURE_EXIT_CODE
    }
}

struct DashboardServices {
    auth_service: auth::AuthService,
    nameserver_runtime: Arc<nameserver::NameServerRuntimeState>,
    nameserver_manager: nameserver::NameServerManager,
    cluster_manager: cluster::ClusterManager,
    consumer_manager: consumer::ConsumerManager,
    message_manager: message::MessageManager,
    producer_manager: producer::ProducerManager,
    acl_manager: acl::AclManager,
    topic_manager: topic::TopicManager,
}

fn initialize_services(
    database_path: &std::path::Path,
    client_runtime: Arc<ClientRuntime>,
) -> error::DashboardResult<DashboardServices> {
    let auth_db = auth::AuthDb::from_path(database_path);
    auth_db.init()?;
    log::info!("Local auth SQLite database initialized");

    let auth_service = auth::AuthService::new(auth_db);
    let bootstrap_status = auth_service.bootstrap_default_admin()?;

    if bootstrap_status.created {
        log::warn!(
            "Initialized local dashboard admin account `{}` with the bootstrap password. The password must be \
             changed after login.",
            bootstrap_status.username
        );
    }

    let nameserver_db = nameserver::NameServerDb::from_path(database_path);
    nameserver_db.init()?;
    log::info!("Local NameServer SQLite tables initialized");

    let nameserver_store = nameserver::SqliteNameServerStore::new(nameserver_db.clone());
    let nameserver_runtime = Arc::new(nameserver::NameServerRuntimeState::with_admin_config(
        nameserver_store.load_snapshot()?,
        client_runtime,
        connection::AdminConnectionConfig::from_environment()?,
    ));
    let nameserver_manager = nameserver::NameServerManager::new(nameserver_db, nameserver_runtime.clone())?;
    let cluster_manager = cluster::ClusterManager::new(nameserver_runtime.clone());
    let consumer_manager = consumer::ConsumerManager::new(nameserver_runtime.clone());
    let message_manager = message::MessageManager::new(nameserver_runtime.clone());
    let producer_manager = producer::ProducerManager::new(nameserver_runtime.clone());
    let acl_manager = acl::AclManager::new(nameserver_runtime.clone());
    let topic_manager = topic::TopicManager::new(nameserver_runtime.clone());
    let proxy_db = proxy::ProxyDb::from_path(database_path);
    proxy_db.init()?;
    log::info!("Local Proxy SQLite tables initialized");

    Ok(DashboardServices {
        auth_service,
        nameserver_runtime,
        nameserver_manager,
        cluster_manager,
        consumer_manager,
        message_manager,
        producer_manager,
        acl_manager,
        topic_manager,
    })
}

fn build_application() -> Result<DashboardApplication, i32> {
    let runtime_plan = match RuntimeOwner::plan(RuntimeConfig::server_default("rocketmq-dashboard-tauri-client")) {
        Ok(plan) => plan,
        Err(_error) => {
            eprintln!("Dashboard startup failed while planning the client runtime");
            return Err(STARTUP_FAILURE_EXIT_CODE);
        }
    };
    let client_runtime_owner = match runtime_plan.build() {
        Ok(owner) => owner,
        Err(_error) => {
            eprintln!("Dashboard startup failed while creating the client runtime");
            return Err(STARTUP_FAILURE_EXIT_CODE);
        }
    };
    let client_runtime = match ClientRuntime::try_new(
        client_runtime_owner.root_context().component("rocketmq-admin-client"),
        ClientRuntimeConfig::default(),
        TelemetryHandle::noop(),
    ) {
        Ok(runtime) => runtime,
        Err(_error) => {
            eprintln!("Dashboard startup failed while initializing the admin client");
            match client_runtime_owner.shutdown_runtime_blocking() {
                Ok(report) if !report.is_healthy() => {
                    eprintln!("Dashboard client runtime cleanup was incomplete after startup failure");
                }
                Err(_) => eprintln!("Dashboard client runtime cleanup failed after startup failure"),
                Ok(_) => {}
            }
            return Err(STARTUP_FAILURE_EXIT_CODE);
        }
    };
    let setup_client_runtime = client_runtime.clone();
    let storage_context = client_runtime_owner.root_context().component("dashboard-storage");
    let audit_context = client_runtime_owner.root_context().component("dashboard-audit");
    let admin_lifecycle = Arc::new(OnceLock::new());
    let setup_lifecycle = admin_lifecycle.clone();
    let app = tauri::Builder::default()
        .setup(move |app| {
            if cfg!(debug_assertions) {
                app.handle().plugin(
                    tauri_plugin_log::Builder::default()
                        .level(log::LevelFilter::Info)
                        .build(),
                )?;
            }

            let storage =
                persistence::StorageManager::new(persistence::resolve_data_path(app.handle())?, storage_context);
            let database_path = storage.path().to_path_buf();
            // Tauri setup is the synchronous UI composition boundary. All startup
            // SQLite/file work executes on the owned StorageIo blocking lane.
            tauri::async_runtime::block_on(storage.initialize())?;
            let DashboardServices {
                auth_service,
                nameserver_runtime,
                nameserver_manager,
                cluster_manager,
                consumer_manager,
                message_manager,
                producer_manager,
                acl_manager,
                topic_manager,
            } = tauri::async_runtime::block_on(storage.run("storage-bootstrap", move |_connection| {
                initialize_services(&database_path, setup_client_runtime)
            }))?;
            log::info!("Dashboard storage initialized: {:?}", storage.health());

            let connections = tauri::async_runtime::block_on(connection::ConnectionManager::initialize(
                storage.clone(),
                nameserver_runtime.clone(),
            ))?;
            let history = history::HistoryManager::new(
                storage.clone(),
                connections.clone(),
                cluster_manager.clone(),
                topic_manager.clone(),
            )?;
            history.start()?;
            let audit = audit::AuditManager::new(storage.clone(), audit_context);
            audit.start_cleanup()?;
            setup_lifecycle
                .set(DashboardAdminLifecycle {
                    audit: audit.clone(),
                    storage: storage.clone(),
                    cluster_manager: cluster_manager.clone(),
                    consumer_manager: consumer_manager.clone(),
                    message_manager: message_manager.clone(),
                    producer_manager: producer_manager.clone(),
                    acl_manager: acl_manager.clone(),
                    topic_manager: topic_manager.clone(),
                })
                .map_err(|_| crate::error::DashboardError::Internal("admin lifecycle initialized twice"))?;

            let sessions = auth::SessionState::new(storage.clone(), auth_service)?;
            sessions.start_cleanup()?;
            app.manage(history);
            app.manage(storage);
            app.manage(sessions);
            app.manage(audit);
            app.manage(connections);
            app.manage(nameserver_runtime);
            app.manage(nameserver_manager);
            app.manage(cluster_manager);
            app.manage(consumer_manager);
            app.manage(message_manager);
            app.manage(producer_manager);
            app.manage(acl_manager);
            app.manage(topic_manager);

            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            connection::commands::get_connection_settings,
            connection::commands::replace_name_servers,
            audit::commands::query_audit_events,
            auth::commands::login,
            auth::commands::logout,
            auth::commands::restore_session,
            auth::commands::change_password,
            auth::commands::get_current_user_profile,
            auth::commands::get_auth_bootstrap_status,
            auth::commands::list_sessions,
            auth::commands::revoke_user_sessions,
            nameserver::commands::get_name_server_home_page,
            nameserver::commands::add_name_server,
            nameserver::commands::switch_name_server,
            nameserver::commands::delete_name_server,
            nameserver::commands::update_vip_channel,
            nameserver::commands::update_use_tls,
            cluster::commands::get_cluster_home_page,
            cluster::commands::get_cluster_broker_config,
            cluster::commands::update_cluster_broker_config,
            cluster::commands::get_cluster_broker_status,
            consumer::commands::query_consumer_groups,
            consumer::commands::refresh_consumer_group,
            consumer::commands::refresh_all_consumer_groups,
            consumer::commands::query_consumer_connection,
            consumer::commands::query_consumer_topic_detail,
            consumer::commands::query_consumer_config,
            consumer::commands::query_consumer_config_summary,
            consumer::commands::query_consumer_running_info,
            consumer::commands::query_consumer_jstack,
            consumer::commands::create_or_update_consumer_group,
            consumer::commands::delete_consumer_group,
            dashboard::commands::get_dashboard_broker_overview,
            dashboard::commands::get_dashboard_overview,
            history::query_broker_history,
            history::query_topic_history,
            history::get_history_status,
            dashboard::commands::query_dashboard_topic_current,
            message::commands::query_message_by_topic_key,
            message::commands::query_message_by_id,
            message::commands::query_message_page_by_topic,
            message::commands::query_dlq_message_by_consumer_group,
            message::commands::view_message_detail,
            message::commands::view_dlq_message_detail,
            message::commands::resend_dlq_message,
            message::commands::batch_resend_dlq_message,
            message::commands::export_dlq_message,
            message::commands::batch_export_dlq_message,
            message::commands::consume_message_directly,
            message::commands::query_message_trace_by_id,
            message::commands::view_message_trace_detail,
            producer::commands::get_producer_topic_options,
            producer::commands::list_producer_groups,
            acl::commands::list_acl_users,
            acl::commands::create_acl_user,
            acl::commands::update_acl_user,
            acl::commands::delete_acl_user,
            acl::commands::list_acl_policies,
            acl::commands::create_acl_policy,
            acl::commands::update_acl_policy,
            acl::commands::delete_acl_policy,
            producer::commands::query_producer_connections,
            proxy::commands::get_proxy_home_page,
            proxy::commands::add_proxy_addr,
            proxy::commands::switch_proxy_addr,
            proxy::commands::delete_proxy_addr,
            topic::commands::get_topic_list,
            topic::commands::get_topic_route,
            topic::commands::get_topic_stats,
            topic::commands::get_topic_config,
            topic::commands::create_or_update_topic,
            topic::commands::delete_topic,
            topic::commands::delete_topic_by_broker,
            topic::commands::get_topic_consumer_groups,
            topic::commands::get_topic_consumers,
            topic::commands::reset_consumer_offset,
            topic::commands::skip_message_accumulate,
            topic::commands::send_topic_message
        ])
        .build(tauri::generate_context!());

    let app = match app {
        Ok(app) => app,
        Err(_error) => {
            eprintln!("Dashboard startup failed while building the application");
            match client_runtime_owner
                .block_on(async { tokio::time::timeout(ADMIN_SHUTDOWN_TIMEOUT, client_runtime.shutdown()).await })
            {
                Ok(report) if !report.is_healthy() => {
                    eprintln!("Dashboard admin client cleanup was incomplete after startup failure");
                }
                Err(_) => {
                    eprintln!("Dashboard admin client cleanup timed out after startup failure");
                }
                Ok(_) => {}
            }
            match client_runtime_owner.shutdown_runtime_blocking() {
                Ok(report) if !report.is_healthy() => {
                    eprintln!("Dashboard client runtime cleanup was incomplete after startup failure");
                }
                Err(_) => eprintln!("Dashboard client runtime cleanup failed after startup failure"),
                Ok(_) => {}
            }
            return Err(STARTUP_FAILURE_EXIT_CODE);
        }
    };

    Ok(DashboardApplication {
        app,
        client_runtime_owner,
        client_runtime,
        admin_lifecycle,
    })
}

#[cfg(desktop)]
pub fn run() -> i32 {
    let DashboardApplication {
        app,
        client_runtime_owner,
        client_runtime,
        admin_lifecycle,
    } = match build_application() {
        Ok(application) => application,
        Err(exit_code) => return exit_code,
    };

    let exit_code = app.run_return(|_, _| {});
    let mut cleanup_healthy = true;
    if let Some(lifecycle) = admin_lifecycle.get() {
        let shutdown = tauri::async_runtime::block_on(async {
            tokio::time::timeout(ADMIN_SHUTDOWN_TIMEOUT * 3, lifecycle.shutdown()).await
        });
        if !matches!(shutdown, Ok(true)) {
            cleanup_healthy = false;
            log::error!("Dashboard storage or admin session shutdown was incomplete");
        }
    }
    let client_shutdown = client_runtime_owner
        .block_on(async { tokio::time::timeout(ADMIN_SHUTDOWN_TIMEOUT, client_runtime.shutdown()).await });
    match client_shutdown {
        Ok(report) => {
            cleanup_healthy &= report.is_healthy();
            if !report.is_healthy() {
                log::error!("Dashboard client runtime cleanup was incomplete");
            }
        }
        Err(_) => {
            cleanup_healthy = false;
            log::error!("Timed out while shutting down the dashboard client runtime");
        }
    }
    match client_runtime_owner.shutdown_runtime_blocking() {
        Ok(report) if !report.is_healthy() => {
            cleanup_healthy = false;
            log::error!("Dashboard client runtime cleanup was incomplete");
        }
        Err(_) => {
            cleanup_healthy = false;
            log::error!("Failed to shut down dashboard client runtime");
        }
        Ok(_) => {}
    }
    final_exit_code(exit_code, cleanup_healthy)
}

#[cfg(mobile)]
#[tauri::mobile_entry_point]
pub fn run() {
    let DashboardApplication {
        app,
        client_runtime_owner: _client_runtime_owner,
        client_runtime: _client_runtime,
        admin_lifecycle: _admin_lifecycle,
    } = match build_application() {
        Ok(application) => application,
        Err(_) => return,
    };

    app.run(|_, _| {});
}

#[cfg(test)]
mod tests {
    use super::CLEANUP_FAILURE_EXIT_CODE;
    use super::final_exit_code;

    #[test]
    fn exit_code_preserves_application_failure_and_reports_cleanup_failure() {
        assert_eq!(final_exit_code(9, false), 9);
        assert_eq!(final_exit_code(0, false), CLEANUP_FAILURE_EXIT_CODE);
        assert_eq!(final_exit_code(0, true), 0);
    }
}
