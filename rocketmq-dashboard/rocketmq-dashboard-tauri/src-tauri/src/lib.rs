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

mod auth;
mod nameserver;

use tauri::Manager;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .setup(|app| {
            if cfg!(debug_assertions) {
                app.handle().plugin(
                    tauri_plugin_log::Builder::default()
                        .level(log::LevelFilter::Info)
                        .build(),
                )?;
            }

            let auth_db = auth::AuthDb::new(app.handle())?;
            auth_db.init()?;
            log::info!(
                "Local auth SQLite database initialized at: {}",
                auth_db.db_path().display()
            );

            let auth_service = auth::AuthService::new(auth_db);
            let bootstrap_status = auth_service.bootstrap_default_admin()?;

            let nameserver_db = nameserver::NameServerDb::new(app.handle())?;
            nameserver_db.init()?;
            log::info!(
                "Local NameServer SQLite database initialized at: {}",
                nameserver_db.db_path().display()
            );

            let nameserver_service = nameserver::NameServerService::new(nameserver_db);
            nameserver_service.bootstrap_defaults()?;

            if bootstrap_status.created {
                log::warn!(
                    "Initialized local dashboard admin account `{}` with the bootstrap password. The password must be \
                     changed after login.",
                    bootstrap_status.username
                );
            }

            app.manage(auth_service);
            app.manage(nameserver_service);
            app.manage(auth::SessionState::default());

            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            auth::commands::login,
            auth::commands::logout,
            auth::commands::restore_session,
            auth::commands::change_password,
            auth::commands::get_auth_bootstrap_status,
            nameserver::commands::get_name_server_home_page,
            nameserver::commands::add_name_server,
            nameserver::commands::switch_name_server,
            nameserver::commands::delete_name_server,
            nameserver::commands::update_vip_channel,
            nameserver::commands::update_use_tls
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
