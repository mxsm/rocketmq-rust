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

            if bootstrap_status.created {
                log::warn!(
                    "Initialized local dashboard admin account `{}` with the bootstrap password. The password must be \
                     changed after login.",
                    bootstrap_status.username
                );
            }

            app.manage(auth_service);
            app.manage(auth::SessionState::default());

            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            auth::commands::login,
            auth::commands::logout,
            auth::commands::restore_session,
            auth::commands::change_password,
            auth::commands::get_auth_bootstrap_status
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
