# Authentication Configuration

## Overview

The RocketMQ Dashboard Tauri application now uses a local embedded SQLite database for authentication.
The backend stores users in `dashboard.db`, hashes passwords with Argon2, and persists SHA-256 login token digests in the same database.

## Default Administrator Bootstrap

On first startup, the application creates a default local administrator account:

- Username: `admin`
- Initial password source:
  - `ROCKETMQ_DASHBOARD_INIT_PASSWORD` environment variable, if set
  - otherwise `admin123`

The password is stored only as an Argon2 hash. The bootstrap password is never written back to disk in plain text.

After the first successful login, the administrator must change the password before the dashboard becomes available.

## Database Location

Authentication and saved NameServer/Proxy configuration share `dashboard.db` in the `data` subdirectory of the application config directory for `com.rocketmqrust.dashboard`. On Linux, `XDG_CONFIG_HOME` overrides `~/.config`.

### Windows

```text
C:\Users\<YourUsername>\AppData\Roaming\com.rocketmqrust.dashboard\data\dashboard.db
```

### macOS

```text
~/Library/Application Support/com.rocketmqrust.dashboard/data/dashboard.db
```

### Linux

```text
~/.config/com.rocketmqrust.dashboard/data/dashboard.db
```

Set `DASHBOARD_TAURI_DATA_DIR` to an isolated directory to override the default;
its database is `<configured-directory>/dashboard.db`. An empty override is rejected.
This storage design starts with a fresh database. It does not import accounts or
addresses from the previous unversioned database, which remains untouched at its
old location. The new database bootstraps the administrator as described above.
Unversioned or unsupported databases are rejected without deleting or rebuilding
them; choose a new data directory to start fresh.

## Schema

Schema version 3 contains `dashboard_schema`, `users`, `sessions`, `audit_events`, and connection configuration tables. Version 1/2 development databases are not migrated; select a new data directory. The account table is:

```sql
CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  username TEXT NOT NULL UNIQUE,
  password_hash TEXT NOT NULL,
  is_active INTEGER NOT NULL DEFAULT 1,
  must_change_password INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  last_login_at TEXT
);
```

## Session Behavior

- Sessions survive application restarts while their account is active and the session is neither expired nor revoked.
- The default absolute lifetime is **8 hours**. Set `DASHBOARD_TAURI_SESSION_TTL_SECS` before launch to override it (1 to 31,536,000 seconds). Invalid values prevent startup.
- Authorization reads the database and updates last-seen time without extending expiry. First-login sessions can change their password but cannot run business commands or manage sessions.
- Login returns a random bearer token. SQLite stores only its SHA-256 digest and a separate random safe session ID; lists and logs never expose the bearer or digest. The frontend retains the bearer in localStorage to restore the session after restart.
- **Changing a password revokes every existing session, including the current one. Sign in again with the new password.** Password update and revocation commit together. Concurrent logins must still match the current password hash when creating their session.
- Account ¡ú Sessions shows creation, expiry, last visit, revocation, and current-session status with cursor pagination. Management is limited to the signed-in account; the username parameter cannot grant cross-account access. There is no local role system.
- Signing out all sessions clears the current frontend token and returns to login. Ordinary business commands do the same when the backend reports an invalid session; late errors from an old token do not clear a newer login.
- An application-owned cleanup runs at startup and hourly, deleting at most 500 records expired or revoked at least seven days ago per pass. Authorization rejects invalid records immediately, independent of cleanup progress. Shutdown cancels the timer and waits for accepted storage work.

## Resetting Local Authentication

Deleting this shared database resets the administrator and saved NameServer/Proxy configuration. To reset all of this local state:

1. Stop the application.
2. Back up `dashboard.db`, then remove it.
3. Restart the application.
4. Sign in with `admin` and the bootstrap password source described above.

## Security Notes

- Password hashing uses Argon2.
- SQLite access uses parameterized queries through `rusqlite`.
- The default admin password should be changed immediately.
- Multi-user provisioning, RBAC, and lockout policy are not implemented.

## Tauri Commands

The backend currently exposes these authentication commands:

- `login`
- `logout`
- `restore_session`
- `change_password`
- `get_auth_bootstrap_status`
- `get_current_user_profile`
- `list_sessions`
- `revoke_user_sessions`

## Verification

Backend tests:

```bash
cd src-tauri
cargo test --lib auth::
```

Frontend build:

```bash
npm run build
npm test -- src/services/auth-session.test.ts src/services/invoke.test.ts
```
