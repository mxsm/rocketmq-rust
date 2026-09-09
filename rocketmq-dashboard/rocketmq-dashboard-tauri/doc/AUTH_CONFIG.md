# Authentication Configuration

## Overview

The RocketMQ Dashboard Tauri application now uses a local embedded SQLite database for authentication.
The backend stores users in `dashboard.db`, hashes passwords with Argon2, and keeps login sessions in memory.

## Default Administrator Bootstrap

On first startup, the application creates a default local administrator account:

- Username: `admin`
- Initial password source:
  - `ROCKETMQ_DASHBOARD_INIT_PASSWORD` environment variable, if set
  - otherwise `admin123`

The password is stored only as an Argon2 hash. The bootstrap password is never written back to disk in plain text.

After the first successful login, the administrator must change the password before the dashboard becomes available.

## Database Location

Authentication and saved NameServer/Proxy configuration share `dashboard.db` in the application config directory for `com.rocketmqrust.dashboard`. On Linux, `XDG_CONFIG_HOME` overrides `~/.config`.

### Windows

```text
C:\Users\<YourUsername>\AppData\Roaming\com.rocketmqrust.dashboard\dashboard.db
```

### macOS

```text
~/Library/Application Support/com.rocketmqrust.dashboard/dashboard.db
```

### Linux

```text
~/.config/com.rocketmqrust.dashboard/dashboard.db
```

## Schema

The embedded database currently uses a single `users` table:

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

- Sessions are stored only in process memory.
- Session restore works while the Tauri backend process is still alive.
- Restarting the desktop application clears all active sessions.

This is intentional for the first version to keep the design simple and reduce local attack surface.

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
- Multi-user support, RBAC, lockout policy, and persistent session storage are not part of this first version.

## Tauri Commands

The backend currently exposes these authentication commands:

- `login`
- `logout`
- `restore_session`
- `change_password`
- `get_auth_bootstrap_status`

## Verification

Backend tests:

```bash
cd src-tauri
cargo test
```

Frontend build:

```bash
npm run build
```
