# Authentication Configuration

## Overview

The RocketMQ Dashboard Tauri application uses a file-based authentication system. Credentials are stored in a configuration file and verified by the Rust backend.

## Default Credentials

When you first run the application, it will create a default configuration with:

- **Username**: `admin`
- **Password**: `admin123`

⚠️ **Security Warning**: Please change the default password immediately after first login!

## Configuration File Location

The authentication configuration is stored in:

### Windows
```
C:\Users\<YourUsername>\AppData\Roaming\com.rocketmq-rust.dashboard\auth_config.json
```

### macOS
```
~/Library/Application Support/com.rocketmq-rust.dashboard/auth_config.json
```

### Linux
```
~/.config/rocketmq-rust-dashboard/auth_config.json
```

## Configuration File Format

```json
{
  "username": "admin",
  "password_hash": "hashed_password_value"
}
```

**Note**: The password is stored as a hash, not plain text.

## Changing Credentials

### Method 1: Manual Configuration File Edit

1. Stop the application
2. Delete the `auth_config.json` file
3. Restart the application
4. A new configuration with default credentials will be created
5. Login with default credentials and change password through UI (future feature)

### Method 2: Direct File Edit (Advanced)

You can manually edit the configuration file, but you'll need to generate the password hash using the application's hashing algorithm.

For testing purposes, you can use the default hash values:

| Password | Hash |
|----------|------|
| `admin123` | (generated on first run) |

## Security Considerations

### Current Implementation

- ✅ Password hashing (not plain text storage)
- ✅ File-based configuration
- ✅ Secure Rust backend verification
- ✅ Frontend validation through Tauri commands

### Future Enhancements

- [ ] Use stronger hashing (bcrypt/argon2) instead of default hasher
- [ ] Add password change functionality in UI
- [ ] Support multiple users
- [ ] Add role-based access control (RBAC)
- [ ] Session management with timeouts
- [ ] Password complexity requirements
- [ ] Account lockout after failed attempts
- [ ] Audit logging

## Troubleshooting

### Cannot Login

1. **Check credentials**: Ensure you're using correct username/password
2. **Reset configuration**: Delete `auth_config.json` and restart
3. **Check logs**: Look for authentication errors in the application logs

### Configuration File Not Found

The configuration file is created automatically on first run. If it's missing:

1. Restart the application
2. Check application logs for errors
3. Verify you have write permissions to the config directory

### Password Lost/Forgotten

Since passwords are hashed, they cannot be retrieved. To reset:

1. Stop the application
2. Delete `auth_config.json`
3. Restart the application (default credentials will be created)
4. Login with `admin` / `admin123`

## Development Notes

### Running in Development Mode

When running in development mode, the application will:
- Create configuration in the development config directory
- Log authentication attempts at INFO level
- Show detailed error messages

### Testing

The `auth.rs` module includes unit tests for:
- Password hashing consistency
- Credential verification
- Password update functionality

Run tests with:
```bash
cd src-tauri
cargo test
```

## API Reference

### Tauri Command: `verify_login`

**Purpose**: Verify user credentials

**Parameters**:
- `username: String` - The username to verify
- `password: String` - The password to verify

**Returns**: `LoginResponse`
```typescript
interface LoginResponse {
  success: boolean;
  message: string;
}
```

**Example Usage** (TypeScript):
```typescript
import {invoke} from '@tauri-apps/api/core';

const result = await invoke<{success: boolean; message: string}>('verify_login', {
    username: 'admin',
    password: 'admin123'
});

if (result.success) {
    // Login successful
    console.log('Logged in successfully');
} else {
    // Login failed
    console.error('Login failed:', result.message);
}
```

## Upgrading Security (Production)

For production use, update `src-tauri/Cargo.toml`:

```toml
[dependencies]
# Add secure password hashing
bcrypt = "0.15"
# OR
argon2 = "0.5"
```

Then update `src-tauri/src/auth.rs` to use the stronger hashing algorithm.

Example with bcrypt:
```rust
use bcrypt::{hash, verify, DEFAULT_COST};

fn hash_password(password: &str) -> String {
    hash(password, DEFAULT_COST).expect("Failed to hash password")
}

fn verify_password(password: &str, hash: &str) -> bool {
    verify(password, hash).unwrap_or(false)
}
```

---

**Last Updated**: 2026-02-04
**Version**: 0.1.0
