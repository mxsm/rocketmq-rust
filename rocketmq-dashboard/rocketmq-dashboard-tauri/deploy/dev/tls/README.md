# Local TLS verification

This isolated Compose project uses the same locally built Rust images as the
[main fixture](../README.md). Docker Desktop shows `rocketmq-tauri-tls-debug`.
Its NameServer accepts TLS and plaintext so the Broker can register internally;
the Broker client listener enforces TLS. All published ports bind to loopback.

Provide these PEM files under `certs/` (ignored by Git):

- `server.pem`: a server certificate valid for `localhost` and `127.0.0.1`.
- `server.key`: its matching unencrypted private key, used only for this fixture.
- `ca.pem`: the issuing CA trusted by the desktop process.

For a fresh development directory, create a two-day certificate with OpenSSL:

```powershell
New-Item -ItemType Directory -Path certs -ErrorAction Stop | Out-Null
openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 2 -subj '/CN=RocketMQ Rust local CA' -keyout certs/ca.key -out certs/ca.pem -addext 'basicConstraints=critical,CA:TRUE' -addext 'keyUsage=critical,keyCertSign,cRLSign'
if ($LASTEXITCODE -ne 0) { throw 'CA generation failed' }
openssl req -new -newkey rsa:2048 -nodes -subj '/CN=localhost' -keyout certs/server.key -out certs/server.csr
if ($LASTEXITCODE -ne 0) { throw 'Server request generation failed' }
@'
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=DNS:localhost,IP:127.0.0.1
'@ | Set-Content -LiteralPath certs/server.ext -Encoding ascii
openssl x509 -req -in certs/server.csr -CA certs/ca.pem -CAkey certs/ca.key -CAcreateserial -out certs/server.pem -days 2 -sha256 -extfile certs/server.ext
if ($LASTEXITCODE -ne 0) { throw 'Server signing failed' }
openssl verify -CAfile certs/ca.pem certs/server.pem
if ($LASTEXITCODE -ne 0) { throw 'Certificate verification failed' }
docker compose up -d --wait --wait-timeout 120
```

OpenSSL must be on PATH. On Windows, Git for Windows also includes an executable
under its installation's `usr/bin` directory. Keep the generated CA and key local;
use newly issued certificates when the development certificates expire.

Before starting the desktop from the same PowerShell session, trust this CA for
that process:

```powershell
$env:SSL_CERT_FILE = (Resolve-Path certs/ca.pem).Path
```

Select NameServer `127.0.0.1:39786`, disable VIP, and enable TLS. The cluster page
should show `TauriTlsDebugCluster` and the Broker at `127.0.0.1:33911`; its config
and status queries must succeed. Certificate verification stays enabled. The
ordinary development cluster still requires TLS disabled when switching back.

Readiness ports are `38088` and `38090`. To independently verify the encrypted
Broker listener:

```powershell
'' | openssl s_client -connect 127.0.0.1:33911 -CAfile certs/ca.pem -verify_return_error -brief
```

Stop with `docker compose down`; named data volumes are retained. This fixture
verifies server TLS, not mutual TLS or ACL. Use the [ACL fixture](../acl/README.md)
for credential and policy tests.
