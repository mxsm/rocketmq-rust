# rocketmq-mcp-auth

JWKS retrieval, caching, and RS256 key selection shared by the RocketMQ MCP servers.

`rocketmq-mcp-control` uses it for every request. `rocketmq-mcp` uses it for the `oauth-jwt` mode of its
`streamable-http` transport. Both servers accept OAuth Bearer tokens signed with RS256 and both need the same
answer first: which public key of the issuer must verify this token.

## What it does

| Item | Role |
| --- | --- |
| `bearer_token` | Takes the token out of an `Authorization: Bearer` header and refuses one above a length limit. |
| `JwksVerifier` | Reads the token header, requires RS256 and a `kid`, and returns the matching key from a cached JWKS document. |
| `HttpJwksSource` | Fetches the JWKS document over HTTPS without following redirects, up to a size limit. |
| `parse_jwks` | Reads the usable RS256 keys of a JWKS document. |
| `JwksPolicy` | The limits and timings a server chooses. The crate has no default. |
| `OutboundAddressPolicy` | Whether the JWKS endpoint must resolve to public addresses (`PublicOnly`, the default) or may be private. |

Signature verification, claims, principals, HTTP responses, and telemetry stay in each server.

## JWKS documents

An entry of `keys` is used when all of these hold:

- `kty` is `RSA`;
- `use` is absent or `sig`;
- `key_ops` is absent or lists `verify`;
- `alg` is absent or `RS256`;
- `kid` is 1 to 128 bytes of the character set the policy allows;
- the modulus and exponent fit the policy (both servers require 2048 to 8192 bits and exponent 65537).

Every other entry is skipped, and members the crate does not read, such as `x5c` and `x5t`, are ignored. A
document is rejected when it is larger than the policy allows, lists no entry or too many, repeats a `kid` among
its usable entries, or has no usable entry.

A key without `alg` is still used for RS256 only. The token header must name RS256, and each server verifies
the signature with RS256.

## Caching

- Keys are used for `cache_ttl` after a fetch. The next lookup after that fetches again, and concurrent lookups
  share one fetch.
- A token whose `kid` is not in the current key set triggers one fetch. If the key is still missing, or the
  fetch fails, further fetches wait for `unknown_kid_cooldown`, so unknown key ids cannot be used to make the
  server call the issuer at will.
- While fetching fails, keys already accepted may stand in until they are `max_stale` old. With `max_stale`
  at zero, which the control server uses, a key is never used past `cache_ttl`.
- A lookup reports either that the token was rejected or that the key set is unavailable, so a server can
  answer an issuer outage differently from a bad token.

## Development

```bash
cargo fmt --all -- --check
```

```bash
cargo test --locked
```

The tests need no network. The fetch tests answer on a loopback port chosen by the operating system.
