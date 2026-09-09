# Production authentication defaults and strict TLS configuration

Production Helm deployments enable authentication and authorization on every
enabled service. Development opt-out is explicit in `values-dev-single.yaml`.
Named Kubernetes Secrets supply ACL files and Broker/Proxy inner-client JSON
credentials; those credentials do not enter ConfigMaps or Helm values. The chart
enables Proxy's outbound ACL signing together with authentication.

An explicitly configured signer or mounted credential file is required to be
usable. Invalid configuration fails startup instead of silently continuing with
unsigned requests. Inline credentials preserve their existing precedence. The
legacy optional credential parser remains available to compatibility callers.

Broker, Controller, and Proxy reject `secure-enforced` startup when either request
authentication or authorization is disabled, before loading bootstrap materials
or binding listeners. NameServer already enforces this requirement. The existing
development and Java-compatible process defaults remain available; the stronger
defaults apply to production chart deployments and explicitly selected secure
process profiles.

## TLS configuration

Structured TLS deserialization and operational Java-properties loaders reject
unknown server modes, client-auth policies, and invalid security booleans. Valid
values remain case-insensitive;
`required` remains an alias for `require`. Omitted fields retain their existing
defaults. Properties files retain their comment and quoting syntax.

`TlsMode::parse_strict`, `TlsClientAuth::parse_strict`,
`TlsConfig::try_apply_java_property`, and `try_apply_java_properties_str` expose
typed failures. Applying a full properties snapshot is atomic. Startup rejects
invalid explicit policies even in permissive mode. A failed live reload retains
the previous acceptor and generation; it does not weaken client authentication.
Missing explicitly named properties files also fail; the default properties-file
location remains optional for compatibility.
The infallible legacy parsers retain Java fallback behavior for callers that
explicitly need that compatibility API.

The protobuf schemas, remoting request/response codes, Java ACL credential shape,
signature algorithm, and persisted storage layouts are unchanged. TLS encryption
is configured separately from authentication; the production Proxy TLS preset
enables gRPC TLS. These changes do not establish end-to-end cluster encryption or
replace deployment fault and interoperability qualification.
