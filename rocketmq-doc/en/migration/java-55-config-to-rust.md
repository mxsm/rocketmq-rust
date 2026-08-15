# Convert a Java 5.5 broker configuration

RocketMQ Rust keeps TOML as its canonical, strict configuration format. A Java
5.5 flat `.conf` or `.properties` file can be converted at the Broker startup
boundary without making canonical TOML accept Java aliases.

```text
rocketmq-broker-rust \
  --configFile conf/broker.conf \
  --config-format properties \
  --conversion-report target/broker-config-conversion.json
```

The explicit format wins. Without it, `.toml` selects canonical TOML and
`.conf`/`.properties` selects Java properties; other extensions fail closed.

The converter validates the complete input once and returns one atomic result:
the canonical Broker/Store configuration, the per-key conversion entries, and
warnings. Broker startup writes the redacted report before consuming the
configuration. A parse, validation, or report-write failure prevents listener
startup.

Java `storeType=default` and `storeType=defaultRocksDB` are accepted with ASCII
case-insensitive comparison and normalize to `LocalFile` and `RocksDB`.
Canonical TOML accepts only those canonical spellings. Duplicate keys, unknown
keys, conflicting aliases, unsupported values, and all DLedger CommitLog keys
are rejected.

Authentication and ACL path values map to the canonical `[broker]` fields. The
report records that referenced files must be copied or mounted separately; it
does not embed file contents or reveal sensitive values.
