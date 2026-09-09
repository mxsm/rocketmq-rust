# RocketMQ Rust core chart

This community chart deploys NameServer, Broker, Controller, and Proxy. Select a
profile with `-f values-dev-single.yaml`, `-f values-production-default-ha.yaml`,
`-f values-production-controller-ha.yaml`, or `-f values-production-proxy-tls.yaml`.
The single-node profile is for development. Production profiles require separate
hosts for replicas; configure storage classes and resources for the target cluster.

The chart defaults to `securityProfile: production` and enables authentication and
authorization for every service. Production rejects an override that disables
service authentication. `values-dev-single.yaml` explicitly selects development
and disables authentication for its local test deployment.

Every StatefulSet ordinal gets its own complete TOML configuration and stable DNS
identity. Peer addresses are internal Kubernetes addresses. Processes run without
a shell, with retained PVCs and HTTP startup, readiness, liveness, and drain probes.
The default shutdown budget is 45 seconds within a 60-second Pod grace period.

Use existing Secrets for `services.<service>.auth` and `services.proxy.tls`.
Clients need matching ACL credentials; a TLS Proxy image must include the `tls`
feature. Secret contents are never Helm values. Mounted Secret updates do not
reload credentials or certificates automatically; restart the affected processes
after rotation. See `values.yaml` for key names and mount options.

The default auth Secret names are `rocketmq-namesrv-auth`, `rocketmq-broker-auth`,
`rocketmq-controller-auth`, and `rocketmq-proxy-auth`. Create the Secrets for the
enabled services before installing the release, or override their names. Each
contains `plain_acl.yml`. Broker and Proxy Secrets also contain `inner-client.json`
with the Java-compatible `accessKey`, `secretKey`, and optional `securityToken`
fields. Grant those inner-client identities the required cluster permissions in
the receiving services' ACL files. Keep the credential values in Secrets.

The chart mounts the inner credentials and sets
`ROCKETMQ_INNER_CLIENT_CREDENTIALS_FILE`. Missing, malformed, or incomplete mounted
credentials fail initialization. Proxy enables its cluster ACL signer when auth is
enabled; it refuses to start with an invalid signer configuration. Existing inline
`innerClientAuthenticationCredentials` retain precedence for non-chart deployments.

The chart's `securityProfile` selects authentication defaults. The process-level
`ROCKETMQ_SECURITY_PROFILE=secure-enforced` also requires its existing bootstrap
materials and now rejects disabled authentication or authorization in Broker,
Controller, and Proxy, matching NameServer. Select TLS explicitly for the traffic
that requires encryption; the Proxy TLS preset configures its gRPC listener.

Configuration changes update Pod-template checksums. NameServer, Broker, and
Controller use `OnDelete`, so a Helm upgrade does not automatically restart them.
Proxy uses a Deployment with zero unavailable replicas. Controller membership,
identity, listeners, and storage configuration require a restart; changing the
configured peer list does not perform a Raft membership change. Keep those values
consistent with the persisted membership when upgrading an existing cluster.

## Restarting one Controller

The packaged `files/rollout.py` requires Python 3.11+, `kubectl`, and a matching
`rocketmq-admin-cli` build. Its Kubernetes context needs read access to the target
StatefulSet, Pods, ConfigMap, and PDB, plus permission to create Pod evictions.
Select the current Controller leader's remoting endpoint. For example, if ordinal
zero is the current leader, keep this port-forward running in a separate terminal:

```sh
kubectl -n rocketmq port-forward pod/core-controller-0 19878:9878
```

With ACL enabled, provide `ROCKETMQ_ACL_ACCESS_KEY`, `ROCKETMQ_ACL_SECRET_KEY`, and
optionally `ROCKETMQ_ACL_SECURITY_TOKEN` in the operator's environment. Then restart
one ordinal, using the actual release's StatefulSet name:

```sh
python files/rollout.py --namespace rocketmq --statefulset core-controller \
  --ordinal 1 --controller-address localhost:19878
```

The tool checks the live Controller membership and fresh replication evidence
before submitting an eviction with the target Pod UID. It verifies that the peer
addresses match the selected deployment and that a majority remains without the
target. Kubernetes enforces the matching PDB. Missing or unsupported observations,
stale peers, a follower endpoint, or a refused eviction stop the operation. There
is no automatic retry of a refused eviction and no direct Pod deletion fallback.

The command waits for a new Pod UID owned by the same StatefulSet to become Ready.
Run it separately for each next ordinal, selecting the current leader each time.
The observation is a current check, not a reservation against subsequent failures.
Do not overlap planned restarts. Direct Pod deletion and forced deletion bypass
this procedure and Kubernetes disruption budgets.

Broker PDBs preserve `minInSyncReplicas`. A two-replica group requiring both replicas
therefore has no voluntary disruption allowance; plan the replication/availability
tradeoff before maintenance. The Controller rollout tool does not manage Broker
role changes or data catch-up.

## Validation scope

The repository tests render all four profiles, load their TOML with the Rust
configuration loaders, and exercise a native three-voter Controller quorum. The
operator's Kubernetes interactions have deterministic subprocess tests. These
checks do not establish Pod recovery, PVC recovery, or production RPO/RTO; validate
the selected profile in its deployment environment before making those claims.
