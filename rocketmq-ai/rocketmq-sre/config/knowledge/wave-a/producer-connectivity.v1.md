# Producer connectivity diagnosis

Use this runbook for send timeout, route-not-found, authentication failure, or connection churn.

## Required evidence

- Topic route and Broker readiness.
- Producer connection metadata through the read-only Admin adapter.
- Proxy or remoting error counts grouped by bounded outcome.
- Sanitized logs and traces without message body, token, address, or credential material.

## Interpretation

Fresh routes with healthy Brokers refute a cluster-wide route outage. Repeated timeout outcomes with connection churn support a transport path issue. Authentication outcome counts may support an identity/configuration issue, but raw principals and secrets must never leave the source.

## Read-only recommendation

Compare the failing client version, route epoch, TLS/ACL configuration version fingerprint, and deployment time. Do not modify ACLs, recreate topics, or retry sends with business payloads.
