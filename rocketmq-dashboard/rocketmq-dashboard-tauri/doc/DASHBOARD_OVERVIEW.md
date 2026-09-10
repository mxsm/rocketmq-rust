# Global dashboard overview

The authenticated overview command reuses the Cluster, Topic, Consumer and Producer managers and checks connection revision before and after the combined read. No additional admin session or detached worker is created.

Statuses distinguish UNCONFIGURED, DOWN (all metrics unavailable), PARTIAL, and READY (queries returned). Each nullable metric also carries complete, reported, partial, or unknown quality. Topic catalog counts are reported because the existing catalog does not expose source coverage. Consumer and Producer counts use workspace inventory failure evidence; total lag sums only observed nonnegative values and is explicitly partial if any group is missing. No unavailable metric is substituted with zero.

Existing Broker TPS/Top and Topic queue/type charts remain. New cards show Topic, Consumer-group, Producer-group and observed lag counts. Actions use the shared navigation store to open NameServer configuration, Broker availability, Consumer backlog, or a specific lagging group's progress. Refresh timestamps describe observations, not persisted history.
