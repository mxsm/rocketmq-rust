---
title: "Admin CLI reference"
---

# Admin CLI reference

`rocketmq-admin-cli` exposes domain subcommands over Admin Core. Use it for a selected cluster or Broker; it does not start those services. The current registration contains 17 domains and 102 leaf commands, plus the root `show` command. This catalog follows the parser and locally inspected help, not an older command count in a README.

## Invocation and discovery

Run these commands from the repository root:

```bash
cargo run -p rocketmq-admin-cli -- --help
cargo run -p rocketmq-admin-cli -- topic updateTopic --help
cargo run -p rocketmq-admin-cli -- broker updateBrokerConfig --help
```

After building, use `target/debug/rocketmq-admin-cli` (Windows: `target/debug/rocketmq-admin-cli.exe`). Cargo consumes options before `--`; the CLI consumes options after it. `--verbose` is global and adds controlled diagnostic fields to errors. `--generate-completion` supports shells selected by its parser, including Bash, Zsh, Fish and PowerShell. `show` displays the category table.

There is no universal global `-n`, `--yes`, `--json`, `--format` or timeout flag. A leaf may embed common arguments or define its own. Always inspect that leaf. The argument normalizer translates legacy `-bn` to `--brokerName`; it is not a general Java mqadmin compatibility layer.

## Command catalog

Read and mutation commands share domains. The effect column describes the category; individual help and the service response determine the selected operation's actual target and outcome.

| Domain | Registered subcommands | Effect |
| --- | --- | --- |
| `auth` | `copyAcl`, `copyUser`, `createAcl`, `createUser`, `deleteAcl`, `deleteUser`, `getAcl`, `getUser`, `listAcl`, `listUser`, `updateAcl`, `updateUser` | Read identities/ACLs with get/list; copy/create/update/delete change access control. |
| `broker` | `brokerConsumeStats`, `brokerStatus`, `cleanExpiredCQ`, `cleanUnusedTopic`, `deleteExpiredCommitLog`, `getBrokerConfig`, `getBrokerEpoch`, `getColdDataFlowCtrInfo`, `removeColdDataFlowCtrGroupConfig`, `resetMasterFlushOffset`, `sendMsgStatus`, `switchTimerEngine`, `updateColdDataFlowCtrGroupConfig`, `updateBrokerConfig`, `setCommitLogReadAheadMode` | Status/configuration/epoch reads; configuration, cleanup, offset and timer commands mutate state. sendMsgStatus sends probe messages. |
| `cluster` | `clusterList`, `clusterRT` | clusterList reads topology; clusterRT runs a message latency probe. |
| `connection` | `consumerConnection`, `producerConnection` | Read producer/consumer connections. |
| `consumer` | `consumerStatus`, `consumer`, `deleteSubGroup`, `getConsumerConfig`, `setConsumeMode`, `startMonitoring`, `updateSubGroupList`, `updateSubGroup`, `consumerProgress` | Read status/configuration/progress or monitor; update/delete and setConsumeMode change group metadata or behavior. |
| `controller` | `cleanBrokerMetadata`, `electMaster`, `getControllerConfig`, `getControllerMetaData`, `updateControllerConfig` | Read metadata/configuration; cleanBrokerMetadata, electMaster and updateControllerConfig change control-plane state. |
| `export` | `exportConfigs`, `exportMetrics`, `exportMetadataInRocksDB`, `exportMetadata`, `exportPopRecord`, `rocksDBConfigToJson` | Export selected information to local output; these commands do not constitute a coordinated message-store backup. |
| `ha` | `getSyncStateSet`, `haStatus` | Read HA and synchronization-set state. |
| `lite` | `getBrokerLiteInfo`, `getLiteClientInfo`, `getLiteGroupInfo`, `getLiteTopicInfo`, `getParentTopicInfo`, `triggerLiteDispatch` | Read Lite routing/client/group state; triggerLiteDispatch actively triggers dispatch. |
| `message` | `checkMsgSendRT`, `consumeMessage`, `decodeMessageId`, `dumpCompactionLog`, `printMsg`, `printMsgByQueue`, `queryMsgById`, `queryMsgByKey`, `queryMsgByOffset`, `queryMsgByUniqueKey`, `queryMsgTraceById`, `sendMessage` | Query/decode/print/pull data; sendMessage and checkMsgSendRT produce messages. Pulling/printing is not proof of application completion. |
| `nameserver` | `addWritePerm`, `deleteKvConfig`, `getNamesrvConfig`, `updateKvConfig`, `updateNamesrvConfig`, `wipeWritePerm` | getNamesrvConfig reads; remaining commands modify configuration, KV metadata or Broker write permission. |
| `offset` | `cloneGroupOffset`, `getConsumerStatus`, `resetOffsetByTime`, `resetOffsetByTimeOld`, `skipAccumulatedMessage` | getConsumerStatus reads; clone/reset/skip change consumer progress and may replay or bypass retained messages. |
| `producer` | `producer` | Read producer information. |
| `queue` | `checkRocksdbCqWriteProgress`, `queryCq` | Inspect ConsumeQueue and RocksDB ConsumeQueue progress. |
| `release-checkpoint` | `capabilities`, `create-set`, `verify-set`, `restore-verify` | Validate local JSON inputs; create-set writes a new combined manifest. No automatic RPC checkpoint creation or data restoration. |
| `stats` | `statsAll` | Read aggregated statistics. |
| `topic` | `allocateMQ`, `deleteTopic`, `remappingStaticTopic`, `topicClusterList`, `topicList`, `topicRoute`, `topicStatus`, `updateOrderConf`, `updateStaticTopic`, `updateTopicList`, `updateTopicPerm`, `updateTopic` | Read lists/routes/status or calculate allocation; create/update/delete/remap operations change topic metadata. |

## Addresses and credentials

| Input | Scope and default behavior |
| --- | --- |
| `NAMESRV_ADDR` | Client discovery environment input; quote semicolon-separated addresses. Useful for commands such as `clusterList` and `updateSubGroup` that do not accept `-n`. |
| `-n / --namesrvAddr` | Only on leaves that declare it, including `topic updateTopic` and `nameserver updateNamesrvConfig`. |
| `-b / --brokerAddr` | A Broker endpoint on commands that declare it; do not confuse it with `message sendMessage -b / --broker`, which takes a Broker name. |
| `-c / --clusterName` | A cluster target where supported. It can expand one action to several Brokers. |
| `ROCKETMQ_ACL_ACCESS_KEY` and `ROCKETMQ_ACL_SECRET_KEY` | Optional paired credentials loaded by the CLI. Blank values are treated as absent; an incomplete pair is an argument error. |
| `ROCKETMQ_ACL_SECURITY_TOKEN` | Optional security token used with credentials. |

Supply credentials through the process environment using your secret-management workflow; avoid putting them in command examples or captured diagnostic output. Successful authentication does not grant every operation: the target service's authorization still applies.

For a local cluster already running on the documented port:

```bash
export NAMESRV_ADDR='127.0.0.1:9876'
cargo run -p rocketmq-admin-cli -- cluster clusterList
```

```powershell
$env:NAMESRV_ADDR = '127.0.0.1:9876'
cargo run -p rocketmq-admin-cli -- cluster clusterList
```

The expected result is a topology table. An empty or failed query does not justify creating replacement cluster metadata. Use [first diagnosis](../operations/first-diagnosis.md) to separate discovery, advertised addresses, Broker readiness and credentials.

## Frequently used parameters

| Command | Required/important inputs | Defaults and effect |
| --- | --- | --- |
| `topic updateTopic` | `-t / --topic`; select a Broker with `-b` or a cluster with `-c` | Read/write queue counts `-r`/`-w` default to 8; `-p / --perm` accepts 2 write, 4 read, 6 read/write. Creates or updates metadata. `--order`, `--unit` and `--hasUnitSub` take Boolean values. |
| `consumer updateSubGroup` | `-g / --groupName`; select `-b` or `-c` | Creates or updates a group; optional consumption switches take Boolean values. Retry counts/policy, Broker selection and attributes are explicit options. No `-n` on this leaf. |
| `broker updateBrokerConfig` | Exactly one target `-b`/`-c`; one `-k` with `-v`, or repeated `-p / --property KEY=VALUE` | `--dryRun` (alias `--dry-run`) previews validation without applying changes. `--noRollback` disables automatic rollback after partial cluster failure. `-y` skips the command's confirmation. |
| `nameserver updateNamesrvConfig` | `-k / --key` and `-v / --value`; optional `-n` | NameServer's durable desired configuration and effective configuration are distinct. Some accepted changes require restart. |
| `offset resetOffsetByTime` | `-g / --group`, `-t / --topic`, `-s / --timestamp` | Timestamp accepts `now`, epoch milliseconds or `yyyy-MM-dd#HH:mm:ss:SSS`. `now` skips accumulated backlog; the command can affect all queues for the topic. |
| `message sendMessage` | `-t / --topic` and `-p / --body` | UTF-8 body; optional `-k / --key`, `-c / --tags`, `-b / --broker` and `-i / --qid`. `-m / --msgTraceEnable` defaults to false. Sends real data. |

Use explicit numeric timestamps when an operational script must avoid ambiguous formatted-date interpretation. Coordinate consumer ownership before reset; the help describes online notification and an offline legacy fallback, but a successful request does not establish that every application has applied the new cursor. Inspect group progress and business replay effects afterward.

## Preview a supported Broker change

The following command contacts the selected local Broker to validate an update without applying it. It assumes the service and credentials are already configured; it was not run against a live Broker while preparing this reference.

```bash
cargo run -p rocketmq-admin-cli -- broker updateBrokerConfig -b 127.0.0.1:10911 -p autoCreateTopicEnable=false --dryRun
```

Read [Broker configuration](../configuration/broker-config.md) before applying the update. Ordinary runtime updates support only the documented live keys and return `persisted=false`; update the managed startup configuration separately when the setting must survive restart. Automatic rollback across several Brokers is best-effort recovery, not a distributed atomic commit. Retain per-Broker outcomes and investigate a partial failure before retrying.

## Output, exit status and scripting

Output is command-specific: tables, text, JSON or files. Do not parse every command as JSON or assume column layouts form a stable machine API. For an application integration, prefer typed Admin Core operations with explicit response handling.

| Exit code | Canonical category |
| --- | --- |
| 0 | Successful execution or help/version display |
| 64 | Invalid usage/arguments |
| 65 | Invalid data or mapped conflict/state error |
| 66 | Missing entity |
| 69 | Unavailable service |
| 70 | Internal software failure |
| 75 | Temporary failure |
| 77 | Permission failure |
| 78 | Configuration failure |

The command's canonical error descriptor determines its mapped exit code. See [errors](./errors.md) for retry hints: code 75 is not permission to blindly repeat message sends or partially applied mutations. Default errors retain a stable code and safe message. `--verbose` adds bounded diagnostic context without making raw causes or credentials public.

Capture the actual CLI process status; PowerShell exposes it as `$LASTEXITCODE`. When using `cargo run`, a compilation failure is Cargo's failure, not a service error from this table. For reliable operations, build first and invoke the binary directly.

## Checkpoint and export boundaries

`release-checkpoint capabilities` reads a capabilities-response JSON file; it does not call the service. `create-set` combines supplied Controller and Store manifests and writes a new output file. `verify-set` and `restore-verify` inspect supplied manifests/proofs. They do not stop writers, copy the store, start a replacement node or prove a live application recovered. Follow [maintenance](../operations/maintenance.md) and [backup/recovery](../operations/backup-recovery.md) for the surrounding procedure.

Validation for this page inspected the existing binary's root/domain help and representative leaf help, plus the current parser and execution boundaries. It did not execute destructive commands, offset resets or remote mutations.

Sources: [registration and common arguments](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-tools/rocketmq-admin/rocketmq-admin-cli/src/commands.rs), [CLI parsing and credentials](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-tools/rocketmq-admin/rocketmq-admin-cli/src/rocketmq_cli.rs), [entry point](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-tools/rocketmq-admin/rocketmq-admin-cli/src/main.rs), [checkpoint commands](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-tools/rocketmq-admin/rocketmq-admin-cli/src/commands/release_checkpoint.rs).
