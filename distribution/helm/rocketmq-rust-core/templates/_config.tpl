{{/* Startup schemas rendered before Pod creation; no shell substitution. */}}
{{- define "rocketmq-core.authConfig" -}}
authenticationEnabled = {{ .enabled }}
authorizationEnabled = {{ .enabled }}
authConfigPath = "/var/lib/rocketmq/auth"
{{ if .enabled -}}
aclFile = "/etc/rocketmq/acl/plain_acl.yml"
{{- end }}
{{- end -}}

{{- define "rocketmq-core.config" -}}
{{- $root := .root -}}
{{- $service := .service -}}
{{- $config := index $root.Values.services $service -}}
{{- if eq $service "namesrv" -}}
listenPort = {{ $config.port }}
bindAddress = "0.0.0.0"
kvConfigPath = "/var/lib/rocketmq/namesrv/kvConfig.json"
configStorePath = "/var/lib/rocketmq/namesrv/namesrv.properties"
{{ include "rocketmq-core.authConfig" $config.auth }}
{{- else if eq $service "controller" -}}
controllerType = "Raft"
nodeId = {{ add1 .ordinal }}
listenAddr = "0.0.0.0:{{ $config.port }}"
raftListenAddr = "0.0.0.0:{{ $config.raftPort }}"
storagePath = "/var/lib/rocketmq/controller/raft"
controllerStorePath = "/var/lib/rocketmq/controller"
storageBackend = {{ $config.storageBackend | quote }}
configStorePath = "/var/lib/rocketmq/controller/controller.toml"
configBlackList = "configBlackList;configStorePath;nodeId;listenAddr;raftListenAddr;raftPeers;controllerPeers;raftPeerEndpoints;controllerPeerEndpoints;storagePath;storageBackend"
{{ include "rocketmq-core.authConfig" $config.auth }}
{{ range $ordinal := until (int $config.replicas) }}
[[raftPeerEndpoints]]
id = {{ add1 $ordinal }}
addr = "{{ include "rocketmq-core.peerHost" (dict "root" $root "service" "controller" "ordinal" $ordinal) }}:{{ $config.raftPort }}"
{{ end }}
{{ range $ordinal := until (int $config.replicas) }}
[[controllerPeerEndpoints]]
id = {{ add1 $ordinal }}
addr = "{{ include "rocketmq-core.peerHost" (dict "root" $root "service" "controller" "ordinal" $ordinal) }}:{{ $config.port }}"
{{ end }}
{{- else if eq $service "broker" -}}
[broker]
listenPort = {{ $config.port }}
brokerIp1 = {{ include "rocketmq-core.peerHost" . | quote }}
brokerIp2 = {{ include "rocketmq-core.peerHost" . | quote }}
storePathRootDir = "/var/lib/rocketmq/store"
namesrvAddr = {{ include "rocketmq-core.addresses" (dict "root" $root "service" "namesrv") | quote }}
enableControllerMode = {{ $config.controllerMode }}
{{ if $config.controllerMode -}}
controllerAddr = {{ include "rocketmq-core.addresses" (dict "root" $root "service" "controller") | quote }}
{{ end -}}
{{ include "rocketmq-core.authConfig" $config.auth }}

[broker.brokerIdentity]
brokerName = {{ $config.brokerName | quote }}
brokerClusterName = {{ $root.Values.global.clusterName | quote }}
brokerId = {{ ternary (add1 .ordinal) .ordinal $config.controllerMode }}

[broker.brokerServerConfig]
bindAddress = "0.0.0.0"

[store]
storePathRootDir = "/var/lib/rocketmq/store"
storePathBrokerIdentity = "/var/lib/rocketmq/store/brokerIdentity"
{{ if or $config.controllerMode (gt (int .ordinal) 0) -}}
brokerRole = "SLAVE"
{{ else if gt (int $config.replicas) 1 -}}
brokerRole = "SYNC_MASTER"
{{ else -}}
brokerRole = "ASYNC_MASTER"
{{ end -}}
flushDiskType = {{ $config.flushDiskType | quote }}
haListenAddress = "0.0.0.0"
haListenPort = {{ $config.haPort }}
{{ if and (not $config.controllerMode) (gt (int .ordinal) 0) -}}
haMasterAddress = "{{ include "rocketmq-core.peerHost" (dict "root" $root "service" "broker" "ordinal" 0) }}:{{ $config.haPort }}"
{{ end -}}
totalReplicas = {{ $config.replicas }}
inSyncReplicas = {{ $config.minInSyncReplicas }}
minInSyncReplicas = {{ $config.minInSyncReplicas }}
{{- else if eq $service "proxy" -}}
mode = "cluster"

[grpc]
listenAddr = "0.0.0.0:{{ $config.port }}"

[grpc.tls]
enabled = {{ $config.tls.enabled }}
{{ if $config.tls.enabled -}}
certificatePath = "/etc/rocketmq/tls/tls.crt"
privateKeyPath = "/etc/rocketmq/tls/tls.key"
clientAuth = {{ $config.tls.clientAuth | quote }}
{{ if ne $config.tls.clientAuth "none" -}}
clientCaPath = "/etc/rocketmq/tls/ca.crt"
{{ end -}}
{{ end }}
[remoting]
enabled = false

[cluster]
namesrvAddr = {{ include "rocketmq-core.addresses" (dict "root" $root "service" "namesrv") | quote }}
brokerClusterName = {{ $root.Values.global.clusterName | quote }}
ioMaxInflight = {{ $config.cluster.ioMaxInflight }}
controlReserve = {{ $config.cluster.controlReserve }}
commandQueueCapacity = {{ $config.cluster.commandQueueCapacity }}

[auth]
{{ include "rocketmq-core.authConfig" $config.auth }}
{{- end -}}
{{- end -}}
