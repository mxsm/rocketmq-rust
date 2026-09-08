{{- define "rocketmq-core.name" -}}
{{- printf "%s-%s" .Release.Name .service | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "rocketmq-core.labels" -}}
app.kubernetes.io/name: {{ include "rocketmq-core.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: {{ .service }}
app.kubernetes.io/managed-by: Helm
rocketmqrust.com/distribution: unofficial-community
{{- end -}}

{{- define "rocketmq-core.image" -}}
{{- printf "%s/%s:%s" .Values.global.imageRegistry .service .Values.global.candidateVersion -}}
{{- end -}}

{{- define "rocketmq-core.peerHost" -}}
{{- $name := include "rocketmq-core.name" (dict "Release" .root.Release "service" .service) -}}
{{- printf "%s-%d.%s-peer.%s.svc.%s" $name (int .ordinal) $name .root.Release.Namespace .root.Values.global.clusterDomain -}}
{{- end -}}

{{- define "rocketmq-core.addresses" -}}
{{- $config := index .root.Values.services .service -}}
{{- $addresses := list -}}
{{- range $ordinal := until (int $config.replicas) -}}
{{- $host := include "rocketmq-core.peerHost" (dict "root" $.root "service" $.service "ordinal" $ordinal) -}}
{{- $addresses = append $addresses (printf "%s:%d" $host (int $config.port)) -}}
{{- end -}}
{{- join ";" $addresses -}}
{{- end -}}

{{- define "rocketmq-core.validate" -}}
{{- if gt (len .Release.Name) 40 -}}
{{- fail "release name must be at most 40 characters to preserve unique peer DNS names" -}}
{{- end -}}
{{- $broker := .Values.services.broker -}}
{{- $controller := .Values.services.controller -}}
{{- if and $broker.enabled (not .Values.services.namesrv.enabled) -}}
{{- fail "broker requires namesrv in this chart" -}}
{{- end -}}
{{- if and .Values.services.proxy.enabled (or (not $broker.enabled) (not .Values.services.namesrv.enabled)) -}}
{{- fail "cluster proxy requires broker and namesrv" -}}
{{- end -}}
{{- if and $broker.controllerMode (not $controller.enabled) -}}
{{- fail "controllerMode requires the Controller service" -}}
{{- end -}}
{{- if and $controller.enabled (or (lt (int $controller.replicas) 3) (eq (mod (int $controller.replicas) 2) 0)) -}}
{{- fail "Controller HA requires an odd membership of at least three replicas" -}}
{{- end -}}
{{- if or (gt (int $broker.minInSyncReplicas) (int $broker.replicas)) (and $broker.controllerMode (lt (int $broker.minInSyncReplicas) 2)) -}}
{{- fail "minInSyncReplicas must fit broker replicas and be at least two in Controller mode" -}}
{{- end -}}
{{- if le (int .Values.runtime.terminationGracePeriodSeconds) (int .Values.runtime.shutdownTimeoutSeconds) -}}
{{- fail "terminationGracePeriodSeconds must exceed shutdownTimeoutSeconds" -}}
{{- end -}}
{{- $cluster := .Values.services.proxy.cluster -}}
{{- if or (ge (int $cluster.controlReserve) (int $cluster.ioMaxInflight)) (lt (int $cluster.commandQueueCapacity) (int $cluster.ioMaxInflight)) -}}
{{- fail "proxy requires controlReserve < ioMaxInflight <= commandQueueCapacity" -}}
{{- end -}}
{{- range $service, $config := .Values.services -}}
{{- if $config.enabled -}}
{{- $requestCpu := include "rocketmq-core.cpuMillis" $config.resources.requests.cpu | float64 -}}
{{- $limitCpu := include "rocketmq-core.cpuMillis" $config.resources.limits.cpu | float64 -}}
{{- $requestMemory := include "rocketmq-core.memoryMi" $config.resources.requests.memory | int64 -}}
{{- $limitMemory := include "rocketmq-core.memoryMi" $config.resources.limits.memory | int64 -}}
{{- if or (le $requestCpu 0.0) (gt $requestCpu $limitCpu) (gt $requestMemory $limitMemory) -}}
{{- fail (printf "%s resources require positive requests no greater than limits" $service) -}}
{{- end -}}
{{- $ports := list (int $config.port) (int $.Values.runtime.healthPort) -}}
{{- if eq $service "broker" -}}{{- $ports = append $ports (int $config.haPort) -}}{{- end -}}
{{- if eq $service "controller" -}}{{- $ports = append $ports (int $config.raftPort) -}}{{- end -}}
{{- if ne (len $ports) (len (uniq $ports)) -}}{{- fail (printf "%s listener ports must be distinct" $service) -}}{{- end -}}
{{- if and $config.auth.enabled (empty $config.auth.secretName) -}}
{{- fail (printf "services.%s.auth.secretName is required when auth is enabled" $service) -}}
{{- end -}}

{{- end -}}
{{- end -}}
{{- if and .Values.services.proxy.tls.enabled (empty .Values.services.proxy.tls.secretName) -}}
{{- fail "services.proxy.tls.secretName is required when TLS is enabled" -}}
{{- end -}}
{{- end -}}

{{- define "rocketmq-core.cpuMillis" -}}
{{- if hasSuffix "m" . -}}{{- trimSuffix "m" . -}}
{{- else -}}{{- mulf (float64 .) 1000 -}}{{- end -}}
{{- end -}}

{{- define "rocketmq-core.memoryMi" -}}
{{- if hasSuffix "Gi" . -}}{{- mul (trimSuffix "Gi" . | int64) 1024 -}}
{{- else -}}{{- trimSuffix "Mi" . -}}{{- end -}}
{{- end -}}
