{{/* Common chart identity. */}}
{{- define "rocketmq.partOf" -}}
rocketmq-rust
{{- end -}}

{{- define "rocketmq.serviceName" -}}
rocketmq-{{ .service }}
{{- end -}}

{{/* Parse the chart's deliberately narrow positive whole-second/minute duration format. */}}
{{- define "rocketmq.durationSeconds" -}}
{{- $value := .value -}}
{{- if not (regexMatch "^[1-9][0-9]{0,8}[sm]$" $value) -}}
  {{- fail (printf "%s must be a positive whole number of seconds or minutes (for example 30s or 2m, with at most nine digits)" .field) -}}
{{- end -}}
{{- $magnitude := int (trimSuffix "s" (trimSuffix "m" $value)) -}}
{{- if hasSuffix "m" $value -}}
{{ mul $magnitude 60 }}
{{- else -}}
{{ $magnitude }}
{{- end -}}
{{- end -}}

{{/* Fail early when cross-field topology invariants cannot be expressed by JSON Schema. */}}
{{- define "rocketmq.validateValues" -}}
{{- $profile := .Values.deploymentProfile -}}
{{- $releaseCommit := required "releaseIdentity.commit is required" .Values.releaseIdentity.commit -}}
{{- if or
      (eq $releaseCommit "0000000000000000000000000000000000000000")
      (not (regexMatch "^[0-9a-f]{40}$" $releaseCommit)) -}}
  {{- fail "releaseIdentity.commit must be a non-zero 40-character lowercase hexadecimal commit" -}}
{{- end -}}
{{- $releaseNonce := required "releaseIdentity.nonce is required for every rollout" .Values.releaseIdentity.nonce -}}
{{- if not (regexMatch "^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$" $releaseNonce) -}}
  {{- fail "releaseIdentity.nonce must be 1..=63 lowercase ASCII letters, digits, or interior hyphens" -}}
{{- end -}}
{{- $configDigest := required "releaseIdentity.configDigest is required" .Values.releaseIdentity.configDigest -}}
{{- if not (regexMatch "^sha256:[0-9a-f]{64}$" $configDigest) -}}
  {{- fail "releaseIdentity.configDigest must be a lowercase SHA-256 digest" -}}
{{- end -}}
{{- $secretVersion := required "releaseIdentity.secretVersion is required" .Values.releaseIdentity.secretVersion -}}
{{- if not (regexMatch "^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$" $secretVersion) -}}
  {{- fail "releaseIdentity.secretVersion must be an opaque 1..=128 character version identifier" -}}
{{- end -}}
{{- if lt (int .Values.releaseIdentity.storageGeneration) 1 -}}
  {{- fail "releaseIdentity.storageGeneration must be at least 1" -}}
{{- end -}}
{{- $maintenancePolicy := .Files.Get "files/maintenance-policy.json" -}}
{{- if .Values.maintenance.enabled -}}
  {{- if not $maintenancePolicy -}}
    {{- fail "maintenance.enabled=true requires files/maintenance-policy.json" -}}
  {{- end -}}
  {{- if ne (sha256sum $maintenancePolicy) .Values.maintenance.sha256 -}}
    {{- fail "maintenance policy bytes do not match maintenance.sha256" -}}
  {{- end -}}
  {{- if ne (int .Values.maintenance.version) 1 -}}
    {{- fail "maintenance policy version must match the chart policy version" -}}
  {{- end -}}
{{- end -}}
{{- if eq $profile "production-controller-ha" -}}
  {{- if not .Values.maintenance.enabled -}}
    {{- fail "production-controller-ha requires maintenance.enabled=true" -}}
  {{- end -}}
  {{- $controllerReplicas := int .Values.services.controller.replicas -}}
  {{- $controllerQuorum := add (div $controllerReplicas 2) 1 -}}
  {{- if or (lt $controllerReplicas 3) (eq (mod $controllerReplicas 2) 0) -}}
    {{- fail "production-controller-ha requires an odd Controller replica count of at least 3" -}}
  {{- end -}}
  {{- if ne (len .Values.services.controller.peerServiceClusterIPs) $controllerReplicas -}}
    {{- fail "production-controller-ha requires one unique Controller peer Service IP per Controller replica" -}}
  {{- end -}}
  {{- if lt (int .Values.services.broker.replicas) 3 -}}
    {{- fail "production-controller-ha requires at least 3 Broker replicas in the Controller-managed replica group" -}}
  {{- end -}}
  {{- if ne .Values.services.controller.storageBackend "RocksDB" -}}
    {{- fail "production-controller-ha requires Controller RocksDB storage" -}}
  {{- end -}}
  {{- range $service := list "broker" "namesrv" "controller" -}}
    {{- $config := index $.Values.services $service -}}
    {{- if not $config.persistence.enabled -}}
      {{- fail (printf "production-controller-ha requires services.%s.persistence.enabled=true" $service) -}}
    {{- end -}}
  {{- end -}}
  {{- if lt (int .Values.services.controller.pdb.minAvailable) (int $controllerQuorum) -}}
    {{- fail (printf "Controller PDB minAvailable must preserve quorum (%d)" $controllerQuorum) -}}
  {{- end -}}
{{- else if eq $profile "dev-single" -}}
  {{- if or (ne (int .Values.services.broker.replicas) 1) (ne (int .Values.services.controller.replicas) 1) -}}
    {{- fail "dev-single requires exactly one Broker and one Controller" -}}
  {{- end -}}
{{- else -}}
  {{- fail (printf "unsupported deploymentProfile %q" $profile) -}}
{{- end -}}
{{- range $service, $config := .Values.services -}}
  {{- if and $config.pdb.enabled (gt (int $config.pdb.minAvailable) (int $config.replicas)) -}}
    {{- fail (printf "services.%s.pdb.minAvailable cannot exceed replicas" $service) -}}
  {{- end -}}
{{- end -}}
{{- if and .Values.metrics.enabled (not .Values.metrics.service.enabled) -}}
  {{- fail "metrics.enabled=true requires metrics.service.enabled=true" -}}
{{- end -}}
{{- if and .Values.metrics.serviceMonitor.enabled (not .Values.metrics.enabled) -}}
  {{- fail "metrics.serviceMonitor.enabled=true requires metrics.enabled=true" -}}
{{- end -}}
{{- if and .Values.metrics.serviceMonitor.enabled (not .Values.metrics.service.enabled) -}}
  {{- fail "metrics.serviceMonitor.enabled=true requires metrics.service.enabled=true" -}}
{{- end -}}
{{- if and .Values.metrics.enabled .Values.networkPolicy.enabled (not .Values.metrics.networkPolicy.enabled) -}}
  {{- fail "metrics.enabled=true with the chart NetworkPolicy requires metrics.networkPolicy.enabled=true" -}}
{{- end -}}
{{- $metricsPath := .Values.metrics.path -}}
{{- if or
      (and (ne $metricsPath "/") (hasSuffix "/" $metricsPath))
      (contains "//" $metricsPath)
      (contains "/./" $metricsPath)
      (contains "/../" $metricsPath)
      (hasSuffix "/." $metricsPath)
      (hasSuffix "/.." $metricsPath) -}}
  {{- fail "metrics.path must be canonical and cannot contain empty, dot, or parent segments" -}}
{{- end -}}
{{- if has (int .Values.metrics.port) (list 8080 8081 8088 8089 9876 10911 10912 60109 60110) -}}
  {{- fail "metrics.port conflicts with a fixed RocketMQ service or health port" -}}
{{- end -}}
{{- if .Values.metrics.serviceMonitor.enabled -}}
  {{- $intervalSeconds := int (include "rocketmq.durationSeconds" (dict
        "field" "metrics.serviceMonitor.interval"
        "value" .Values.metrics.serviceMonitor.interval)) -}}
  {{- $scrapeTimeoutSeconds := int (include "rocketmq.durationSeconds" (dict
        "field" "metrics.serviceMonitor.scrapeTimeout"
        "value" .Values.metrics.serviceMonitor.scrapeTimeout)) -}}
  {{- if gt $scrapeTimeoutSeconds $intervalSeconds -}}
    {{- fail "metrics.serviceMonitor.scrapeTimeout must be less than or equal to metrics.serviceMonitor.interval" -}}
  {{- end -}}
{{- end -}}
{{- end -}}

{{- define "rocketmq.labels" -}}
app.kubernetes.io/name: {{ include "rocketmq.serviceName" . }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/part-of: {{ include "rocketmq.partOf" . }}
app.kubernetes.io/managed-by: {{ .root.Release.Service }}
app.kubernetes.io/version: {{ .root.Chart.AppVersion | quote }}
rocketmqrust.com/service: {{ .service }}
rocketmqrust.com/architecture-milestone: P0-05
{{- end -}}

{{/* Shared pre-bind lifecycle and security contract. The health port is kubelet-only and is not exposed by Services. */}}
{{- define "rocketmq.lifecycleEnv" -}}
- {name: ROCKETMQ_HEALTH_BIND_ADDR, value: "0.0.0.0:8088"}
- {name: ROCKETMQ_SHUTDOWN_TIMEOUT_SECONDS, value: "45"}
- {name: ROCKETMQ_LIVENESS_STALE_SECONDS, value: "30"}
- {name: ROCKETMQ_SECURITY_PROFILE, value: "secure-enforced"}
- {name: ROCKETMQ_SECURITY_TRUST_ANCHOR, value: "/var/run/secrets/rocketmq/ca.crt"}
- {name: ROCKETMQ_SECURITY_TLS_CERT, value: "/var/run/secrets/rocketmq/tls.crt"}
- {name: ROCKETMQ_SECURITY_TLS_KEY, value: "/var/run/secrets/rocketmq/tls.key"}
- {name: ROCKETMQ_SECURITY_SECRET_PROVIDER, value: "mounted-files"}
- {name: ROCKETMQ_SECURITY_ADMIN_IDENTITY, value: "/var/run/secrets/rocketmq/admin.identity"}
- {name: ROCKETMQ_SECURITY_REQUEST_POLICY, value: "/var/run/secrets/rocketmq/request-policy.json"}
{{- end -}}

{{/* Release identity is always supplied as a process-root input. */}}
{{- define "rocketmq.releaseIdentityEnv" -}}
- {name: ROCKETMQ_RELEASE_COMMIT, value: {{ required "releaseIdentity.commit is required" .Values.releaseIdentity.commit | quote }}}
- {name: ROCKETMQ_RELEASE_NONCE, value: {{ required "releaseIdentity.nonce is required" .Values.releaseIdentity.nonce | quote }}}
- {name: ROCKETMQ_RELEASE_CONFIG_DIGEST, value: {{ required "releaseIdentity.configDigest is required" .Values.releaseIdentity.configDigest | quote }}}
- {name: ROCKETMQ_RELEASE_SECRET_VERSION, value: {{ required "releaseIdentity.secretVersion is required" .Values.releaseIdentity.secretVersion | quote }}}
- {name: ROCKETMQ_STORAGE_GENERATION, value: {{ .Values.releaseIdentity.storageGeneration | quote }}}
{{- end -}}

{{/* Explicit compatibility mode: environment values override canonical observability file values. */}}
{{- define "rocketmq.observabilityEnvironmentOverrides" -}}
{{- if .Values.global.observability.environmentOverridesEnabled }}
- {name: ROCKETMQ_METRICS_ENABLED, value: {{ .Values.metrics.enabled | quote }}}
- {name: ROCKETMQ_METRICS_EXPORTER, value: {{ ternary "prometheus" "disable" .Values.metrics.enabled | quote }}}
- {name: ROCKETMQ_METRICS_BIND_ADDR, value: {{ printf "0.0.0.0:%d" (int .Values.metrics.port) | quote }}}
- {name: ROCKETMQ_METRICS_PATH, value: {{ .Values.metrics.path | quote }}}
- {name: OTEL_EXPORTER_OTLP_ENDPOINT, value: {{ include "rocketmq.observabilityOtlpEndpoint" . | quote }}}
- {name: OTEL_EXPORTER_OTLP_PROTOCOL, value: {{ .Values.global.observability.otlpProtocol | quote }}}
{{- end }}
{{- end -}}

{{/* Resolve the structured OTLP endpoint while preserving the legacy alias when only it was customized. */}}
{{- define "rocketmq.observabilityOtlpEndpoint" -}}
{{- $defaultEndpoint := "http://otel-collector.observability.svc.cluster.local:4317" -}}
{{- $structuredEndpoint := .Values.global.observability.otlpEndpoint -}}
{{- $legacyEndpoint := .Values.global.otelEndpoint -}}
{{- if ne $structuredEndpoint $defaultEndpoint -}}
{{- $structuredEndpoint -}}
{{- else if ne $legacyEndpoint $defaultEndpoint -}}
{{- $legacyEndpoint -}}
{{- else -}}
{{- $structuredEndpoint -}}
{{- end -}}
{{- end -}}

{{/* Canonical file configuration shared by Broker, NameServer, Controller, Proxy, and MCP. */}}
{{- define "rocketmq.observabilityConfig" -}}
[observability]
[observability.metrics]
exporter = {{ .Values.global.observability.metricsExporter | quote }}
[observability.traces]
exporter = {{ .Values.global.observability.tracesExporter | quote }}
[observability.logs]
exporter = {{ .Values.global.observability.logsExporter | quote }}
[observability.otlp]
endpoint = {{ include "rocketmq.observabilityOtlpEndpoint" . | quote }}
protocol = {{ .Values.global.observability.otlpProtocol | quote }}
[observability.prometheus]
host = "0.0.0.0"
port = {{ .Values.metrics.port }}
path = {{ .Values.metrics.path | quote }}
{{- end -}}

{{- define "rocketmq.releaseAnnotations" -}}
rocketmqrust.com/release-commit: {{ .Values.releaseIdentity.commit | quote }}
rocketmqrust.com/release-nonce: {{ .Values.releaseIdentity.nonce | quote }}
rocketmqrust.com/release-config-digest: {{ .Values.releaseIdentity.configDigest | quote }}
rocketmqrust.com/release-secret-version: {{ .Values.releaseIdentity.secretVersion | quote }}
rocketmqrust.com/storage-generation: {{ .Values.releaseIdentity.storageGeneration | quote }}
{{- end -}}

{{/* Protected diagnostics are separate from the anonymous health listener and disabled by default. */}}
{{- define "rocketmq.runtimeDiagnosticsEnv" -}}
{{- if .Values.global.runtimeDiagnostics.enabled }}
- {name: ROCKETMQ_RUNTIME_DIAGNOSTICS_BIND_ADDR, value: {{ .Values.global.runtimeDiagnostics.bindAddress | quote }}}
- {name: ROCKETMQ_RUNTIME_DIAGNOSTICS_TOKEN_FILE, value: {{ .Values.global.runtimeDiagnostics.tokenFile | quote }}}
- {name: ROCKETMQ_RUNTIME_DIAGNOSTICS_SAMPLE_INTERVAL_SECONDS, value: {{ .Values.global.runtimeDiagnostics.sampleIntervalSeconds | quote }}}
- {name: ROCKETMQ_RUNTIME_DIAGNOSTICS_ALLOW_INSECURE_HTTP, value: {{ .Values.global.runtimeDiagnostics.allowInsecureHttp | quote }}}
{{- end }}
{{- end -}}

{{- define "rocketmq.lifecycleProbes" -}}
lifecycle:
  preStop:
    httpGet:
      path: /drainz
      port: health
      scheme: HTTP
readinessProbe:
  httpGet:
    path: /readyz
    port: health
    scheme: HTTP
  periodSeconds: 5
  timeoutSeconds: 1
  failureThreshold: 1
livenessProbe:
  httpGet:
    path: /livez
    port: health
    scheme: HTTP
  initialDelaySeconds: 10
  periodSeconds: 10
  timeoutSeconds: 1
  failureThreshold: 3
{{- end -}}

{{- define "rocketmq.selectorLabels" -}}
app.kubernetes.io/name: {{ include "rocketmq.serviceName" . }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
rocketmqrust.com/service: {{ .service }}
{{- end -}}

{{- define "rocketmq.image" -}}
{{- $repository := required (printf "services.%s.image.repository is required" .service) .image.repository -}}
{{- if .image.digest -}}
{{ printf "%s@%s" $repository .image.digest }}
{{- else -}}
{{- $tag := required (printf "services.%s.image.tag is required when digest is empty" .service) .image.tag -}}
{{ printf "%s:%s" $repository $tag }}
{{- end -}}
{{- end -}}

{{- define "rocketmq.namesrvAddresses" -}}
{{- if .Values.services.namesrv.discovery.enabled -}}
rocketmq-namesrv-discovery.{{ .Release.Namespace }}.svc.cluster.local:9876
{{- else -}}
{{- $addresses := list -}}
{{- range $ordinal := until (int .Values.services.namesrv.replicas) -}}
  {{- $addresses = append $addresses (printf "rocketmq-namesrv-%d.rocketmq-namesrv-headless.%s.svc.cluster.local:9876" $ordinal $.Release.Namespace) -}}
{{- end -}}
{{ join ";" $addresses }}
{{- end -}}
{{- end -}}

{{- define "rocketmq.controllerAddresses" -}}
{{- if eq .Values.deploymentProfile "dev-single" -}}
rocketmq-controller.{{ .Release.Namespace }}.svc.cluster.local:60109
{{- else -}}
{{- $addresses := list -}}
{{- range $address := .Values.services.controller.peerServiceClusterIPs -}}
  {{- $addresses = append $addresses (printf "%s:60109" $address) -}}
{{- end -}}
{{ join ";" $addresses }}
{{- end -}}
{{- end -}}

{{- define "rocketmq.podSecurityContext" -}}
runAsNonRoot: true
runAsUser: {{ .Values.global.podSecurity.runAsUser }}
runAsGroup: {{ .Values.global.podSecurity.runAsGroup }}
fsGroup: {{ .Values.global.podSecurity.fsGroup }}
fsGroupChangePolicy: OnRootMismatch
seccompProfile:
  type: RuntimeDefault
{{- end -}}

{{- define "rocketmq.containerSecurityContext" -}}
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
runAsNonRoot: true
runAsUser: {{ .Values.global.podSecurity.runAsUser }}
runAsGroup: {{ .Values.global.podSecurity.runAsGroup }}
capabilities:
  drop:
    - ALL
seccompProfile:
  type: RuntimeDefault
{{- end -}}

{{- define "rocketmq.secretVolume" -}}
{{- if .Values.global.secretRefs.existingSecret -}}
secret:
  secretName: {{ .Values.global.secretRefs.existingSecret | quote }}
  optional: false
  defaultMode: 0440
{{- else -}}
csi:
  driver: secrets-store.csi.k8s.io
  readOnly: true
  volumeAttributes:
    secretProviderClass: {{ required "global.secretRefs.secretProviderClassName is required" .Values.global.secretRefs.secretProviderClassName | quote }}
{{- end -}}
{{- end -}}

{{- define "rocketmq.topology" -}}
{{- if and (eq .root.Values.deploymentProfile "production-controller-ha") (gt (int (index .root.Values.services .service).replicas) 1) }}
affinity:
  podAntiAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:
      - topologyKey: kubernetes.io/hostname
        labelSelector:
          matchLabels:
{{ include "rocketmq.selectorLabels" . | indent 12 }}
topologySpreadConstraints:
  - maxSkew: 1
    topologyKey: kubernetes.io/hostname
    whenUnsatisfiable: DoNotSchedule
    labelSelector:
      matchLabels:
{{ include "rocketmq.selectorLabels" . | indent 8 }}
{{- if eq .service "controller" }}
  - maxSkew: 1
    topologyKey: topology.kubernetes.io/zone
    whenUnsatisfiable: ScheduleAnyway
    labelSelector:
      matchLabels:
{{ include "rocketmq.selectorLabels" . | indent 8 }}
{{- end }}
{{- end }}
{{- end -}}
