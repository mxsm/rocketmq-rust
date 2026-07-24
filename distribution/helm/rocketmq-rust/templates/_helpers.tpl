{{/* Common chart identity. */}}
{{- define "rocketmq.partOf" -}}
rocketmq-rust
{{- end -}}

{{- define "rocketmq.serviceName" -}}
rocketmq-{{ .service }}
{{- end -}}

{{/* Fail early when cross-field topology invariants cannot be expressed by JSON Schema. */}}
{{- define "rocketmq.validateValues" -}}
{{- $profile := .Values.deploymentProfile -}}
{{- if eq $profile "production-controller-ha" -}}
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
{{- end -}}

{{- define "rocketmq.labels" -}}
app.kubernetes.io/name: {{ include "rocketmq.serviceName" . }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/part-of: {{ include "rocketmq.partOf" . }}
app.kubernetes.io/managed-by: {{ .root.Release.Service }}
app.kubernetes.io/version: {{ .root.Chart.AppVersion | quote }}
rocketmq.apache.org/service: {{ .service }}
rocketmq.apache.org/architecture-milestone: P0-04
{{- end -}}

{{/* Shared process lifecycle contract. The health port is kubelet-only and is not exposed by Services. */}}
{{- define "rocketmq.lifecycleEnv" -}}
- {name: ROCKETMQ_HEALTH_BIND_ADDR, value: "0.0.0.0:8088"}
- {name: ROCKETMQ_SHUTDOWN_TIMEOUT_SECONDS, value: "45"}
- {name: ROCKETMQ_LIVENESS_STALE_SECONDS, value: "30"}
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
rocketmq.apache.org/service: {{ .service }}
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
{{- $addresses := list -}}
{{- range $ordinal := until (int .Values.services.namesrv.replicas) -}}
  {{- $addresses = append $addresses (printf "rocketmq-namesrv-%d.rocketmq-namesrv-headless.%s.svc.cluster.local:9876" $ordinal $.Release.Namespace) -}}
{{- end -}}
{{ join ";" $addresses }}
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
