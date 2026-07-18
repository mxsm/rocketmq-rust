{{/* Common chart identity. */}}
{{- define "rocketmq.partOf" -}}
rocketmq-rust
{{- end -}}

{{- define "rocketmq.serviceName" -}}
rocketmq-{{ .service }}
{{- end -}}

{{- define "rocketmq.labels" -}}
app.kubernetes.io/name: {{ include "rocketmq.serviceName" . }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/part-of: {{ include "rocketmq.partOf" . }}
app.kubernetes.io/managed-by: {{ .root.Release.Service }}
app.kubernetes.io/version: {{ .root.Chart.AppVersion | quote }}
rocketmq.apache.org/service: {{ .service }}
rocketmq.apache.org/architecture-milestone: M11-10
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
{{- $placeholder := "sha256:0000000000000000000000000000000000000000000000000000000000000000" -}}
{{- $digest := required (printf "services.%s.image.digest is required" .service) .digest -}}
{{- if eq $digest $placeholder -}}
{{- fail (printf "services.%s.image.digest is unpublished; inject a verified signed digest" .service) -}}
{{- end -}}
{{ printf "%s/%s@%s" .root.Values.global.imageRegistry .service $digest }}
{{- end -}}

{{- define "rocketmq.controllerServiceIP" -}}
{{- $address := required "three explicit Controller peer Service IPs are required" .address -}}
{{- if hasPrefix "192.0.2." $address -}}
{{- fail "Controller peer Service IP is an unpublished documentation sentinel; inject an unused address from the cluster Service CIDR" -}}
{{- end -}}
{{- $address -}}
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
    whenUnsatisfiable: DoNotSchedule
    labelSelector:
      matchLabels:
{{ include "rocketmq.selectorLabels" . | indent 8 }}
{{- end }}
{{- end -}}
