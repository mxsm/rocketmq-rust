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
