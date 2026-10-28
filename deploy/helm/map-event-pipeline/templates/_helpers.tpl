{{- define "mep.fullname" -}}
{{- printf "%s" .Release.Name | trunc 50 | trimSuffix "-" -}}
{{- end -}}

{{- define "mep.labels" -}}
app.kubernetes.io/part-of: map-event-pipeline
app.kubernetes.io/instance: {{ .Release.Name }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{- define "mep.secretName" -}}
{{- .Values.postgres.existingSecret | default (printf "%s-postgres" (include "mep.fullname" .)) -}}
{{- end -}}

{{/* Where the apps find their dependencies: in-cluster services, or the configured external ones */}}
{{- define "mep.postgresHost" -}}{{ include "mep.fullname" . }}-postgres{{- end -}}
{{- define "mep.kafkaHost" -}}{{ include "mep.fullname" . }}-kafka{{- end -}}
{{- define "mep.postgresUrl" -}}
{{- if .Values.infra.enabled -}}
jdbc:postgresql://{{ include "mep.postgresHost" . }}:5432/{{ .Values.postgres.db }}
{{- else -}}
{{ required "postgres.url is required when infra.enabled is false" .Values.postgres.url }}
{{- end -}}
{{- end -}}
{{- define "mep.redisHost" -}}
{{- if .Values.infra.enabled -}}{{ include "mep.fullname" . }}-redis{{- else -}}{{ required "redis.host is required when infra.enabled is false" .Values.redis.host }}{{- end -}}
{{- end -}}
{{- define "mep.kafkaServers" -}}
{{- if .Values.infra.enabled -}}{{ include "mep.kafkaHost" . }}:9092{{- else -}}{{ required "kafka.bootstrapServers is required when infra.enabled is false" .Values.kafka.bootstrapServers }}{{- end -}}
{{- end -}}

{{/* Init containers: wait for Postgres and Kafka to accept connections */}}
{{- define "mep.waitForDeps" -}}
{{- if .Values.infra.enabled }}
initContainers:
  {{- range $name, $target := dict "postgres" (printf "%s 5432" (include "mep.postgresHost" .)) "kafka" (printf "%s 9092" (include "mep.kafkaHost" .)) }}
  - name: wait-for-{{ $name }}
    image: {{ $.Values.waitImage }}
    command: ["sh", "-c", "until nc -z {{ $target }}; do echo waiting for {{ $name }}; sleep 2; done"]
  {{- end }}
{{- end }}
{{- end -}}
