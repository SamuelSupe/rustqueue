{{- define "rustqueue.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "rustqueue.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name (include "rustqueue.name" .) | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{- define "rustqueue.labels" -}}
app.kubernetes.io/name: {{ include "rustqueue.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | quote }}
{{- end }}

{{- define "rustqueue.operatorServiceAccount" -}}
{{- printf "%s-operator" (include "rustqueue.fullname" .) }}
{{- end }}
