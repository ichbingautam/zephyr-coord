{{/*
Expand the name of the chart.
*/}}
{{- define "zephyr-coord.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "zephyr-coord.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "zephyr-coord.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "zephyr-coord.labels" -}}
helm.sh/chart: {{ include "zephyr-coord.chart" . }}
{{ include "zephyr-coord.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "zephyr-coord.selectorLabels" -}}
app.kubernetes.io/name: {{ include "zephyr-coord.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app: {{ include "zephyr-coord.fullname" . }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "zephyr-coord.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "zephyr-coord.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Headless service name for peer discovery
*/}}
{{- define "zephyr-coord.headlessServiceName" -}}
{{- printf "%s-headless" (include "zephyr-coord.fullname" .) }}
{{- end }}
