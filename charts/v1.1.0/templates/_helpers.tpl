{{/*
Expand the name of the chart.
*/}}
{{- define "ack-kube-queue.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "ack-kube-queue.fullname" -}}
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
{{- define "ack-kube-queue.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "ack-kube-queue.labels" -}}
helm.sh/chart: {{ include "ack-kube-queue.chart" . }}
{{ include "ack-kube-queue.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "ack-kube-queue.selectorLabels" -}}
app.kubernetes.io/name: {{ include "ack-kube-queue.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "ack-kube-queue.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "ack-kube-queue.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "ack-kube-queue.imagePrefix" -}}
{{- if or (eq (.Values.global.clusterType | default "Default") "ExternalKubernetes") (eq (.Values.global.clusterProfile | default "Default") "Edge")}}
{{- .Values.global.imagePrefix }}
{{- else if .Values.global.pullImageByVPCNetwork }}
{{- .Values.global.imagePrefix }}
{{- else }}
{{- .Values.global.imagePrefix }}
{{- end }}
{{- end }}
