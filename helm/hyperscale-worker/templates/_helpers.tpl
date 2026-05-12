{{/*
Common naming and label helpers for hyperscale-worker.
*/}}

{{- define "hyperscale-worker.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "hyperscale-worker.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "hyperscale-worker.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "hyperscale-worker.labels" -}}
helm.sh/chart: {{ include "hyperscale-worker.chart" . }}
{{ include "hyperscale-worker.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: hyperscale
hyperscale.io/role: worker
hyperscale.io/cluster-id: {{ .Values.cluster.id | quote }}
hyperscale.io/datacenter-id: {{ .Values.cluster.datacenterId | quote }}
{{- end -}}

{{- define "hyperscale-worker.selectorLabels" -}}
app.kubernetes.io/name: {{ include "hyperscale-worker.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "hyperscale-worker.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "hyperscale-worker.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "hyperscale-worker.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}

{{- define "hyperscale-worker.metricsServiceName" -}}
{{- printf "%s-metrics" (include "hyperscale-worker.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
AD-52 §12 manager-seeds. Required; the chart fails the install if empty
to prevent workers from being launched with no way to find their owner
manager — a deployment that would never come Ready.
*/}}
{{- define "hyperscale-worker.managerSeeds" -}}
{{- if eq (len .Values.cluster.managerSeeds) 0 -}}
{{- fail "cluster.managerSeeds is required (AD-52 §12). Provide at least one locator URI (e.g., dns://hyperscale-manager-headless.<ns>.svc.cluster.local:8080)." -}}
{{- end -}}
{{- join "," .Values.cluster.managerSeeds -}}
{{- end -}}
