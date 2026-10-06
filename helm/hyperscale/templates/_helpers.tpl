{{/*
Expand the name of the chart.
*/}}
{{- define "hyperscale.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "hyperscale.fullname" -}}
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
{{- define "hyperscale.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "hyperscale.labels" -}}
helm.sh/chart: {{ include "hyperscale.chart" . }}
{{ include "hyperscale.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "hyperscale.selectorLabels" -}}
app.kubernetes.io/name: {{ include "hyperscale.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Selector labels of one role, optionally in one datacenter:
(dict "root" $ "component" "manager" "datacenter" "dc-a")
*/}}
{{- define "hyperscale.componentSelectorLabels" -}}
{{ include "hyperscale.selectorLabels" .root }}
app.kubernetes.io/component: {{ .component }}
{{- if .datacenter }}
hyperscale.io/datacenter: {{ .datacenter }}
{{- end }}
{{- end }}

{{/*
Names of each role's workload and headless Service.
*/}}
{{- define "hyperscale.gate.name" -}}
{{- printf "%s-gate" (include "hyperscale.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "hyperscale.manager.name" -}}
{{- printf "%s-manager-%s" (include "hyperscale.fullname" .root) .datacenter | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "hyperscale.worker.name" -}}
{{- printf "%s-worker-%s" (include "hyperscale.fullname" .root) .datacenter | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
The DNS name of StatefulSet pod `ordinal` behind its headless Service:
(dict "root" $ "name" <statefulset/service name> "ordinal" 0)
*/}}
{{- define "hyperscale.podHost" -}}
{{- printf "%s-%d.%s.%s.svc.%s" .name (int .ordinal) .name .root.Release.Namespace .root.Values.clusterDomain }}
{{- end }}

{{/*
Every pod address of a StatefulSet as space-separated host:port values:
(dict "root" $ "name" <name> "replicas" 3 "port" 8231)
*/}}
{{- define "hyperscale.podAddresses" -}}
{{- $addresses := list }}
{{- range $ordinal := until (int .replicas) }}
{{- $host := include "hyperscale.podHost" (dict "root" $.root "name" $.name "ordinal" $ordinal) }}
{{- $addresses = append $addresses (printf "%s:%d" $host (int $.port)) }}
{{- end }}
{{- join " " $addresses }}
{{- end }}

{{/*
The Secret holding the cluster auth secret.
*/}}
{{- define "hyperscale.secretName" -}}
{{- if .Values.clusterSecret.existingSecret }}
{{- .Values.clusterSecret.existingSecret }}
{{- else }}
{{- printf "%s-auth" (include "hyperscale.fullname" .) }}
{{- end }}
{{- end }}

{{- define "hyperscale.image" -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) }}
{{- end }}
