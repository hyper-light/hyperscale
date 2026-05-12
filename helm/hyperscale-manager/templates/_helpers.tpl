{{/*
Common naming and label helpers for hyperscale-manager.
*/}}

{{- define "hyperscale-manager.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "hyperscale-manager.fullname" -}}
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

{{- define "hyperscale-manager.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "hyperscale-manager.labels" -}}
helm.sh/chart: {{ include "hyperscale-manager.chart" . }}
{{ include "hyperscale-manager.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: hyperscale
hyperscale.io/role: manager
hyperscale.io/cluster-id: {{ .Values.cluster.id | quote }}
hyperscale.io/datacenter-id: {{ .Values.cluster.datacenterId | quote }}
{{- end -}}

{{- define "hyperscale-manager.selectorLabels" -}}
app.kubernetes.io/name: {{ include "hyperscale-manager.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "hyperscale-manager.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "hyperscale-manager.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "hyperscale-manager.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}

{{- define "hyperscale-manager.headlessServiceName" -}}
{{- printf "%s-headless" (include "hyperscale-manager.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "hyperscale-manager.adminServiceName" -}}
{{- printf "%s-admin" (include "hyperscale-manager.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "hyperscale-manager.headlessServiceFQDN" -}}
{{- printf "%s.%s.svc.%s" (include "hyperscale-manager.headlessServiceName" .) .Release.Namespace .Values.seeds.clusterDomain -}}
{{- end -}}

{{- define "hyperscale-manager.initialMembers" -}}
{{- $ctx := . -}}
{{- $port := .Values.listen.port | int -}}
{{- $fqdn := include "hyperscale-manager.headlessServiceFQDN" . -}}
{{- $name := include "hyperscale-manager.fullname" . -}}
{{- $parts := list -}}
{{- range $podOrdinal, $unused := until (int .Values.replicas) -}}
{{- $entry := printf "%s-%d@dns://%s-%d.%s:%d" $name $podOrdinal $name $podOrdinal $fqdn $port -}}
{{- $parts = append $parts $entry -}}
{{- end -}}
{{- join "," $parts -}}
{{- end -}}

{{- define "hyperscale-manager.seeds" -}}
{{- $port := .Values.listen.port | int -}}
{{- if eq .Values.seeds.mode "dns" -}}
{{- printf "dns://%s:%d" (include "hyperscale-manager.headlessServiceFQDN" .) $port -}}
{{- else if eq .Values.seeds.mode "tcp" -}}
{{- $entries := list -}}
{{- range .Values.seeds.entries -}}
{{- $entries = append $entries (printf "tcp://%s" .) -}}
{{- end -}}
{{- join "," $entries -}}
{{- else if eq .Values.seeds.mode "file" -}}
{{- printf "file:///etc/hyperscale/seeds/seeds.txt" -}}
{{- else -}}
{{- fail (printf "seeds.mode must be one of dns|tcp|file, got %q" .Values.seeds.mode) -}}
{{- end -}}
{{- end -}}

{{/*
AD-52 §17 federation — gate seed locators. Comma-separated list of
already-formatted AD-52 §2 URIs supplied by the operator. Empty list →
this manager cluster runs without a gate tier above it.
*/}}
{{- define "hyperscale-manager.gateSeeds" -}}
{{- join "," .Values.cluster.gateSeeds -}}
{{- end -}}

{{- define "hyperscale-manager.seedsConfigMapName" -}}
{{- printf "%s-seeds" (include "hyperscale-manager.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
