{{/*
Common naming and label helpers for hyperscale-gate.
*/}}

{{- define "hyperscale-gate.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "hyperscale-gate.fullname" -}}
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

{{- define "hyperscale-gate.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Standard labels applied to every resource the chart renders.
*/}}
{{- define "hyperscale-gate.labels" -}}
helm.sh/chart: {{ include "hyperscale-gate.chart" . }}
{{ include "hyperscale-gate.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: hyperscale
hyperscale.io/role: gate
hyperscale.io/cluster-id: {{ .Values.cluster.id | quote }}
{{- end -}}

{{/*
Selector labels — used by Service / StatefulSet selectors. MUST be
stable across upgrades (these are immutable in StatefulSet selector).
*/}}
{{- define "hyperscale-gate.selectorLabels" -}}
app.kubernetes.io/name: {{ include "hyperscale-gate.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "hyperscale-gate.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "hyperscale-gate.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "hyperscale-gate.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}

{{/*
Headless Service FQDN — the AD-52 §2 dns:// locator target. StatefulSet
pods get DNS records at <pod>.<headlessService>.<namespace>.svc.<clusterDomain>.
*/}}
{{- define "hyperscale-gate.headlessServiceName" -}}
{{- printf "%s-headless" (include "hyperscale-gate.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "hyperscale-gate.adminServiceName" -}}
{{- printf "%s-admin" (include "hyperscale-gate.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "hyperscale-gate.headlessServiceFQDN" -}}
{{- printf "%s.%s.svc.%s" (include "hyperscale-gate.headlessServiceName" .) .Release.Namespace .Values.seeds.clusterDomain -}}
{{- end -}}

{{- define "hyperscale-gate.podFQDN" -}}
{{- $podOrdinal := .ordinal -}}
{{- $ctx := .ctx -}}
{{- printf "%s-%d.%s" (include "hyperscale-gate.fullname" $ctx) $podOrdinal (include "hyperscale-gate.headlessServiceFQDN" $ctx) -}}
{{- end -}}

{{/*
Render the AD-52 §4 --initial-members value.
   node_id_0@dns://<pod-0>.<headless>.<ns>.svc.<domain>:<port>,
   node_id_1@dns://<pod-1>...,
   ...
The node_id values here are operator-assigned founding-set identifiers
(AD-52 §4: "operator-assigned founding identifiers, not the uuid4()
runtime IDs"). We derive them deterministically from the pod ordinal so
every pod renders the identical string.
*/}}
{{- define "hyperscale-gate.initialMembers" -}}
{{- $ctx := . -}}
{{- $port := .Values.listen.port | int -}}
{{- $fqdn := include "hyperscale-gate.headlessServiceFQDN" . -}}
{{- $name := include "hyperscale-gate.fullname" . -}}
{{- $parts := list -}}
{{- range $podOrdinal, $unused := until (int .Values.replicas) -}}
{{- $entry := printf "%s-%d@dns://%s-%d.%s:%d" $name $podOrdinal $name $podOrdinal $fqdn $port -}}
{{- $parts = append $parts $entry -}}
{{- end -}}
{{- join "," $parts -}}
{{- end -}}

{{/*
Render the AD-52 §5 --seeds value based on seeds.mode.
*/}}
{{- define "hyperscale-gate.seeds" -}}
{{- $port := .Values.listen.port | int -}}
{{- if eq .Values.seeds.mode "dns" -}}
{{- printf "dns://%s:%d" (include "hyperscale-gate.headlessServiceFQDN" .) $port -}}
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

{{- define "hyperscale-gate.seedsConfigMapName" -}}
{{- printf "%s-seeds" (include "hyperscale-gate.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
