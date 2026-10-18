package main

// The describe view of one object (ObjectInfo): what the object page shows
// and what RCA and ✦ AI analysis put into their prompts.
//
// It shows only what is set: a row is added only when its value is not
// empty, so absent fields add nothing. Lists inside one value are joined
// with ", ". Secret and ConfigMap values and literal env values are never
// shown, only names and references. Small models need the decisive setting
// in front of them (a selector, a toleration, a probe path, a missing
// ConfigMap in envFrom), so the pod spec is shown in some depth.

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// describeBody writes an ObjectInfo as text: age, labels, annotations, the
// sections and the conditions. RCA's describe and ✦ AI analysis both use it.
func describeBody(info ObjectInfo) string {
	var sb strings.Builder
	if info.Age != "" {
		fmt.Fprintf(&sb, "Age: %s\n", info.Age)
	}
	if len(info.Labels) > 0 {
		sb.WriteString("Labels:")
		writeLabels(&sb, info.Labels)
		sb.WriteString("\n")
	}
	if len(info.Annotations) > 0 {
		sb.WriteString("Annotations:")
		writeLabels(&sb, info.Annotations)
		sb.WriteString("\n")
	}
	for _, sec := range info.Sections {
		fmt.Fprintf(&sb, "%s:\n", sec.Title)
		for _, row := range sec.Rows {
			fmt.Fprintf(&sb, "  %s: %s\n", row.Key, row.Value)
		}
	}
	if len(info.Conditions) > 0 {
		sb.WriteString("Conditions:\n")
		for _, c := range info.Conditions {
			if c.Reason != "" || c.Message != "" {
				fmt.Fprintf(&sb, "  %s: %s (%s: %s)\n", c.Type, c.Status, c.Reason, c.Message)
			} else {
				fmt.Fprintf(&sb, "  %s: %s\n", c.Type, c.Status)
			}
		}
	}
	return sb.String()
}

// writeLabels writes " key=value" for each label, in key order, so the same
// object always gives the same prompt.
func writeLabels(sb *strings.Builder, labels map[string]string) {
	for _, k := range sortedKeys(labels) {
		fmt.Fprintf(sb, " %s=%s", k, labels[k])
	}
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// describeObject fills info (kind, name and namespace set) from the raw
// object: age at now, labels, annotations (without last-applied-configuration
// and values over 200 characters), sections and conditions. endpoints is a
// Service's Endpoints row.
func describeObject(info ObjectInfo, raw map[string]interface{}, endpoints string, now time.Time) ObjectInfo {
	meta := nmMap(raw, "metadata")
	info.Labels = toStrMap(meta["labels"])
	if ct := nmStr(meta, "creationTimestamp"); ct != "" {
		if t, err := time.Parse(time.RFC3339, ct); err == nil {
			info.Age = formatDuration(now.Sub(t))
		}
	}
	if ann := toStrMap(meta["annotations"]); ann != nil {
		filtered := make(map[string]string)
		for k, v := range ann {
			if strings.Contains(k, "last-applied-configuration") || len([]rune(v)) > 200 {
				continue
			}
			filtered[k] = v
		}
		if len(filtered) > 0 {
			info.Annotations = filtered
		}
	}
	info.Sections, info.Conditions = describeSections(info.Kind, raw, endpoints)
	return info
}

// describeSections builds the sections and conditions of an object of kind
// from its raw (unstructured) form. endpoints is a Service's Endpoints row
// (see formatEndpoints); other kinds ignore it.
func describeSections(kind string, raw map[string]interface{}, endpoints string) ([]InfoSection, []InfoCondition) {
	spec := nmMap(raw, "spec")
	status := nmMap(raw, "status")
	switch strings.ToLower(kind) {
	case "pod":
		return infoForPod(spec, status), extractConditions(status)
	case "deployment":
		return infoForDeployment(spec, status), extractConditions(status)
	case "replicaset":
		return infoForReplicaSet(spec, status), extractConditions(status)
	case "statefulset":
		return infoForStatefulSet(spec, status), extractConditions(status)
	case "daemonset":
		return infoForDaemonSet(spec, status), extractConditions(status)
	case "job":
		return infoForJob(spec, status), extractConditions(status)
	case "cronjob":
		return infoForCronJob(spec, status), nil
	case "service":
		return infoForService(spec, status, endpoints), nil
	case "persistentvolumeclaim":
		return infoForPVC(spec, status), extractConditions(status)
	case "persistentvolume":
		return infoForPV(spec, status), nil
	case "node":
		return infoForNode(spec, status), extractConditions(status)
	case "horizontalpodautoscaler":
		return infoForHPA(spec, status), extractConditions(status)
	case "ingress":
		return infoForIngress(spec, status), nil
	case "configmap":
		return infoForConfigMap(raw), nil
	case "secret":
		return infoForSecret(raw), nil
	}
	// ServiceAccounts and the like have no sections; custom resources
	// (cert-manager Certificates, …) have status.conditions.
	return nil, extractConditions(status)
}

// section appends a section unless it has no rows.
func section(sections []InfoSection, title string, rows []InfoRow) []InfoSection {
	if len(rows) == 0 {
		return sections
	}
	return append(sections, InfoSection{title, rows})
}

func nmBool(m map[string]interface{}, key string) bool {
	b, _ := m[key].(bool)
	return b
}

// nmIntOr is nmInt, or dflt when the field isn't set.
func nmIntOr(m map[string]interface{}, key string, dflt int64) int64 {
	if m == nil || m[key] == nil {
		return dflt
	}
	return nmInt(m, key)
}

func nmStrings(m map[string]interface{}, key string) []string {
	var out []string
	for _, v := range nmSlice(m, key) {
		out = append(out, fmt.Sprintf("%v", v))
	}
	return out
}

// joinKV is "k=v, k2=v2" in key order.
func joinKV(m map[string]string) string {
	var parts []string
	for _, k := range sortedKeys(m) {
		parts = append(parts, k+"="+m[k])
	}
	return strings.Join(parts, ", ")
}

func countStr(m map[string]interface{}, key string) string {
	return fmt.Sprintf("%d", nmInt(m, key))
}

// ── Selectors ──────────────────────────────────────────────────────────────

// formatRequirement is one selector expression: "zone in (a, b)",
// "tier notin (x)", "gpu" (Exists), "!gpu" (DoesNotExist), "cores>4".
func formatRequirement(e map[string]interface{}) string {
	key, vals := nmStr(e, "key"), strings.Join(nmStrings(e, "values"), ", ")
	switch nmStr(e, "operator") {
	case "In":
		return key + " in (" + vals + ")"
	case "NotIn":
		return key + " notin (" + vals + ")"
	case "Exists":
		return key
	case "DoesNotExist":
		return "!" + key
	case "Gt":
		return key + ">" + vals
	case "Lt":
		return key + "<" + vals
	}
	return key + " " + nmStr(e, "operator") + " (" + vals + ")"
}

// formatLabelSelector is a workload's selector: matchLabels as "k=v" in key
// order, then matchExpressions.
func formatLabelSelector(sel map[string]interface{}) string {
	if sel == nil {
		return ""
	}
	var parts []string
	if ml := joinKV(toStrMap(sel["matchLabels"])); ml != "" {
		parts = append(parts, ml)
	}
	for _, raw := range nmSlice(sel, "matchExpressions") {
		if e, _ := raw.(map[string]interface{}); e != nil {
			parts = append(parts, formatRequirement(e))
		}
	}
	return strings.Join(parts, ", ")
}

// formatNodeSelectorTerms is a node selector: the expressions of one term
// joined with ", ", terms (any of which may match) with " or ".
func formatNodeSelectorTerms(terms []interface{}) string {
	var out []string
	for _, raw := range terms {
		t, _ := raw.(map[string]interface{})
		var exprs []string
		for _, f := range []string{"matchExpressions", "matchFields"} {
			for _, er := range nmSlice(t, f) {
				if e, _ := er.(map[string]interface{}); e != nil {
					exprs = append(exprs, formatRequirement(e))
				}
			}
		}
		if len(exprs) > 0 {
			out = append(out, strings.Join(exprs, ", "))
		}
	}
	return strings.Join(out, " or ")
}

// ── Pod spec ───────────────────────────────────────────────────────────────

// isDefaultToleration reports the not-ready / unreachable NoExecute 300s
// tolerations the DefaultTolerationSeconds admission plugin adds to every Pod.
func isDefaultToleration(t map[string]interface{}) bool {
	k := nmStr(t, "key")
	return (k == "node.kubernetes.io/not-ready" || k == "node.kubernetes.io/unreachable") &&
		nmStr(t, "operator") == "Exists" && nmStr(t, "effect") == "NoExecute" &&
		nmIntOr(t, "tolerationSeconds", -1) == 300
}

// formatToleration is "key=value:Effect"; operator Exists gives "key:Effect",
// no effect "key=value", an empty key ":Effect", plus " for Ns" when
// tolerationSeconds is set. A toleration of everything (empty key, Exists,
// no effect) is "op=Exists", as kubectl shows it.
func formatToleration(t map[string]interface{}) string {
	key, effect := nmStr(t, "key"), nmStr(t, "effect")
	s := key
	if nmStr(t, "operator") != "Exists" && nmStr(t, "value") != "" {
		s += "=" + nmStr(t, "value")
	}
	if effect != "" {
		s += ":" + effect
	}
	if s == "" {
		s = "op=Exists"
	}
	if t["tolerationSeconds"] != nil {
		s += fmt.Sprintf(" for %ds", nmInt(t, "tolerationSeconds"))
	}
	return s
}

func formatTolerations(ts []interface{}) string {
	var parts []string
	for _, raw := range ts {
		if t, _ := raw.(map[string]interface{}); t != nil && !isDefaultToleration(t) {
			parts = append(parts, formatToleration(t))
		}
	}
	return strings.Join(parts, ", ")
}

// affinityTypes names the kinds of affinity a pod spec sets.
func affinityTypes(a map[string]interface{}) string {
	var types []string
	for _, t := range []string{"nodeAffinity", "podAffinity", "podAntiAffinity"} {
		if len(nmMap(a, t)) > 0 {
			types = append(types, t)
		}
	}
	return strings.Join(types, ", ")
}

// schedulingRows are the pod spec's rows shared by a Pod's Runtime section
// and a workload's Pod Template section.
func schedulingRows(rows []InfoRow, spec map[string]interface{}) []InfoRow {
	rows = rowIf(rows, "Service Account", nmStr(spec, "serviceAccountName"))
	if rp := nmStr(spec, "restartPolicy"); rp != "Always" {
		rows = rowIf(rows, "Restart Policy", rp)
	}
	rows = rowIf(rows, "Node Selector", joinKV(toStrMap(spec["nodeSelector"])))
	rows = rowIf(rows, "Tolerations", formatTolerations(nmSlice(spec, "tolerations")))
	rows = rowIf(rows, "Affinity", affinityTypes(nmMap(spec, "affinity")))
	rows = rowIf(rows, "Priority Class", nmStr(spec, "priorityClassName"))
	return rows
}

// joinWords joins a command or its args with spaces, on one line.
func joinWords(words []string) string {
	return strings.Join(strings.Fields(strings.Join(words, " ")), " ")
}

func seconds(n int64) string { return (time.Duration(n) * time.Second).String() }

// formatProbe is a probe as kubectl describe shows it, e.g.
// "http-get http://:8080/healthz delay=0s timeout=1s period=10s #success=1 #failure=3".
func formatProbe(p map[string]interface{}) string {
	if p == nil {
		return ""
	}
	var s string
	if h := nmMap(p, "httpGet"); h != nil {
		scheme := strings.ToLower(nmStr(h, "scheme"))
		if scheme == "" {
			scheme = "http"
		}
		s = fmt.Sprintf("http-get %s://%s:%s%s", scheme, nmStr(h, "host"), nmStr(h, "port"), nmStr(h, "path"))
	} else if t := nmMap(p, "tcpSocket"); t != nil {
		s = fmt.Sprintf("tcp-socket %s:%s", nmStr(t, "host"), nmStr(t, "port"))
	} else if e := nmMap(p, "exec"); e != nil {
		s = "exec [" + strings.Join(nmStrings(e, "command"), " ") + "]"
	} else if g := nmMap(p, "grpc"); g != nil {
		s = "grpc :" + nmStr(g, "port")
	} else {
		return ""
	}
	return s + fmt.Sprintf(" delay=%s timeout=%s period=%s #success=%d #failure=%d",
		seconds(nmIntOr(p, "initialDelaySeconds", 0)), seconds(nmIntOr(p, "timeoutSeconds", 1)),
		seconds(nmIntOr(p, "periodSeconds", 10)), nmIntOr(p, "successThreshold", 1), nmIntOr(p, "failureThreshold", 3))
}

func optional(m map[string]interface{}) string {
	if nmBool(m, "optional") {
		return " (optional)"
	}
	return ""
}

// formatEnvFrom names the ConfigMaps and Secrets a container loads whole.
func formatEnvFrom(c map[string]interface{}) string {
	var parts []string
	for _, raw := range nmSlice(c, "envFrom") {
		ef, _ := raw.(map[string]interface{})
		if r := nmMap(ef, "configMapRef"); r != nil {
			parts = append(parts, "configmap/"+nmStr(r, "name")+optional(r))
		}
		if r := nmMap(ef, "secretRef"); r != nil {
			parts = append(parts, "secret/"+nmStr(r, "name")+optional(r))
		}
	}
	return strings.Join(parts, ", ")
}

// formatEnvRefs lists the env vars that reference something
// (NAME=secret/name:key, NAME=configmap/name:key, NAME=field:path,
// NAME=resource:limits.memory). Literal values are never shown.
func formatEnvRefs(c map[string]interface{}) string {
	var parts []string
	for _, raw := range nmSlice(c, "env") {
		e, _ := raw.(map[string]interface{})
		vf := nmMap(e, "valueFrom")
		if vf == nil {
			continue
		}
		name := nmStr(e, "name")
		switch {
		case nmMap(vf, "secretKeyRef") != nil:
			r := nmMap(vf, "secretKeyRef")
			parts = append(parts, name+"=secret/"+nmStr(r, "name")+":"+nmStr(r, "key")+optional(r))
		case nmMap(vf, "configMapKeyRef") != nil:
			r := nmMap(vf, "configMapKeyRef")
			parts = append(parts, name+"=configmap/"+nmStr(r, "name")+":"+nmStr(r, "key")+optional(r))
		case nmMap(vf, "fieldRef") != nil:
			parts = append(parts, name+"=field:"+nmStr(nmMap(vf, "fieldRef"), "fieldPath"))
		case nmMap(vf, "resourceFieldRef") != nil:
			parts = append(parts, name+"=resource:"+nmStr(nmMap(vf, "resourceFieldRef"), "resource"))
		}
	}
	return strings.Join(parts, ", ")
}

// isAPIAccessVolume is the projected service-account token volume every Pod
// gets; it says nothing about the workload.
func isAPIAccessVolume(name string) bool { return strings.HasPrefix(name, "kube-api-access-") }

func formatMounts(c map[string]interface{}) string {
	var parts []string
	for _, raw := range nmSlice(c, "volumeMounts") {
		m, _ := raw.(map[string]interface{})
		if m == nil || isAPIAccessVolume(nmStr(m, "name")) {
			continue
		}
		s := nmStr(m, "mountPath") + "=" + nmStr(m, "name")
		if nmBool(m, "readOnly") {
			s += " (ro)"
		}
		parts = append(parts, s)
	}
	return strings.Join(parts, ", ")
}

// formatSecurity lists the security settings that are set, the Pod's
// runAsNonRoot / runAsUser / fsGroup included (the container's win).
func formatSecurity(c, podSpec map[string]interface{}) string {
	sc, psc := nmMap(c, "securityContext"), nmMap(podSpec, "securityContext")
	var parts []string
	add := func(name, v string) {
		if v != "" {
			parts = append(parts, name+"="+v)
		}
	}
	either := func(key string) string {
		if v := nmStr(sc, key); v != "" {
			return v
		}
		return nmStr(psc, key)
	}
	add("runAsNonRoot", either("runAsNonRoot"))
	add("runAsUser", either("runAsUser"))
	add("fsGroup", nmStr(psc, "fsGroup"))
	add("readOnlyRootFilesystem", nmStr(sc, "readOnlyRootFilesystem"))
	add("allowPrivilegeEscalation", nmStr(sc, "allowPrivilegeEscalation"))
	add("privileged", nmStr(sc, "privileged"))
	caps := nmMap(sc, "capabilities")
	add("capabilities.drop", strings.Join(nmStrings(caps, "drop"), ","))
	add("capabilities.add", strings.Join(nmStrings(caps, "add"), ","))
	return strings.Join(parts, " ")
}

// containerRows are the rows of one container's section.
func containerRows(c, podSpec map[string]interface{}) []InfoRow {
	var rows []InfoRow
	rows = rowIf(rows, "Image", nmStr(c, "image"))
	res := nmMap(c, "resources")
	req, lim := nmMap(res, "requests"), nmMap(res, "limits")
	rows = rowIf(rows, "CPU Request", nmStr(req, "cpu"))
	rows = rowIf(rows, "Memory Request", nmStr(req, "memory"))
	rows = rowIf(rows, "CPU Limit", nmStr(lim, "cpu"))
	rows = rowIf(rows, "Memory Limit", nmStr(lim, "memory"))
	rows = rowIf(rows, "Command", truncate(joinWords(nmStrings(c, "command")), 200))
	rows = rowIf(rows, "Args", truncate(joinWords(nmStrings(c, "args")), 200))
	var ports []string
	for _, raw := range nmSlice(c, "ports") {
		p, _ := raw.(map[string]interface{})
		proto := nmStr(p, "protocol")
		if proto == "" {
			proto = "TCP"
		}
		ports = append(ports, nmStr(p, "containerPort")+"/"+proto)
	}
	rows = rowIf(rows, "Ports", strings.Join(ports, ", "))
	rows = rowIf(rows, "Liveness", formatProbe(nmMap(c, "livenessProbe")))
	rows = rowIf(rows, "Readiness", formatProbe(nmMap(c, "readinessProbe")))
	rows = rowIf(rows, "Startup", formatProbe(nmMap(c, "startupProbe")))
	rows = rowIf(rows, "Env From", formatEnvFrom(c))
	rows = rowIf(rows, "Env Refs", formatEnvRefs(c))
	rows = rowIf(rows, "Mounts", formatMounts(c))
	rows = rowIf(rows, "Security", formatSecurity(c, podSpec))
	return rows
}

// containerSections is one section per container, init containers first,
// titled "<prefix>Init Container: <name>" / "<prefix>Container: <name>".
func containerSections(sections []InfoSection, spec map[string]interface{}, prefix string) []InfoSection {
	for _, group := range []struct{ field, title string }{
		{"initContainers", "Init Container: "}, {"containers", "Container: "},
	} {
		for _, raw := range nmSlice(spec, group.field) {
			if c, _ := raw.(map[string]interface{}); c != nil {
				sections = append(sections, InfoSection{prefix + group.title + nmStr(c, "name"), containerRows(c, spec)})
			}
		}
	}
	return sections
}

// volumeSource says where a pod volume's data comes from.
func volumeSource(v map[string]interface{}) string {
	switch {
	case nmMap(v, "persistentVolumeClaim") != nil:
		return "pvc/" + nmStr(nmMap(v, "persistentVolumeClaim"), "claimName")
	case nmMap(v, "configMap") != nil:
		r := nmMap(v, "configMap")
		return "configmap/" + nmStr(r, "name") + optional(r)
	case nmMap(v, "secret") != nil:
		r := nmMap(v, "secret")
		return "secret/" + nmStr(r, "secretName") + optional(r)
	case v["emptyDir"] != nil:
		ed := nmMap(v, "emptyDir")
		s := "emptyDir"
		if m := nmStr(ed, "medium"); m != "" {
			s += " medium=" + m
		}
		if l := nmStr(ed, "sizeLimit"); l != "" {
			s += " sizeLimit=" + l
		}
		return s
	case nmMap(v, "hostPath") != nil:
		return "hostPath " + nmStr(nmMap(v, "hostPath"), "path")
	case nmMap(v, "csi") != nil:
		return "csi " + nmStr(nmMap(v, "csi"), "driver")
	case nmMap(v, "nfs") != nil:
		n := nmMap(v, "nfs")
		return "nfs " + nmStr(n, "server") + ":" + nmStr(n, "path")
	}
	for _, k := range sortedKeys(v) {
		if k != "name" {
			return k // ephemeral, projected, downwardAPI, …
		}
	}
	return ""
}

func volumeRows(spec map[string]interface{}) []InfoRow {
	var rows []InfoRow
	for _, raw := range nmSlice(spec, "volumes") {
		v, _ := raw.(map[string]interface{})
		if v == nil || isAPIAccessVolume(nmStr(v, "name")) {
			continue
		}
		rows = rowIf(rows, nmStr(v, "name"), volumeSource(v))
	}
	return rows
}

// podTemplateSections is a workload's pod template: Pod Template, the
// template's containers and Template Volumes.
func podTemplateSections(sections []InfoSection, tmpl map[string]interface{}) []InfoSection {
	tspec := nmMap(tmpl, "spec")
	var rows []InfoRow
	rows = rowIf(rows, "Labels", joinKV(toStrMap(nmMap(tmpl, "metadata")["labels"])))
	rows = schedulingRows(rows, tspec)
	sections = section(sections, "Pod Template", rows)
	sections = containerSections(sections, tspec, "Template ")
	return section(sections, "Template Volumes", volumeRows(tspec))
}

// ── Per kind ───────────────────────────────────────────────────────────────

func infoForPod(spec, status map[string]interface{}) []InfoSection {
	var rows []InfoRow
	rows = rowIf(rows, "Phase", nmStr(status, "phase"))
	rows = rowIf(rows, "Reason", nmStr(status, "reason"))
	rows = rowIf(rows, "Message", nmStr(status, "message"))
	rows = rowIf(rows, "Pod IP", nmStr(status, "podIP"))
	rows = rowIf(rows, "Node", nmStr(spec, "nodeName"))
	rows = rowIf(rows, "Host IP", nmStr(status, "hostIP"))
	rows = rowIf(rows, "QoS Class", nmStr(status, "qosClass"))
	rows = schedulingRows(rows, spec)
	sections := section(nil, "Runtime", rows)
	sections = containerSections(sections, spec, "")
	sections = section(sections, "Volumes", volumeRows(spec))

	var csRows []InfoRow
	for _, group := range []struct{ field, suffix string }{
		{"initContainerStatuses", " (init)"}, {"containerStatuses", ""},
	} {
		for _, raw := range nmSlice(status, group.field) {
			cs, _ := raw.(map[string]interface{})
			if cs == nil {
				continue
			}
			csRows = append(csRows, InfoRow{
				nmStr(cs, "name") + group.suffix,
				fmt.Sprintf("ready=%v  restarts=%d", nmBool(cs, "ready"), nmInt(cs, "restartCount")),
			})
		}
	}
	return section(sections, "Container Status", csRows)
}

func infoForDeployment(spec, status map[string]interface{}) []InfoSection {
	var rows []InfoRow
	rows = rowIf(rows, "Replicas (desired)", countStr(spec, "replicas"))
	rows = rowIf(rows, "Ready", countStr(status, "readyReplicas"))
	rows = rowIf(rows, "Available", countStr(status, "availableReplicas"))
	rows = rowIf(rows, "Unavailable", countStr(status, "unavailableReplicas"))
	rows = rowIf(rows, "Updated", countStr(status, "updatedReplicas"))
	rows = rowIf(rows, "Strategy", nmStr(nmMap(spec, "strategy"), "type"))
	rows = rowIf(rows, "Selector", formatLabelSelector(nmMap(spec, "selector")))
	return podTemplateSections([]InfoSection{{"Replicas", rows}}, nmMap(spec, "template"))
}

func infoForReplicaSet(spec, status map[string]interface{}) []InfoSection {
	var rows []InfoRow
	rows = rowIf(rows, "Replicas (desired)", countStr(spec, "replicas"))
	rows = rowIf(rows, "Ready", countStr(status, "readyReplicas"))
	rows = rowIf(rows, "Available", countStr(status, "availableReplicas"))
	rows = rowIf(rows, "Selector", formatLabelSelector(nmMap(spec, "selector")))
	return podTemplateSections([]InfoSection{{"Replicas", rows}}, nmMap(spec, "template"))
}

// formatClaimTemplate is "data (fast-ssd, 10Gi, ReadWriteOnce)": the storage
// class (when set), the request and the access modes joined with "/".
func formatClaimTemplate(t map[string]interface{}) string {
	ts := nmMap(t, "spec")
	var parts []string
	if sc := nmStr(ts, "storageClassName"); sc != "" {
		parts = append(parts, sc)
	}
	if size := nmStr(nmMap(nmMap(ts, "resources"), "requests"), "storage"); size != "" {
		parts = append(parts, size)
	}
	if modes := nmStrings(ts, "accessModes"); len(modes) > 0 {
		parts = append(parts, strings.Join(modes, "/"))
	}
	s := nmStr(nmMap(t, "metadata"), "name")
	if len(parts) > 0 {
		s += " (" + strings.Join(parts, ", ") + ")"
	}
	return s
}

func infoForStatefulSet(spec, status map[string]interface{}) []InfoSection {
	var rows []InfoRow
	rows = rowIf(rows, "Replicas (desired)", countStr(spec, "replicas"))
	rows = rowIf(rows, "Ready", countStr(status, "readyReplicas"))
	rows = rowIf(rows, "Current", countStr(status, "currentReplicas"))
	rows = rowIf(rows, "Updated", countStr(status, "updatedReplicas"))
	rows = rowIf(rows, "Service Name", nmStr(spec, "serviceName"))
	rows = rowIf(rows, "Selector", formatLabelSelector(nmMap(spec, "selector")))
	var vcts []string
	for _, raw := range nmSlice(spec, "volumeClaimTemplates") {
		if t, _ := raw.(map[string]interface{}); t != nil {
			vcts = append(vcts, formatClaimTemplate(t))
		}
	}
	rows = rowIf(rows, "Volume Claim Templates", strings.Join(vcts, ", "))
	return podTemplateSections([]InfoSection{{"Replicas", rows}}, nmMap(spec, "template"))
}

func infoForDaemonSet(spec, status map[string]interface{}) []InfoSection {
	var rows []InfoRow
	rows = rowIf(rows, "Desired", countStr(status, "desiredNumberScheduled"))
	rows = rowIf(rows, "Current", countStr(status, "currentNumberScheduled"))
	rows = rowIf(rows, "Ready", countStr(status, "numberReady"))
	rows = rowIf(rows, "Available", countStr(status, "numberAvailable"))
	rows = rowIf(rows, "Unavailable", countStr(status, "numberUnavailable"))
	rows = rowIf(rows, "Selector", formatLabelSelector(nmMap(spec, "selector")))
	return podTemplateSections([]InfoSection{{"Node Schedule", rows}}, nmMap(spec, "template"))
}

func infoForJob(spec, status map[string]interface{}) []InfoSection {
	var rows []InfoRow
	if spec["completions"] != nil {
		rows = rowIf(rows, "Completions", countStr(spec, "completions"))
	}
	rows = rowIf(rows, "Active", countStr(status, "active"))
	rows = rowIf(rows, "Succeeded", countStr(status, "succeeded"))
	rows = rowIf(rows, "Failed", countStr(status, "failed"))
	rows = rowIf(rows, "Start Time", nmStr(status, "startTime"))
	rows = rowIf(rows, "Completion Time", nmStr(status, "completionTime"))
	if spec["backoffLimit"] != nil {
		rows = rowIf(rows, "Backoff Limit", countStr(spec, "backoffLimit"))
	}
	if spec["activeDeadlineSeconds"] != nil {
		rows = rowIf(rows, "Active Deadline", fmt.Sprintf("%ds", nmInt(spec, "activeDeadlineSeconds")))
	}
	return podTemplateSections([]InfoSection{{"Status", rows}}, nmMap(spec, "template"))
}

func infoForCronJob(spec, status map[string]interface{}) []InfoSection {
	var rows []InfoRow
	rows = rowIf(rows, "Schedule", nmStr(spec, "schedule"))
	rows = rowIf(rows, "Suspend", nmStr(spec, "suspend"))
	rows = rowIf(rows, "Last Schedule", nmStr(status, "lastScheduleTime"))
	rows = rowIf(rows, "Last Successful", nmStr(status, "lastSuccessfulTime"))
	rows = rowIf(rows, "Active Jobs", fmt.Sprintf("%d", len(nmSlice(status, "active"))))
	rows = rowIf(rows, "Concurrency Policy", nmStr(spec, "concurrencyPolicy"))
	tmpl := nmMap(nmMap(nmMap(spec, "jobTemplate"), "spec"), "template")
	return podTemplateSections([]InfoSection{{"CronJob", rows}}, tmpl)
}

func infoForService(spec, status map[string]interface{}, endpoints string) []InfoSection {
	var rows []InfoRow
	rows = rowIf(rows, "Type", nmStr(spec, "type"))
	rows = rowIf(rows, "Cluster IP", nmStr(spec, "clusterIP"))
	rows = rowIf(rows, "External IPs", strings.Join(nmStrings(spec, "externalIPs"), ", "))
	rows = rowIf(rows, "Load Balancer", loadBalancerIngress(status))
	var ports []string
	for _, raw := range nmSlice(spec, "ports") {
		p, _ := raw.(map[string]interface{})
		if p == nil {
			continue
		}
		s := fmt.Sprintf("%s→%s/%s", nmStr(p, "port"), nmStr(p, "targetPort"), nmStr(p, "protocol"))
		if n := nmStr(p, "name"); n != "" {
			s = n + ":" + s
		}
		ports = append(ports, s)
	}
	rows = rowIf(rows, "Ports", strings.Join(ports, ", "))
	rows = rowIf(rows, "Session Affinity", nmStr(spec, "sessionAffinity"))
	sel := joinKV(toStrMap(spec["selector"]))
	if sel == "" {
		sel = "none"
	}
	rows = rowIf(rows, "Selector", sel)
	rows = rowIf(rows, "Endpoints", endpoints)
	return []InfoSection{{"Service", rows}}
}

// serviceEndpoints is a Service's Endpoints row, from the EndpointSlices
// labelled with its name; "" (no row) when they can't be listed.
func (c *clusterState) serviceEndpoints(ns, name string) string {
	if c.client == nil {
		return ""
	}
	list, err := c.client.DiscoveryV1().EndpointSlices(ns).List(context.Background(),
		metav1.ListOptions{LabelSelector: discoveryv1.LabelServiceName + "=" + name})
	if err != nil {
		return ""
	}
	return formatEndpoints(list.Items)
}

// maxEndpointAddrs caps the addresses a Service's Endpoints row lists.
const maxEndpointAddrs = 5

// formatEndpoints sums up a Service's EndpointSlices: "2 ready, 1 not
// ready: 10.244.1.5:8080, 10.244.2.7:8080" (ready addresses first, at most
// five), "3 ready: …" when all are ready, "none" when there are none.
func formatEndpoints(slices []discoveryv1.EndpointSlice) string {
	type ep struct {
		addr  string
		ready bool
	}
	var eps []ep
	seen := map[string]bool{}
	for _, s := range slices {
		port := ""
		if len(s.Ports) > 0 && s.Ports[0].Port != nil {
			port = fmt.Sprintf(":%d", *s.Ports[0].Port)
		}
		for _, e := range s.Endpoints {
			if len(e.Addresses) == 0 {
				continue
			}
			addr := e.Addresses[0] + port
			if seen[addr] {
				continue
			}
			seen[addr] = true
			// A nil ready condition means ready (EndpointConditions docs).
			eps = append(eps, ep{addr, e.Conditions.Ready == nil || *e.Conditions.Ready})
		}
	}
	if len(eps) == 0 {
		return "none"
	}
	sort.SliceStable(eps, func(i, j int) bool {
		if eps[i].ready != eps[j].ready {
			return eps[i].ready
		}
		return eps[i].addr < eps[j].addr
	})
	ready := 0
	var addrs []string
	for i, e := range eps {
		if e.ready {
			ready++
		}
		if i < maxEndpointAddrs {
			addrs = append(addrs, e.addr)
		}
	}
	s := fmt.Sprintf("%d ready", ready)
	if n := len(eps) - ready; n > 0 {
		s += fmt.Sprintf(", %d not ready", n)
	}
	return s + ": " + strings.Join(addrs, ", ")
}

func infoForPVC(spec, status map[string]interface{}) []InfoSection {
	var rows []InfoRow
	rows = rowIf(rows, "Status", nmStr(status, "phase"))
	rows = rowIf(rows, "Volume", nmStr(spec, "volumeName"))
	rows = rowIf(rows, "Storage Class", nmStr(spec, "storageClassName"))
	rows = rowIf(rows, "Access Modes", strings.Join(nmStrings(spec, "accessModes"), ", "))
	rows = rowIf(rows, "Requested", nmStr(nmMap(nmMap(spec, "resources"), "requests"), "storage"))
	rows = rowIf(rows, "Capacity", nmStr(nmMap(status, "capacity"), "storage"))
	rows = rowIf(rows, "Volume Mode", nmStr(spec, "volumeMode"))
	return section(nil, "Claim", rows)
}

// pvSource says what backs a PersistentVolume.
func pvSource(spec map[string]interface{}) string {
	switch {
	case nmMap(spec, "nfs") != nil:
		n := nmMap(spec, "nfs")
		return "nfs " + nmStr(n, "server") + ":" + nmStr(n, "path")
	case nmMap(spec, "csi") != nil:
		c := nmMap(spec, "csi")
		return strings.TrimSpace("csi " + nmStr(c, "driver") + " " + nmStr(c, "volumeHandle"))
	case nmMap(spec, "hostPath") != nil:
		return "hostPath " + nmStr(nmMap(spec, "hostPath"), "path")
	case nmMap(spec, "local") != nil:
		return "local " + nmStr(nmMap(spec, "local"), "path")
	}
	notSource := map[string]bool{"accessModes": true, "capacity": true, "claimRef": true, "mountOptions": true,
		"nodeAffinity": true, "persistentVolumeReclaimPolicy": true, "storageClassName": true, "volumeMode": true,
		"volumeAttributesClassName": true}
	for _, k := range sortedKeys(spec) {
		if !notSource[k] {
			return k
		}
	}
	return ""
}

func infoForPV(spec, status map[string]interface{}) []InfoSection {
	var rows []InfoRow
	rows = rowIf(rows, "Status", nmStr(status, "phase"))
	if cr := nmMap(spec, "claimRef"); cr != nil && nmStr(cr, "name") != "" {
		rows = rowIf(rows, "Claim", nmStr(cr, "namespace")+"/"+nmStr(cr, "name"))
	}
	rows = rowIf(rows, "Reclaim Policy", nmStr(spec, "persistentVolumeReclaimPolicy"))
	rows = rowIf(rows, "Storage Class", nmStr(spec, "storageClassName"))
	rows = rowIf(rows, "Access Modes", strings.Join(nmStrings(spec, "accessModes"), ", "))
	rows = rowIf(rows, "Capacity", nmStr(nmMap(spec, "capacity"), "storage"))
	rows = rowIf(rows, "Source", pvSource(spec))
	rows = rowIf(rows, "Node Affinity",
		formatNodeSelectorTerms(nmSlice(nmMap(nmMap(spec, "nodeAffinity"), "required"), "nodeSelectorTerms")))
	rows = rowIf(rows, "Message", nmStr(status, "message"))
	return section(nil, "Volume", rows)
}

func infoForNode(spec, status map[string]interface{}) []InfoSection {
	var rows []InfoRow
	if nmBool(spec, "unschedulable") {
		rows = rowIf(rows, "Unschedulable", "true")
	}
	var taints []string
	for _, raw := range nmSlice(spec, "taints") {
		t, _ := raw.(map[string]interface{})
		if t == nil {
			continue
		}
		s := nmStr(t, "key")
		if v := nmStr(t, "value"); v != "" {
			s += "=" + v
		}
		taints = append(taints, s+":"+nmStr(t, "effect"))
	}
	rows = rowIf(rows, "Taints", strings.Join(taints, ", "))
	rows = rowIf(rows, "Kubelet Version", nmStr(nmMap(status, "nodeInfo"), "kubeletVersion"))
	// cpu, memory and pods, then extended resources such as nvidia.com/gpu.
	alloc := toStrMap(status["allocatable"])
	var parts []string
	for _, k := range []string{"cpu", "memory", "pods"} {
		if v, ok := alloc[k]; ok {
			parts = append(parts, k+"="+v)
		}
	}
	for _, k := range sortedKeys(alloc) {
		if strings.Contains(k, "/") {
			parts = append(parts, k+"="+alloc[k])
		}
	}
	rows = rowIf(rows, "Allocatable", strings.Join(parts, ", "))
	return section(nil, "Node", rows)
}

// hpaMetric reads one autoscaling/v2 MetricSpec or MetricStatus: the
// metric's name and its source block (which holds target or current).
func hpaMetric(m map[string]interface{}) (name string, src map[string]interface{}) {
	switch nmStr(m, "type") {
	case "Resource":
		src = nmMap(m, "resource")
		return nmStr(src, "name"), src
	case "ContainerResource":
		src = nmMap(m, "containerResource")
		return nmStr(src, "name"), src
	case "Pods":
		src = nmMap(m, "pods")
	case "Object":
		src = nmMap(m, "object")
	case "External":
		src = nmMap(m, "external")
	}
	return nmStr(nmMap(src, "metric"), "name"), src
}

// hpaValue is a target or current value: "70%" for utilization, else the
// average or plain value.
func hpaValue(v map[string]interface{}) string {
	if v["averageUtilization"] != nil {
		return fmt.Sprintf("%d%%", nmInt(v, "averageUtilization"))
	}
	if s := nmStr(v, "averageValue"); s != "" {
		return s
	}
	return nmStr(v, "value")
}

func infoForHPA(spec, status map[string]interface{}) []InfoSection {
	var rows []InfoRow
	if ref := nmMap(spec, "scaleTargetRef"); ref != nil {
		rows = rowIf(rows, "Target", nmStr(ref, "kind")+"/"+nmStr(ref, "name"))
	}
	if spec["minReplicas"] != nil {
		rows = rowIf(rows, "Min Replicas", countStr(spec, "minReplicas"))
	}
	if spec["maxReplicas"] != nil {
		rows = rowIf(rows, "Max Replicas", countStr(spec, "maxReplicas"))
	}
	rows = rowIf(rows, "Current Replicas", countStr(status, "currentReplicas"))
	rows = rowIf(rows, "Desired Replicas", countStr(status, "desiredReplicas"))
	current := map[string]string{}
	for _, raw := range nmSlice(status, "currentMetrics") {
		m, _ := raw.(map[string]interface{})
		if name, src := hpaMetric(m); name != "" {
			current[nmStr(m, "type")+"/"+name] = hpaValue(nmMap(src, "current"))
		}
	}
	var metrics []string
	for _, raw := range nmSlice(spec, "metrics") {
		m, _ := raw.(map[string]interface{})
		name, src := hpaMetric(m)
		if name == "" {
			continue
		}
		cur := current[nmStr(m, "type")+"/"+name]
		if cur == "" {
			cur = "<unknown>"
		}
		metrics = append(metrics, name+": "+cur+"/"+hpaValue(nmMap(src, "target")))
	}
	rows = rowIf(rows, "Metrics", strings.Join(metrics, ", "))
	return section(nil, "Autoscaler", rows)
}

// ingressBackend is "service:port" (port number or name).
func ingressBackend(b map[string]interface{}) string {
	svc := nmMap(b, "service")
	if svc == nil {
		if r := nmMap(b, "resource"); r != nil {
			return nmStr(r, "kind") + "/" + nmStr(r, "name")
		}
		return ""
	}
	port := nmMap(svc, "port")
	p := nmStr(port, "number")
	if p == "" {
		p = nmStr(port, "name")
	}
	if p == "" {
		return nmStr(svc, "name")
	}
	return nmStr(svc, "name") + ":" + p
}

func infoForIngress(spec, status map[string]interface{}) []InfoSection {
	var rows []InfoRow
	rows = rowIf(rows, "Class", nmStr(spec, "ingressClassName"))
	var rules []string
	for _, raw := range nmSlice(spec, "rules") {
		r, _ := raw.(map[string]interface{})
		for _, pr := range nmSlice(nmMap(r, "http"), "paths") {
			p, _ := pr.(map[string]interface{})
			rules = append(rules, nmStr(r, "host")+nmStr(p, "path")+" → "+ingressBackend(nmMap(p, "backend")))
		}
	}
	rows = rowIf(rows, "Rules", strings.Join(rules, ", "))
	rows = rowIf(rows, "Default Backend", ingressBackend(nmMap(spec, "defaultBackend")))
	var tls []string
	for _, raw := range nmSlice(spec, "tls") {
		t, _ := raw.(map[string]interface{})
		s := nmStr(t, "secretName")
		if hosts := nmStrings(t, "hosts"); len(hosts) > 0 {
			s = strings.TrimSpace(s + " (" + strings.Join(hosts, ", ") + ")")
		}
		if s != "" {
			tls = append(tls, s)
		}
	}
	rows = rowIf(rows, "TLS", strings.Join(tls, ", "))
	rows = rowIf(rows, "Load Balancer", loadBalancerIngress(status))
	return section(nil, "Ingress", rows)
}

// loadBalancerIngress joins status.loadBalancer.ingress (IP, else hostname)
// of a Service or Ingress with ", ".
func loadBalancerIngress(status map[string]interface{}) string {
	var lbs []string
	for _, raw := range nmSlice(nmMap(status, "loadBalancer"), "ingress") {
		lb, _ := raw.(map[string]interface{})
		if ip := nmStr(lb, "ip"); ip != "" {
			lbs = append(lbs, ip)
		} else if h := nmStr(lb, "hostname"); h != "" {
			lbs = append(lbs, h)
		}
	}
	return strings.Join(lbs, ", ")
}

// dataKeys are the names of a ConfigMap's or Secret's keys, never their values.
func dataKeys(raw map[string]interface{}) string {
	var keys []string
	for _, f := range []string{"data", "binaryData"} {
		keys = append(keys, sortedKeys(nmMap(raw, f))...)
	}
	sort.Strings(keys)
	return strings.Join(keys, ", ")
}

func infoForConfigMap(raw map[string]interface{}) []InfoSection {
	return section(nil, "Data", rowIf(nil, "Keys", dataKeys(raw)))
}

func infoForSecret(raw map[string]interface{}) []InfoSection {
	rows := rowIf(nil, "Type", nmStr(raw, "type"))
	return section(nil, "Data", rowIf(rows, "Keys", dataKeys(raw)))
}
