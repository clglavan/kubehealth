package main

// Link-following: before a first prompt (RCA or ✦ AI analysis) goes to the
// model, KubeHealth fetches the objects the anchor's evidence clearly names
// and adds a short block of what they say. A small model calls tools but
// often stops one hop short of the cause ("connection refused to redis":
// redis has no ready endpoint because its Pod is Pending); these rules are
// cheap, deterministic and close that hop for it.
//
// Every rule fires only when the anchor's own evidence names the object: its
// events, the warnings of the objects it owns, its describe (the Pods those
// warnings are about included) and the logs KubeHealth already fetched.
//
//  1. claim:      a Pod's FailedScheduling about volumes or claims, a
//     FailedMount/FailedAttachVolume naming a volume, or a Pending Pod whose
//     claim is not bound → the PVC and its StorageClass (rule storageclass
//     when the anchor is the claim itself).
//  2. service:    an error line (connection refused, no such host, …)
//     naming a Service by name, DNS name or cluster IP → its selector,
//     ports and endpoints, and when none is ready the Pods behind it.
//  3. config:     `configmap "x" not found`, `secret "x" not found`,
//     `couldn't find key k in Secret ns/x` → does it exist, its key names.
//  4. nodes:      FailedScheduling about taints, node affinity/selector or
//     Insufficient <resource> → a compact list of the nodes.
//  5. pullsecret: an image pull refused (pull access denied, unauthorized,
//     401, 403) or FailedToRetrieveImagePullSecret → the Pod's and its
//     ServiceAccount's imagePullSecrets.
//
// The block lists them most important first: config, claim, pullsecret,
// service, nodes (small and decisive first, the bulky node list last). It
// goes after the logs, before the closing instruction, and uses at most
// linkedShare of the loaded context: the least important lines go first,
// and the block says how many were trimmed. Secret and ConfigMap
// values are never read into it, only key names.

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	storagev1 "k8s.io/api/storage/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// ── Switch ────────────────────────────────────────────────────────────────────

var (
	linkFollowMu  sync.RWMutex
	linkFollowVal = true
)

func getLinkFollow() bool {
	linkFollowMu.RLock()
	defer linkFollowMu.RUnlock()
	return linkFollowVal
}

func setLinkFollow(on bool) {
	linkFollowMu.Lock()
	linkFollowVal = on
	linkFollowMu.Unlock()
}

// initLinkFollow reads AI_LINK_FOLLOW: "false" or "0" turns it off.
func initLinkFollow() {
	v := os.Getenv("AI_LINK_FOLLOW")
	setLinkFollow(v != "false" && v != "0")
}

// ── Limits ────────────────────────────────────────────────────────────────────

const (
	linkedShare       = 15  // percent of the loaded context the block may use
	linkedMaxPods     = 3   // evidence Pods whose volumes and pull secrets are read
	linkedMaxServices = 3   // Services followed from error lines
	linkedMaxNodes    = 6   // nodes listed for a scheduling failure
	linkedMaxBacking  = 3   // Pods listed under a Service with no ready endpoint
	linkedMaxEvents   = 3   // a followed object's newest warnings
	linkedMaxNames    = 8   // names listed (StorageClasses, ConfigMaps, keys …)
	linkedMsgLen      = 300 // characters of an event message
)

const linkedHeading = "\nLinked evidence (followed from the evidence above):\n"

// linkedItem is one followed object: `- <Kind> <Name>: <Facts>` and its
// indented `  - <line>` lines.
type linkedItem struct {
	Rule  string   `json:"rule"`
	Kind  string   `json:"kind"`
	NS    string   `json:"ns,omitempty"`
	Name  string   `json:"name"`
	Facts string   `json:"facts"`
	Lines []string `json:"lines,omitempty"`
}

func (it linkedItem) key() string { return it.Kind + "/" + it.NS + "/" + it.Name }

// tag names the rule and the object, for the bench results.
func (it linkedItem) tag() string { return it.Rule + ":" + it.Kind + "/" + it.Name }

// ── Rendering ─────────────────────────────────────────────────────────────────

func writeLinked(sb *strings.Builder, items []linkedItem, trimmed int) {
	sb.WriteString(linkedHeading)
	for _, it := range items {
		fmt.Fprintf(sb, "- %s %s: %s\n", it.Kind, it.Name, it.Facts)
		for _, l := range it.Lines {
			sb.WriteString("  - " + l + "\n")
		}
	}
	if trimmed > 0 {
		fmt.Fprintf(sb, "(%d linked evidence %s trimmed to fit the model's context)\n",
			trimmed, pluralWord("line", "lines", trimmed))
	}
}

// renderLinked writes the block within budget tokens (0: no limit). Over
// budget it drops whole objects from the end (the least important rules come
// last), then the last remaining object's lines from the end; if even that
// object's first line doesn't fit, there is no block. It returns the text,
// the items as printed and how many lines were trimmed.
func renderLinked(items []linkedItem, budget int) (string, []linkedItem, int) {
	if len(items) == 0 {
		return "", nil, 0
	}
	kept := make([]linkedItem, len(items))
	for i, it := range items {
		it.Lines = append([]string(nil), it.Lines...)
		kept[i] = it
	}
	trimmed := 0
	for {
		var sb strings.Builder
		writeLinked(&sb, kept, trimmed)
		if budget <= 0 || estimateTokens(sb.String()) <= budget {
			return sb.String(), kept, trimmed
		}
		last := &kept[len(kept)-1]
		switch {
		case len(kept) > 1:
			trimmed += 1 + len(last.Lines)
			kept = kept[:len(kept)-1]
		case len(last.Lines) > 0:
			last.Lines = last.Lines[:len(last.Lines)-1]
			trimmed++
		default:
			return "", nil, trimmed + 1
		}
	}
}

// linkedBudget is the block's token budget, or 0 when the context length is
// unknown (nothing else is trimmed then either).
func linkedBudget(ctxLen int) int {
	if ctxLen <= 0 {
		return 0
	}
	return max(ctxLen*linkedShare/100, 1)
}

// addLinked follows the links in the evidence gathered so far and sets the
// block. Off (AI_LINK_FOLLOW=false), the evidence stays as it was.
func (e *evidence) addLinked(cl *clusterState, ns, kind, name string, ctxLen int) {
	if !getLinkFollow() {
		return
	}
	items := followLinks(cl, ns, kind, name, e.events, e.memberEvents, e.logs)
	e.linked, e.linkedItems, e.linkedTrimmed = renderLinked(items, linkedBudget(ctxLen))
}

// ── The rules ─────────────────────────────────────────────────────────────────

type linker struct {
	cl             *clusterState
	ctx            context.Context
	ns, kind, name string
	warnings       []EventSummary // the anchor's Warning events, then its members'
	logs           string
	seen           map[string]bool
	items          []linkedItem

	pods     map[string]*corev1.Pod // Get cache; nil: not found
	services []corev1.Service       // the anchor namespace's, listed once
	svcErr   error
	svcDone  bool
}

// followLinks runs the rules over the anchor's evidence: its own events,
// the member warnings shown for a workload and the logs fetched.
// Cluster-scoped anchors are not followed.
func followLinks(cl *clusterState, ns, kind, name string, events, members []EventSummary, logs string) []linkedItem {
	if cl == nil || cl.client == nil || ns == "" || ns == clusterScopeNS {
		return nil
	}
	l := &linker{cl: cl, ctx: context.Background(), ns: ns, kind: kind, name: name, logs: logs,
		seen: map[string]bool{}, pods: map[string]*corev1.Pod{}}
	l.seen[kind+"/"+ns+"/"+name] = true
	for _, ev := range events {
		if ev.Type == "Warning" {
			l.warnings = append(l.warnings, ev)
		}
	}
	l.warnings = append(l.warnings, members...)
	l.configRefs()
	l.claims()
	l.pullSecrets()
	l.serviceDeps()
	l.nodes()
	return l.items
}

func (l *linker) add(it linkedItem) bool {
	if l.seen[it.key()] {
		return false
	}
	l.seen[it.key()] = true
	l.items = append(l.items, it)
	return true
}

func (l *linker) followed(kind, ns, name string) bool { return l.seen[kind+"/"+ns+"/"+name] }

func isPodKind(kind string) bool { return strings.EqualFold(kind, "Pod") }

// evidencePods are the Pods the evidence is about: the anchor Pod, or the
// Pods its members' warnings name, in the order they appear.
func (l *linker) evidencePods() []string {
	var out []string
	seen := map[string]bool{}
	if isPodKind(l.kind) {
		out, seen[l.name] = append(out, l.name), true
	}
	for _, ev := range l.warnings {
		if isPodKind(ev.Kind) && !seen[ev.ObjectName] && len(out) < linkedMaxPods {
			out, seen[ev.ObjectName] = append(out, ev.ObjectName), true
		}
	}
	return out
}

// podWarnings are the warnings in the evidence about one Pod.
func (l *linker) podWarnings(pod string) []EventSummary {
	var out []EventSummary
	for _, ev := range l.warnings {
		if isPodKind(ev.Kind) && ev.ObjectName == pod {
			out = append(out, ev)
		}
	}
	return out
}

func (l *linker) pod(name string) *corev1.Pod {
	if p, ok := l.pods[name]; ok {
		return p
	}
	p, err := l.cl.client.CoreV1().Pods(l.ns).Get(l.ctx, name, metav1.GetOptions{})
	if err != nil {
		p = nil
	}
	l.pods[name] = p
	return p
}

// texts are the evidence's messages and log lines, in that order.
func (l *linker) texts() []string {
	var out []string
	for _, ev := range l.warnings {
		out = append(out, ev.Message)
	}
	if l.logs != "" {
		out = append(out, strings.Split(l.logs, "\n")...)
	}
	return out
}

func linkedEventLine(ev EventSummary) string {
	return fmt.Sprintf("[%s] %s: %s  (×%d, %s ago)", ev.Type, ev.Reason, oneLine(truncate(ev.Message, linkedMsgLen)), ev.Count, ev.Age)
}

func oneLine(s string) string { return strings.Join(strings.Fields(s), " ") }

// objectWarnings are the newest n Warning events about one object.
func (l *linker) objectWarnings(ns, kind, name string, n int) []string {
	var ws []EventSummary
	for _, ev := range objectEvents(l.cl, ns, kind, name) {
		if ev.Type == "Warning" {
			ws = append(ws, ev)
		}
	}
	var out []string
	for _, ev := range newestEvents(ws, n) {
		out = append(out, linkedEventLine(ev))
	}
	return out
}

func listNames(names []string, n int) string {
	if len(names) > n {
		return strings.Join(names[:n], ", ") + fmt.Sprintf(", … (%d in all)", len(names))
	}
	return strings.Join(names, ", ")
}

// containsWord reports whether w appears in s as a whole name (not inside a
// longer name such as "metadata" for "data").
func containsWord(s, w string) bool {
	if w == "" {
		return false
	}
	nameByte := func(b byte) bool {
		return b == '-' || b == '.' || b == '_' || ('a' <= b && b <= 'z') || ('A' <= b && b <= 'Z') || ('0' <= b && b <= '9')
	}
	for i := 0; ; {
		j := strings.Index(s[i:], w)
		if j < 0 {
			return false
		}
		j += i
		end := j + len(w)
		before := j == 0 || !nameByte(s[j-1])
		// A trailing dot ends a sentence, not a name.
		after := end == len(s) || !nameByte(s[end]) || (s[end] == '.' && (end+1 == len(s) || !nameByte(s[end+1])))
		if before && after {
			return true
		}
		i = j + 1
	}
}

// ── Rule 3: ConfigMaps and Secrets named by an error ─────────────────────────

var (
	missingRefRe = regexp.MustCompile(`(?i)\b(configmaps?|secrets?) "([a-z0-9]([-a-z0-9.]*[a-z0-9])?)" not found`)
	missingKeyRe = regexp.MustCompile(`couldn't find key (\S+) in (Secret|ConfigMap) ([a-z0-9]([-a-z0-9]*[a-z0-9])?)/([a-z0-9]([-a-z0-9.]*[a-z0-9])?)`)
)

type configRef struct {
	kind, ns, name string
	keys           []string // keys the error says are missing
}

func (l *linker) configRefs() {
	var refs []*configRef
	byKey := map[string]*configRef{}
	ref := func(kind, ns, name string) *configRef {
		k := kind + "/" + ns + "/" + name
		if r := byKey[k]; r != nil {
			return r
		}
		r := &configRef{kind: kind, ns: ns, name: name}
		byKey[k] = r
		refs = append(refs, r)
		return r
	}
	for _, t := range l.texts() {
		for _, m := range missingKeyRe.FindAllStringSubmatch(t, -1) {
			r := ref(m[2], m[3], m[5])
			if !containsString(r.keys, m[1]) {
				r.keys = append(r.keys, m[1])
			}
		}
		for _, m := range missingRefRe.FindAllStringSubmatch(t, -1) {
			kind := "ConfigMap"
			if strings.HasPrefix(strings.ToLower(m[1]), "secret") {
				kind = "Secret"
			}
			ref(kind, l.ns, m[2])
		}
	}
	for _, r := range refs {
		if l.followed(r.kind, r.ns, r.name) {
			continue
		}
		if it, ok := l.configItem(r); ok {
			l.add(it)
		}
	}
}

func containsString(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

// configItem says whether a ConfigMap or Secret exists and, when it does,
// its key names (never values).
func (l *linker) configItem(r *configRef) (linkedItem, bool) {
	it := linkedItem{Rule: "config", Kind: r.kind, NS: r.ns, Name: r.name}
	var keys []string
	var err error
	if r.kind == "Secret" {
		var s *corev1.Secret
		s, err = l.cl.client.CoreV1().Secrets(r.ns).Get(l.ctx, r.name, metav1.GetOptions{})
		if err == nil {
			for k := range s.Data {
				keys = append(keys, k)
			}
			for k := range s.StringData {
				if !containsString(keys, k) {
					keys = append(keys, k)
				}
			}
		}
	} else {
		var cm *corev1.ConfigMap
		cm, err = l.cl.client.CoreV1().ConfigMaps(r.ns).Get(l.ctx, r.name, metav1.GetOptions{})
		if err == nil {
			for k := range cm.Data {
				keys = append(keys, k)
			}
			for k := range cm.BinaryData {
				keys = append(keys, k)
			}
		}
	}
	switch {
	case k8serrors.IsNotFound(err):
		it.Facts = "does not exist"
		if r.ns != l.ns {
			it.Facts += " in namespace " + r.ns
		}
		others := l.configNames(r.kind, r.ns)
		plural := r.kind + "s"
		if len(others) == 0 {
			it.Lines = append(it.Lines, "no "+plural+" in namespace "+r.ns)
		} else {
			it.Lines = append(it.Lines, plural+" in namespace "+r.ns+": "+listNames(others, linkedMaxNames))
		}
		return it, true
	case err != nil:
		return it, false
	}
	sort.Strings(keys)
	if len(keys) == 0 {
		it.Facts = "exists, no keys"
	} else {
		it.Facts = "exists, keys " + listNames(keys, 12)
	}
	var missing []string
	for _, k := range r.keys {
		if !containsString(keys, k) {
			missing = append(missing, fmt.Sprintf("%q", k))
		}
	}
	if len(missing) > 0 {
		it.Facts += " (no key " + strings.Join(missing, ", ") + ")"
	}
	return it, true
}

// configNames lists a namespace's ConfigMaps or Secrets by name, without
// the ones every namespace has (kube-root-ca.crt, service account tokens,
// Helm release records).
func (l *linker) configNames(kind, ns string) []string {
	var names []string
	if kind == "Secret" {
		list, err := l.cl.client.CoreV1().Secrets(ns).List(l.ctx, metav1.ListOptions{})
		if err != nil {
			return nil
		}
		for _, s := range list.Items {
			if s.Type == corev1.SecretTypeServiceAccountToken || s.Type == "helm.sh/release.v1" {
				continue
			}
			names = append(names, s.Name)
		}
	} else {
		list, err := l.cl.client.CoreV1().ConfigMaps(ns).List(l.ctx, metav1.ListOptions{})
		if err != nil {
			return nil
		}
		for _, c := range list.Items {
			if c.Name == "kube-root-ca.crt" {
				continue
			}
			names = append(names, c.Name)
		}
	}
	sort.Strings(names)
	return names
}

// ── Rule 1: claims and StorageClasses ────────────────────────────────────────

var volumeMsgRe = regexp.MustCompile(`(?i)persistentvolumeclaim|volume`)

func (l *linker) claims() {
	if strings.EqualFold(l.kind, "PersistentVolumeClaim") {
		l.anchorClaim()
		return
	}
	for _, pn := range l.evidencePods() {
		p := l.pod(pn)
		if p == nil {
			continue
		}
		var schedVolume bool
		var mounts []string
		for _, w := range l.podWarnings(pn) {
			switch w.Reason {
			case "FailedScheduling":
				schedVolume = schedVolume || volumeMsgRe.MatchString(w.Message)
			case "FailedMount", "FailedAttachVolume":
				mounts = append(mounts, w.Message)
			}
		}
		for _, v := range p.Spec.Volumes {
			claim := ""
			switch {
			case v.PersistentVolumeClaim != nil:
				claim = v.PersistentVolumeClaim.ClaimName
			case v.Ephemeral != nil:
				claim = p.Name + "-" + v.Name
			default:
				continue
			}
			if l.followed("PersistentVolumeClaim", l.ns, claim) {
				continue
			}
			named := false
			for _, m := range mounts {
				named = named || containsWord(m, v.Name) || containsWord(m, claim)
			}
			pvc, err := l.cl.client.CoreV1().PersistentVolumeClaims(l.ns).Get(l.ctx, claim, metav1.GetOptions{})
			missing := k8serrors.IsNotFound(err)
			if err != nil && !missing {
				continue
			}
			unbound := missing || pvc.Status.Phase != corev1.ClaimBound
			if !schedVolume && !named && !(p.Status.Phase == corev1.PodPending && unbound) {
				continue
			}
			l.add(l.claimItem(claim, pvc, missing))
		}
	}
}

func (l *linker) claimItem(name string, pvc *corev1.PersistentVolumeClaim, missing bool) linkedItem {
	it := linkedItem{Rule: "claim", Kind: "PersistentVolumeClaim", NS: l.ns, Name: name}
	if missing {
		it.Facts = "does not exist"
		return it
	}
	phase := string(pvc.Status.Phase)
	if phase == "" {
		phase = string(corev1.ClaimPending)
	}
	parts := []string{phase}
	scMissing := false
	switch sc := pvc.Spec.StorageClassName; {
	case sc == nil:
		parts = append(parts, "no StorageClass")
	case *sc == "":
		parts = append(parts, `StorageClass "" (no dynamic provisioning)`)
	default:
		info, ok := l.storageClassInfo(*sc)
		scMissing = !ok
		parts = append(parts, fmt.Sprintf("StorageClass %q (%s)", *sc, info))
	}
	size := ""
	if q, ok := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; ok {
		size = q.String()
	}
	var modes []string
	for _, m := range pvc.Spec.AccessModes {
		modes = append(modes, string(m))
	}
	if s := strings.TrimSpace(size + " " + strings.Join(modes, ",")); s != "" {
		parts = append(parts, s)
	}
	if pvc.Spec.VolumeName != "" {
		parts = append(parts, "volume "+pvc.Spec.VolumeName)
	}
	it.Facts = strings.Join(parts, ", ")
	it.Lines = l.objectWarnings(l.ns, "PersistentVolumeClaim", name, linkedMaxEvents)
	if scMissing {
		it.Lines = append(it.Lines, l.storageClassList())
	}
	return it
}

// anchorClaim follows the StorageClass of an anchor claim that isn't bound.
func (l *linker) anchorClaim() {
	pvc, err := l.cl.client.CoreV1().PersistentVolumeClaims(l.ns).Get(l.ctx, l.name, metav1.GetOptions{})
	if err != nil || pvc.Status.Phase == corev1.ClaimBound || pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName == "" {
		return
	}
	sc := *pvc.Spec.StorageClassName
	info, ok := l.storageClassInfo(sc)
	it := linkedItem{Rule: "storageclass", Kind: "StorageClass", Name: sc, Facts: info}
	if !ok {
		it.Lines = append(it.Lines, l.storageClassList())
	}
	l.add(it)
}

// storageClassInfo is "provisioner X, <binding mode>", or "does not exist"
// (ok false).
func (l *linker) storageClassInfo(name string) (string, bool) {
	sc, err := l.cl.client.StorageV1().StorageClasses().Get(l.ctx, name, metav1.GetOptions{})
	if k8serrors.IsNotFound(err) {
		return "does not exist", false
	}
	if err != nil {
		return "could not be read", true
	}
	mode := string(storagev1.VolumeBindingImmediate)
	if sc.VolumeBindingMode != nil {
		mode = string(*sc.VolumeBindingMode)
	}
	return "provisioner " + sc.Provisioner + ", " + mode, true
}

// storageClassList names the cluster's StorageClasses, the default marked.
func (l *linker) storageClassList() string {
	list, err := l.cl.client.StorageV1().StorageClasses().List(l.ctx, metav1.ListOptions{})
	if err != nil {
		return "StorageClasses in the cluster: could not be listed"
	}
	var names []string
	for _, sc := range list.Items {
		n := sc.Name
		if sc.Annotations["storageclass.kubernetes.io/is-default-class"] == "true" {
			n += " (default)"
		}
		names = append(names, n)
	}
	if len(names) == 0 {
		return "no StorageClasses in the cluster"
	}
	sort.Strings(names)
	return "StorageClasses in the cluster: " + listNames(names, linkedMaxNames)
}

// ── Rule 5: image pull secrets ───────────────────────────────────────────────

var (
	pullAuthRe = regexp.MustCompile(`(?i)pull access denied|unauthorized|authentication required|\b401\b|\b403\b`)
	pullRe     = regexp.MustCompile(`(?i)pull|image`)
)

func (l *linker) pullSecrets() {
	for _, pn := range l.evidencePods() {
		refused := false
		for _, w := range l.podWarnings(pn) {
			refused = refused || w.Reason == "FailedToRetrieveImagePullSecret" ||
				(pullRe.MatchString(w.Reason+" "+w.Message) && pullAuthRe.MatchString(w.Message))
		}
		if !refused {
			continue
		}
		p := l.pod(pn)
		if p == nil {
			continue
		}
		sa := p.Spec.ServiceAccountName
		if sa == "" {
			sa = "default"
		}
		of := "Pod " + p.Name
		found := false
		for _, r := range p.Spec.ImagePullSecrets {
			found = true
			l.add(l.pullSecretItem(r.Name, of))
		}
		acct, err := l.cl.client.CoreV1().ServiceAccounts(l.ns).Get(l.ctx, sa, metav1.GetOptions{})
		if err == nil {
			for _, r := range acct.ImagePullSecrets {
				found = true
				l.add(l.pullSecretItem(r.Name, "ServiceAccount "+sa))
			}
		}
		if !found {
			facts := "no imagePullSecrets, and " + of + " sets none"
			if k8serrors.IsNotFound(err) {
				facts = "does not exist; " + of + " sets no imagePullSecrets"
			}
			l.add(linkedItem{Rule: "pullsecret", Kind: "ServiceAccount", NS: l.ns, Name: sa, Facts: facts})
		}
		return // the Pods of one workload share their pull secrets
	}
}

func (l *linker) pullSecretItem(name, of string) linkedItem {
	it := linkedItem{Rule: "pullsecret", Kind: "Secret", NS: l.ns, Name: name}
	s, err := l.cl.client.CoreV1().Secrets(l.ns).Get(l.ctx, name, metav1.GetOptions{})
	switch {
	case k8serrors.IsNotFound(err):
		it.Facts = "does not exist (in imagePullSecrets of " + of + ")"
	case err != nil:
		it.Facts = "could not be read (in imagePullSecrets of " + of + ")"
	default:
		it.Facts = "exists, type " + string(s.Type) + " (in imagePullSecrets of " + of + ")"
	}
	return it
}

// ── Rule 2: Services named by connection errors ──────────────────────────────

var (
	connErrRe = regexp.MustCompile(`(?i)connection refused|no such host|i/o timeout|dial tcp|econnrefused|timed out|` +
		`can't connect|could not connect|cannot connect|unreachable|bad address|could not resolve|name or service not known|connection reset`)
	hostTokRe = regexp.MustCompile(`[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)*`)
	ipv4Re    = regexp.MustCompile(`^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$`)
)

// errorLines are the evidence's lines that report a failed connection, each
// after the log line before it (retry loops often name the host there: a
// "connecting to redis:6379" line, then "connection refused").
func (l *linker) errorLines() []string {
	var out []string
	for _, ev := range l.warnings {
		if connErrRe.MatchString(ev.Message) {
			out = append(out, ev.Message)
		}
	}
	lines := strings.Split(l.logs, "\n")
	for i, ln := range lines {
		if !connErrRe.MatchString(ln) {
			continue
		}
		if i > 0 {
			out = append(out, lines[i-1])
		}
		out = append(out, ln)
	}
	return out
}

func (l *linker) nsServices() []corev1.Service {
	if !l.svcDone {
		l.svcDone = true
		list, err := l.cl.client.CoreV1().Services(l.ns).List(l.ctx, metav1.ListOptions{})
		if err != nil {
			l.svcErr = err
		} else {
			l.services = list.Items
		}
	}
	return l.services
}

// serviceFor resolves a host token: <svc>, <svc>.<ns>, <svc>.<ns>.svc,
// <svc>.<ns>.svc.cluster.local, or a Service's cluster IP in the anchor's
// namespace.
func (l *linker) serviceFor(tok string, other map[string]*corev1.Service) *corev1.Service {
	if ipv4Re.MatchString(tok) {
		for i, s := range l.nsServices() {
			if s.Spec.ClusterIP == tok {
				return &l.services[i]
			}
		}
		return nil
	}
	parts := strings.Split(tok, ".")
	if parts[0][0] < 'a' || parts[0][0] > 'z' {
		return nil
	}
	if len(parts) == 1 || (parts[1] == l.ns && len(parts) >= 2) {
		rest := []string{}
		if len(parts) > 2 {
			rest = parts[2:]
		}
		if !dnsTail(rest) {
			return nil
		}
		for i, s := range l.nsServices() {
			if s.Name == parts[0] {
				return &l.services[i]
			}
		}
		return nil
	}
	if !dnsTail(parts[2:]) {
		return nil
	}
	key := parts[1] + "/" + parts[0]
	if s, ok := other[key]; ok {
		return s
	}
	s, err := l.cl.client.CoreV1().Services(parts[1]).Get(l.ctx, parts[0], metav1.GetOptions{})
	if err != nil {
		s = nil
	}
	other[key] = s
	return s
}

// dnsTail reports whether what follows <svc>.<ns> is a cluster DNS suffix.
func dnsTail(rest []string) bool {
	switch strings.Join(rest, ".") {
	case "", "svc", "svc.cluster.local":
		return true
	}
	return false
}

func (l *linker) serviceDeps() {
	other := map[string]*corev1.Service{}
	n := 0
	for _, line := range l.errorLines() {
		for _, tok := range hostTokRe.FindAllString(strings.ToLower(line), -1) {
			if n >= linkedMaxServices {
				return
			}
			s := l.serviceFor(tok, other)
			if s == nil || l.followed("Service", s.Namespace, s.Name) || l.selectsEvidence(s) {
				continue
			}
			if l.add(l.serviceItem(s)) {
				n++
			}
		}
	}
}

// selectsEvidence reports whether a Service selects the evidence's own Pods:
// the anchor's errors are about what it calls, not about itself.
func (l *linker) selectsEvidence(s *corev1.Service) bool {
	if len(s.Spec.Selector) == 0 || s.Namespace != l.ns {
		return false
	}
	sel := labels.SelectorFromSet(s.Spec.Selector)
	for _, pn := range l.evidencePods() {
		if p := l.pod(pn); p != nil && sel.Matches(labels.Set(p.Labels)) {
			return true
		}
	}
	return false
}

func (l *linker) serviceItem(s *corev1.Service) linkedItem {
	it := linkedItem{Rule: "service", Kind: "Service", NS: s.Namespace, Name: s.Name}
	if s.Namespace != l.ns {
		it.Name = s.Name + "." + s.Namespace
	}
	if s.Spec.Type == corev1.ServiceTypeExternalName {
		it.Facts = "ExternalName " + s.Spec.ExternalName
		return it
	}
	var parts []string
	if s.Spec.ClusterIP == corev1.ClusterIPNone {
		parts = append(parts, "headless")
	}
	if len(s.Spec.Selector) == 0 {
		parts = append(parts, "no selector")
	} else {
		parts = append(parts, "selector "+strings.ReplaceAll(joinKV(s.Spec.Selector), ", ", ","))
	}
	var ports []string
	for _, p := range s.Spec.Ports {
		proto := string(p.Protocol)
		if proto == "" {
			proto = "TCP"
		}
		tp := p.TargetPort.String()
		if tp == "0" || tp == fmt.Sprint(p.Port) {
			ports = append(ports, fmt.Sprintf("%d/%s", p.Port, proto))
		} else {
			ports = append(ports, fmt.Sprintf("%d→%s/%s", p.Port, tp, proto))
		}
	}
	if len(ports) > 0 {
		parts = append(parts, "ports "+strings.Join(ports, " "))
	}
	ready, notReady, epOK := l.endpointCounts(s.Namespace, s.Name)
	if epOK {
		e := fmt.Sprintf("endpoints %d ready", ready)
		if notReady > 0 {
			e += fmt.Sprintf(", %d not ready", notReady)
		}
		parts = append(parts, e)
	}
	it.Facts = strings.Join(parts, ", ")
	if len(s.Spec.Selector) == 0 {
		return it
	}
	plist, err := l.cl.client.CoreV1().Pods(s.Namespace).List(l.ctx,
		metav1.ListOptions{LabelSelector: labels.SelectorFromSet(s.Spec.Selector).String()})
	if err != nil {
		return it
	}
	pods := plist.Items
	if epOK && ready == 0 {
		if len(pods) == 0 {
			it.Lines = append(it.Lines, "selector matches no pods")
			it.Lines = append(it.Lines, l.selectorHints(s)...)
		} else {
			sort.SliceStable(pods, func(i, j int) bool {
				ri, rj := podReady(pods[i]), podReady(pods[j])
				if ri != rj {
					return !ri
				}
				return pods[i].Name < pods[j].Name
			})
			for i := range pods {
				if i == linkedMaxBacking {
					it.Lines = append(it.Lines, fmt.Sprintf("(%d more pods)", len(pods)-i))
					break
				}
				it.Lines = append(it.Lines, l.podLine(&pods[i]))
			}
		}
	}
	it.Lines = append(it.Lines, targetPortLines(s, pods)...)
	return it
}

// endpointCounts counts a Service's ready and not-ready endpoints as
// formatEndpoints does; ok false when the EndpointSlices can't be listed.
func (l *linker) endpointCounts(ns, name string) (ready, notReady int, ok bool) {
	list, err := l.cl.client.DiscoveryV1().EndpointSlices(ns).List(l.ctx,
		metav1.ListOptions{LabelSelector: discoveryv1.LabelServiceName + "=" + name})
	if err != nil {
		return 0, 0, false
	}
	seen := map[string]bool{}
	for _, s := range list.Items {
		for _, e := range s.Endpoints {
			if len(e.Addresses) == 0 || seen[e.Addresses[0]] {
				continue
			}
			seen[e.Addresses[0]] = true
			if e.Conditions.Ready == nil || *e.Conditions.Ready {
				ready++
			} else {
				notReady++
			}
		}
	}
	return ready, notReady, true
}

// podLine is one Pod behind a Service: phase, readiness, its containers'
// state and its newest warning.
func (l *linker) podLine(p *corev1.Pod) string {
	phase := string(p.Status.Phase)
	if p.DeletionTimestamp != nil {
		phase = "Terminating"
	}
	if phase == "" {
		phase = string(corev1.PodPending)
	}
	s := "Pod " + p.Name + ": " + phase
	if podReady(*p) {
		s += ", ready"
	} else {
		s += ", not ready"
	}
	if st := containerStateLine(p); st != "" {
		s += "; " + strings.TrimSuffix(strings.TrimPrefix(st, containerStatePrefix), ".")
	}
	var newest *EventSummary
	evs := objectEvents(l.cl, p.Namespace, "Pod", p.Name)
	for i := range evs {
		if evs[i].Type == "Warning" && (newest == nil || evs[i].LastTime.After(newest.LastTime)) {
			newest = &evs[i]
		}
	}
	if newest != nil {
		s += fmt.Sprintf(" — %s: %s  (×%d, %s ago)", newest.Reason, oneLine(truncate(newest.Message, linkedMsgLen)), newest.Count, newest.Age)
	}
	return s
}

// selectorHints explain a selector that matches no Pod: the workloads whose
// pod template it matches (and what their members report), or else the
// values the Pods in the namespace have for the selector's labels, closest
// first (a typo shows there).
func (l *linker) selectorHints(s *corev1.Service) []string {
	sel := labels.SelectorFromSet(s.Spec.Selector)
	var out []string
	type wl struct {
		kind, name    string
		ready, wanted int32
		tmpl          map[string]string
	}
	var wls []wl
	if list, err := l.cl.client.AppsV1().Deployments(s.Namespace).List(l.ctx, metav1.ListOptions{}); err == nil {
		for _, d := range list.Items {
			want := int32(1)
			if d.Spec.Replicas != nil {
				want = *d.Spec.Replicas
			}
			wls = append(wls, wl{"Deployment", d.Name, d.Status.ReadyReplicas, want, d.Spec.Template.Labels})
		}
	}
	if list, err := l.cl.client.AppsV1().StatefulSets(s.Namespace).List(l.ctx, metav1.ListOptions{}); err == nil {
		for _, d := range list.Items {
			want := int32(1)
			if d.Spec.Replicas != nil {
				want = *d.Spec.Replicas
			}
			wls = append(wls, wl{"StatefulSet", d.Name, d.Status.ReadyReplicas, want, d.Spec.Template.Labels})
		}
	}
	if list, err := l.cl.client.AppsV1().DaemonSets(s.Namespace).List(l.ctx, metav1.ListOptions{}); err == nil {
		for _, d := range list.Items {
			wls = append(wls, wl{"DaemonSet", d.Name, d.Status.NumberReady, d.Status.DesiredNumberScheduled, d.Spec.Template.Labels})
		}
	}
	for _, w := range wls {
		if len(out) == 2 {
			break
		}
		if !sel.Matches(labels.Set(w.tmpl)) {
			continue
		}
		line := fmt.Sprintf("%s %s (its pod template matches): %d of %d ready", w.kind, w.name, w.ready, w.wanted)
		if ws, _ := memberEvidence(l.cl, s.Namespace, w.kind, w.name); len(ws) > 0 {
			line += " — " + truncate(strings.TrimPrefix(ws[0], "- "), linkedMsgLen+80)
		}
		out = append(out, line)
	}
	if len(out) > 0 {
		return out
	}
	plist, err := l.cl.client.CoreV1().Pods(s.Namespace).List(l.ctx, metav1.ListOptions{})
	if err != nil {
		return nil
	}
	for _, k := range sortedKeys(s.Spec.Selector) {
		count := map[string]int{}
		for _, p := range plist.Items {
			if v, ok := p.Labels[k]; ok && v != s.Spec.Selector[k] {
				count[v]++
			}
		}
		if len(count) == 0 {
			continue
		}
		// Closest to the selector's value first: a typo shows at the top.
		want := s.Spec.Selector[k]
		vals := sortedKeys(count)
		sort.SliceStable(vals, func(i, j int) bool { return editDistance(vals[i], want) < editDistance(vals[j], want) })
		var parts []string
		for i, v := range vals {
			if i == 3 {
				parts = append(parts, "…")
				break
			}
			parts = append(parts, fmt.Sprintf("%s=%s (%d %s)", k, v, count[v], pluralWord("pod", "pods", count[v])))
		}
		out = append(out, "pods in namespace "+s.Namespace+" with label "+k+": "+strings.Join(parts, ", "))
	}
	return out
}

// editDistance is the Levenshtein distance between a and b.
func editDistance(a, b string) int {
	prev := make([]int, len(b)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(a); i++ {
		cur := make([]int, len(b)+1)
		cur[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			cur[j] = min(prev[j]+1, cur[j-1]+1, prev[j-1]+cost)
		}
		prev = cur
	}
	return prev[len(b)]
}

// targetPortLines flag a Service port whose targetPort none of its Pods
// declares, when they declare ports at all.
func targetPortLines(s *corev1.Service, pods []corev1.Pod) []string {
	type cport struct {
		num   int32
		name  string
		proto string
	}
	var declared []cport
	seen := map[string]bool{}
	for _, p := range pods {
		for _, c := range p.Spec.Containers {
			for _, cp := range c.Ports {
				proto := string(cp.Protocol)
				if proto == "" {
					proto = "TCP"
				}
				k := fmt.Sprintf("%d/%s/%s", cp.ContainerPort, proto, cp.Name)
				if !seen[k] {
					seen[k] = true
					declared = append(declared, cport{cp.ContainerPort, cp.Name, proto})
				}
			}
		}
	}
	if len(declared) == 0 {
		return nil
	}
	var list []string
	for _, d := range declared {
		list = append(list, fmt.Sprintf("%d/%s", d.num, d.proto))
	}
	they := "they declare " + strings.Join(list, " ")
	var out []string
	for _, sp := range s.Spec.Ports {
		proto := string(sp.Protocol)
		if proto == "" {
			proto = "TCP"
		}
		if sp.TargetPort.StrVal != "" && sp.TargetPort.IntVal == 0 {
			found := false
			for _, d := range declared {
				found = found || d.name == sp.TargetPort.StrVal
			}
			if !found {
				out = append(out, fmt.Sprintf("targetPort %q is not a port name its pods declare (%s)", sp.TargetPort.StrVal, they))
			}
			continue
		}
		tp := sp.TargetPort.IntVal
		if tp == 0 {
			tp = sp.Port
		}
		found := false
		for _, d := range declared {
			found = found || (d.num == tp && d.proto == proto)
		}
		if !found {
			out = append(out, fmt.Sprintf("targetPort %d is not a port its pods declare (%s)", tp, they))
		}
	}
	return out
}

// ── Rule 4: nodes, for a scheduling failure ──────────────────────────────────

var schedNodeRe = regexp.MustCompile(`(?i)taint|affinity|selector|insufficient`)

func (l *linker) nodes() {
	var p *corev1.Pod
	for _, pn := range l.evidencePods() {
		for _, w := range l.podWarnings(pn) {
			if w.Reason == "FailedScheduling" && schedNodeRe.MatchString(w.Message) {
				p = l.pod(pn)
				break
			}
		}
		if p != nil {
			break
		}
	}
	if p == nil {
		return
	}
	list, err := l.cl.client.CoreV1().Nodes().List(l.ctx, metav1.ListOptions{})
	if err != nil || len(list.Items) == 0 {
		return
	}
	keys := sortedKeys(p.Spec.NodeSelector)
	if a := p.Spec.Affinity; a != nil && a.NodeAffinity != nil && a.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
		for _, t := range a.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
			for _, e := range t.MatchExpressions {
				if !containsString(keys, e.Key) {
					keys = append(keys, e.Key)
				}
			}
		}
	}
	nodes := list.Items
	sel := labels.SelectorFromSet(p.Spec.NodeSelector)
	sort.SliceStable(nodes, func(i, j int) bool {
		mi, mj := sel.Matches(labels.Set(nodes[i].Labels)), sel.Matches(labels.Set(nodes[j].Labels))
		if mi != mj {
			return mi
		}
		return nodes[i].Name < nodes[j].Name
	})
	for i := range nodes {
		if i == linkedMaxNodes {
			last := &l.items[len(l.items)-1]
			last.Lines = append(last.Lines, fmt.Sprintf("(%d more nodes)", len(nodes)-i))
			break
		}
		l.add(nodeItem(&nodes[i], keys))
	}
}

func nodeItem(n *corev1.Node, keys []string) linkedItem {
	ready := "NotReady"
	for _, c := range n.Status.Conditions {
		if c.Type == corev1.NodeReady && c.Status == corev1.ConditionTrue {
			ready = "Ready"
		}
	}
	parts := []string{ready}
	if n.Spec.Unschedulable {
		parts = append(parts, "cordoned")
	}
	if len(n.Spec.Taints) == 0 {
		parts = append(parts, "no taints")
	} else {
		var ts []string
		for _, t := range n.Spec.Taints {
			s := t.Key
			if t.Value != "" {
				s += "=" + t.Value
			}
			ts = append(ts, s+":"+string(t.Effect))
		}
		parts = append(parts, "taints "+strings.Join(ts, " "))
	}
	var alloc []string
	if q, ok := n.Status.Allocatable[corev1.ResourceCPU]; ok {
		alloc = append(alloc, "cpu="+q.String())
	}
	if q, ok := n.Status.Allocatable[corev1.ResourceMemory]; ok {
		alloc = append(alloc, "memory="+memoryQuantity(q))
	}
	var ext []string
	for r := range n.Status.Allocatable {
		if strings.Contains(string(r), "/") {
			ext = append(ext, string(r))
		}
	}
	sort.Strings(ext)
	for _, r := range ext {
		q := n.Status.Allocatable[corev1.ResourceName(r)]
		alloc = append(alloc, r+"="+q.String())
	}
	if len(alloc) > 0 {
		parts = append(parts, "allocatable "+strings.Join(alloc, " "))
	}
	if len(keys) > 0 {
		var ls []string
		for _, k := range keys {
			v, ok := n.Labels[k]
			if !ok {
				v = "<unset>"
			}
			ls = append(ls, k+"="+v)
		}
		parts = append(parts, "labels "+strings.Join(ls, " "))
	}
	return linkedItem{Rule: "nodes", Kind: "Node", Name: n.Name, Facts: strings.Join(parts, ", ")}
}

// memoryQuantity prints memory in Gi or Mi, one decimal at most
// (16273592Ki → 15.5Gi).
func memoryQuantity(q resource.Quantity) string {
	b := float64(q.Value())
	unit, div := "Mi", float64(1<<20)
	if b >= 1<<30 {
		unit, div = "Gi", float64(1<<30)
	}
	s := strings.TrimSuffix(fmt.Sprintf("%.1f", b/div), ".0")
	return s + unit
}
