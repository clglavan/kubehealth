package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// What a snapshot holds: everything KubeHealth's note, ✦ AI analysis and
// RCA read from a cluster. Events (the sweep and the warning feed), objects
// for describe and owner lookups (dynamic GETs), Pods for container state
// and logs, Namespaces for the sweep, and API discovery for kinds outside
// the static map (PersistentVolume, StorageClass, CRDs).
var (
	benchNamespacedResources = []schema.GroupVersionResource{
		{Version: "v1", Resource: "pods"},
		{Group: "apps", Version: "v1", Resource: "deployments"},
		{Group: "apps", Version: "v1", Resource: "replicasets"},
		{Group: "apps", Version: "v1", Resource: "statefulsets"},
		{Group: "apps", Version: "v1", Resource: "daemonsets"},
		{Group: "batch", Version: "v1", Resource: "jobs"},
		{Group: "batch", Version: "v1", Resource: "cronjobs"},
		{Version: "v1", Resource: "services"},
		{Group: "discovery.k8s.io", Version: "v1", Resource: "endpointslices"},
		{Version: "v1", Resource: "persistentvolumeclaims"},
		{Version: "v1", Resource: "configmaps"}, // metadata and key names only
		{Version: "v1", Resource: "secrets"},    // metadata, type and key names only
		{Group: "autoscaling", Version: "v2", Resource: "horizontalpodautoscalers"},
		{Group: "networking.k8s.io", Version: "v1", Resource: "ingresses"},
		{Version: "v1", Resource: "serviceaccounts"},
		{Version: "v1", Resource: "events"},
	}
	benchClusterResources = []schema.GroupVersionResource{
		{Version: "v1", Resource: "namespaces"},
		{Version: "v1", Resource: "nodes"},
		{Version: "v1", Resource: "persistentvolumes"},
		{Group: "storage.k8s.io", Version: "v1", Resource: "storageclasses"},
	}
	benchSystemNamespaces = []string{"kube-system", "kube-public", "kube-node-lease", "local-path-storage"}
)

const lastAppliedAnnotation = "kubectl.kubernetes.io/last-applied-configuration"

func benchRecordCmd(args []string) error {
	fs := flag.NewFlagSet("bench record", flag.ExitOnError)
	kubeconfig := fs.String("kubeconfig", defaultKubeconfig(), "kubeconfig to read the cluster from")
	kctx := fs.String("context", "", "kubeconfig context (default: its current context)")
	out := fs.String("out", "", "snapshot directory to write, e.g. bench/snapshots/<name> (replaced if it holds a snapshot)")
	skip := fs.String("skip-namespaces", strings.Join(benchSystemNamespaces, ","), "namespaces not to record")
	only := fs.String("namespaces", "", "record only these namespaces (comma-separated; default: all but -skip-namespaces)")
	tail := fs.Int64("tail", getLogTailLines(), "container log lines to record (KubeHealth fetches this many)")
	fs.Parse(args)
	if *out == "" {
		return errors.New("record: -out is required")
	}
	rules := &clientcmd.ClientConfigLoadingRules{ExplicitPath: *kubeconfig}
	cc := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{CurrentContext: *kctx})
	raw, err := cc.RawConfig()
	if err != nil {
		return err
	}
	if *kctx == "" {
		*kctx = raw.CurrentContext
	}
	rc, err := cc.ClientConfig()
	if err != nil {
		return err
	}
	rc.WarningHandler = rest.NoWarnings{}
	cs, err := kubernetes.NewForConfig(rc)
	if err != nil {
		return err
	}
	dyn, err := dynamic.NewForConfig(rc)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	meta, err := recordSnapshot(ctx, cs, dyn, recordOpts{
		dir: *out, context: *kctx, at: time.Now(), tail: *tail, skip: splitList(*skip), only: splitList(*only),
	})
	if err != nil {
		return err
	}
	log.Printf("bench record: %s (context %s) → %s: %d namespaces, recorded at %s",
		meta.Name, meta.Context, *out, len(meta.Namespaces), meta.RecordedAt.Format(time.RFC3339))
	return nil
}

type recordOpts struct {
	dir     string
	context string
	at      time.Time // recording time; truncated to whole seconds
	tail    int64
	skip    []string
	only    []string // when set, the only namespaces recorded (still minus skip)
}

// recordSnapshot writes a snapshot of the cluster behind cs/dyn into o.dir:
//
//	snapshot.yaml                     metadata (snapshotMeta)
//	discovery.yaml                    API discovery (kinds → resources)
//	cluster/<resource>.yaml           cluster-scoped objects
//	namespaces/<ns>/<resource>.yaml   namespaced objects and events
//	logs/<ns>/<pod>/<container>[.previous].log
//
// managedFields and last-applied annotations are dropped; ConfigMaps and
// Secrets keep their metadata and key names, never their values, and
// containers keep the names of literal env vars but not their values.
func recordSnapshot(ctx context.Context, cs kubernetes.Interface, dyn dynamic.Interface, o recordOpts) (*snapshotMeta, error) {
	if err := prepareSnapshotDir(o.dir); err != nil {
		return nil, err
	}
	skip := map[string]bool{}
	for _, ns := range o.skip {
		skip[ns] = true
	}
	only := map[string]bool{}
	for _, ns := range o.only {
		only[ns] = true
	}
	meta := &snapshotMeta{
		Name:              filepath.Base(filepath.Clean(o.dir)),
		Context:           o.context,
		RecordedAt:        o.at.UTC().Truncate(time.Second),
		RecordedWith:      "kubehealth " + version,
		LogTailLines:      o.tail,
		SkippedNamespaces: o.skip,
	}

	for _, gvr := range benchClusterResources {
		items, err := listForSnapshot(ctx, dyn.Resource(gvr), gvr)
		if err != nil {
			meta.Notes = append(meta.Notes, fmt.Sprintf("%s not recorded: %v", gvr.Resource, err))
			continue
		}
		if gvr.Resource == "namespaces" {
			var kept []map[string]any
			for _, it := range items {
				name := nmStr(nmMap(it, "metadata"), "name")
				if skip[name] || (len(only) > 0 && !only[name]) {
					continue
				}
				kept = append(kept, it)
				meta.Namespaces = append(meta.Namespaces, name)
			}
			items = kept
		}
		if err := writeObjects(filepath.Join(o.dir, "cluster", gvr.Resource+".yaml"), items); err != nil {
			return nil, err
		}
	}
	sort.Strings(meta.Namespaces)

	for _, ns := range meta.Namespaces {
		for _, gvr := range benchNamespacedResources {
			items, err := listForSnapshot(ctx, dyn.Resource(gvr).Namespace(ns), gvr)
			if err != nil {
				meta.Notes = append(meta.Notes, fmt.Sprintf("%s/%s not recorded: %v", ns, gvr.Resource, err))
				continue
			}
			if err := writeObjects(filepath.Join(o.dir, "namespaces", ns, gvr.Resource+".yaml"), items); err != nil {
				return nil, err
			}
			if gvr.Resource == "pods" {
				if err := recordPodLogs(ctx, cs, o.dir, ns, items, o.tail); err != nil {
					return nil, err
				}
			}
		}
	}

	if err := recordDiscovery(cs, o.dir); err != nil {
		meta.Notes = append(meta.Notes, "discovery not recorded: "+err.Error())
	}
	if err := writeYAML(filepath.Join(o.dir, snapshotMetaFile), meta); err != nil {
		return nil, err
	}
	return meta, nil
}

// prepareSnapshotDir empties dir when it holds an earlier snapshot, and
// refuses to write into any other non-empty directory.
func prepareSnapshotDir(dir string) error {
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return os.MkdirAll(dir, 0o755)
	}
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}
	if _, err := os.Stat(filepath.Join(dir, snapshotMetaFile)); err != nil {
		return fmt.Errorf("%s is not empty and holds no snapshot; refusing to overwrite it", dir)
	}
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(dir, e.Name())); err != nil {
			return err
		}
	}
	return nil
}

func listForSnapshot(ctx context.Context, ri dynamic.ResourceInterface, gvr schema.GroupVersionResource) ([]map[string]any, error) {
	list, err := ri.List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	gv := gvr.GroupVersion().String()
	kind := strings.TrimSuffix(list.GetKind(), "List")
	var out []map[string]any
	for _, it := range list.Items {
		obj := it.Object
		// List items come without their own apiVersion/kind on some servers.
		if it.GetAPIVersion() == "" {
			obj["apiVersion"] = gv
		}
		if it.GetKind() == "" && kind != "" {
			obj["kind"] = kind
		}
		sanitizeForSnapshot(obj)
		out = append(out, obj)
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := nmMap(out[i], "metadata"), nmMap(out[j], "metadata")
		return nmStr(a, "name") < nmStr(b, "name")
	})
	return out, nil
}

// sanitizeForSnapshot drops what KubeHealth never reads and what must not be
// stored: managedFields, last-applied annotations (they repeat the whole
// object, a Secret's data included), the values of ConfigMap and Secret keys
// (describe prints the key names, so they stay, with empty values) and the
// values of containers' literal env vars (describe shows only references).
func sanitizeForSnapshot(obj map[string]any) {
	md := nmMap(obj, "metadata")
	if md != nil {
		delete(md, "managedFields")
		if ann := nmMap(md, "annotations"); ann != nil {
			delete(ann, lastAppliedAnnotation)
			if len(ann) == 0 {
				delete(md, "annotations")
			}
		}
	}
	switch obj["kind"] {
	case "ConfigMap", "Secret":
		for _, f := range []string{"data", "binaryData", "stringData"} {
			if m := nmMap(obj, f); m != nil {
				for k := range m {
					m[k] = ""
				}
			}
		}
	}
	dropEnvValues(obj)
}

// dropEnvValues removes the value of every literal env var (env[].value) in
// any container list of a Pod or of a workload's pod template.
func dropEnvValues(v any) {
	switch t := v.(type) {
	case map[string]any:
		for k, val := range t {
			if k == "env" {
				for _, e := range asSlice(val) {
					if em, ok := e.(map[string]any); ok {
						delete(em, "value")
					}
				}
				continue
			}
			dropEnvValues(val)
		}
	case []any:
		for _, val := range t {
			dropEnvValues(val)
		}
	}
}

func asSlice(v any) []any {
	s, _ := v.([]any)
	return s
}

func writeObjects(path string, items []map[string]any) error {
	if len(items) == 0 {
		return nil
	}
	return writeYAML(path, map[string]any{"apiVersion": "v1", "kind": "List", "items": items})
}

// recordPodLogs saves each container's current and previous logs, the same
// requests KubeHealth makes (tail lines, Previous=true for crash loops).
// Requests the API refuses (no previous run, container not started) leave
// no file, and replay answers them with an error as the API did.
func recordPodLogs(ctx context.Context, cs kubernetes.Interface, dir, ns string, pods []map[string]any, tail int64) error {
	for _, p := range pods {
		u := unstructured.Unstructured{Object: p}
		inits, _, _ := unstructured.NestedSlice(p, "spec", "initContainers")
		containers, _, _ := unstructured.NestedSlice(p, "spec", "containers")
		for _, c := range append(inits, containers...) {
			name := nmStr(c.(map[string]any), "name")
			for _, previous := range []bool{false, true} {
				stream, err := cs.CoreV1().Pods(ns).GetLogs(u.GetName(), &corev1.PodLogOptions{
					Container: name, TailLines: &tail, Previous: previous,
				}).Stream(ctx)
				if err != nil {
					continue
				}
				b, err := io.ReadAll(stream)
				stream.Close()
				if err != nil {
					continue
				}
				if err := os.MkdirAll(filepath.Join(dir, "logs", ns, u.GetName()), 0o755); err != nil {
					return err
				}
				if err := os.WriteFile(logFile(dir, ns, u.GetName(), name, previous), b, 0o644); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func logFile(dir, ns, pod, container string, previous bool) string {
	name := container + ".log"
	if previous {
		name = container + ".previous.log"
	}
	return filepath.Join(dir, "logs", ns, pod, name)
}

// recordDiscovery saves the API's resource lists, which replay serves so
// that kinds outside KubeHealth's static map resolve as they did.
func recordDiscovery(cs kubernetes.Interface, dir string) error {
	_, lists, err := cs.Discovery().ServerGroupsAndResources()
	if err != nil && lists == nil {
		return err
	}
	sort.Slice(lists, func(i, j int) bool { return lists[i].GroupVersion < lists[j].GroupVersion })
	for _, l := range lists {
		sort.Slice(l.APIResources, func(i, j int) bool { return l.APIResources[i].Name < l.APIResources[j].Name })
	}
	return writeYAML(filepath.Join(dir, "discovery.yaml"), map[string]any{"resources": lists})
}

func splitList(s string) []string {
	var out []string
	for _, v := range strings.Split(s, ",") {
		if v = strings.TrimSpace(v); v != "" {
			out = append(out, v)
		}
	}
	return out
}
