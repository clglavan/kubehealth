package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// A namespace snapshot is the live state of a namespace's workloads, pods
// and volume claims, listed when you open the namespace. Events say what went
// wrong; the snapshot says what is running now, including pods that are not
// ready without having raised an event, and it can say "all healthy".

const (
	snapshotTTL       = 30 * time.Second
	snapshotPendingOK = 5 * time.Minute // a pod pending for less is still starting
	snapshotMaxItems  = 15
)

type readyCount struct {
	Ready int `json:"ready"`
	Total int `json:"total"`
}

// AttentionItem is one thing the snapshot thinks needs a look.
type AttentionItem struct {
	Kind     string `json:"kind"`
	Name     string `json:"name"`
	Text     string `json:"text"`
	Severity string `json:"severity"` // crit or warn
}

type cronJobCounts struct {
	Total     int `json:"total"`
	Suspended int `json:"suspended"`
	Failing   int `json:"failing"`
}

type jobCounts struct {
	Active int `json:"active"`
	Failed int `json:"failed"`
}

type podCounts struct {
	Total     int   `json:"total"`
	Running   int   `json:"running"`
	Pending   int   `json:"pending"`
	Failed    int   `json:"failed"`
	Succeeded int   `json:"succeeded"`
	NotReady  int   `json:"not_ready"`
	Restarts  int32 `json:"restarts"`
}

type pvcCounts struct {
	Total   int `json:"total"`
	Bound   int `json:"bound"`
	Pending int `json:"pending"`
}

type pvCounts struct {
	Total  int `json:"total"`
	Bound  int `json:"bound"`
	Failed int `json:"failed"`
}

type NSSnapshot struct {
	Namespace    string          `json:"namespace"`
	AgeSec       int             `json:"age_s"`
	Deployments  readyCount      `json:"deployments"`
	StatefulSets readyCount      `json:"statefulsets"`
	DaemonSets   readyCount      `json:"daemonsets"`
	CronJobs     cronJobCounts   `json:"cronjobs"`
	Jobs         jobCounts       `json:"jobs"`
	Pods         podCounts       `json:"pods"`
	PVCs         pvcCounts       `json:"pvcs"`
	Nodes        *readyCount     `json:"nodes,omitempty"` // cluster-wide view only
	PVs          *pvCounts       `json:"pvs,omitempty"`   // cluster-wide view only
	Attention    []AttentionItem `json:"attention"`
	More         int             `json:"more,omitempty"`   // attention items left out
	Errors       []string        `json:"errors,omitempty"` // lists that failed (no access, …)

	fetchedAt time.Time
}

type snapshotCache struct {
	sync.Mutex
	byNS map[string]*NSSnapshot
}

func (c *clusterState) snapshot(ctx context.Context, ns string, refresh bool) *NSSnapshot {
	c.snapshots.Lock()
	if c.snapshots.byNS == nil {
		c.snapshots.byNS = map[string]*NSSnapshot{}
	}
	cached := c.snapshots.byNS[ns]
	c.snapshots.Unlock()
	if cached != nil && !refresh && time.Since(cached.fetchedAt) < snapshotTTL {
		out := *cached
		out.AgeSec = int(time.Since(cached.fetchedAt).Seconds())
		return &out
	}

	s := &NSSnapshot{Namespace: ns, Attention: []AttentionItem{}, fetchedAt: time.Now()}
	if ns == clusterScopeNS {
		c.snapshotCluster(ctx, s)
	} else {
		c.snapshotNamespace(ctx, s, ns)
	}
	sort.SliceStable(s.Attention, func(i, j int) bool {
		return s.Attention[i].Severity == "crit" && s.Attention[j].Severity != "crit"
	})
	if len(s.Attention) > snapshotMaxItems {
		s.More = len(s.Attention) - snapshotMaxItems
		s.Attention = s.Attention[:snapshotMaxItems]
	}
	c.snapshots.Lock()
	c.snapshots.byNS[ns] = s
	c.snapshots.Unlock()
	return s
}

func (s *NSSnapshot) attend(kind, name, sev, format string, args ...any) {
	s.Attention = append(s.Attention, AttentionItem{Kind: kind, Name: name, Severity: sev, Text: fmt.Sprintf(format, args...)})
}

func (s *NSSnapshot) failed(what string, err error) {
	s.Errors = append(s.Errors, fmt.Sprintf("%s: %v", what, err))
}

func (c *clusterState) snapshotNamespace(ctx context.Context, s *NSSnapshot, ns string) {
	opts := metav1.ListOptions{}
	apps, batch, core := c.client.AppsV1(), c.client.BatchV1(), c.client.CoreV1()

	if list, err := apps.Deployments(ns).List(ctx, opts); err != nil {
		s.failed("deployments", err)
	} else {
		for _, d := range list.Items {
			want := 1
			if d.Spec.Replicas != nil {
				want = int(*d.Spec.Replicas)
			}
			ready := int(d.Status.ReadyReplicas)
			s.Deployments.Total++
			if ready >= want {
				s.Deployments.Ready++
			} else {
				sev := "warn"
				if ready == 0 {
					sev = "crit"
				}
				s.attend("Deployment", d.Name, sev, "%d of %d replicas ready", ready, want)
			}
		}
	}
	if list, err := apps.StatefulSets(ns).List(ctx, opts); err != nil {
		s.failed("statefulsets", err)
	} else {
		for _, st := range list.Items {
			want := 1
			if st.Spec.Replicas != nil {
				want = int(*st.Spec.Replicas)
			}
			ready := int(st.Status.ReadyReplicas)
			s.StatefulSets.Total++
			if ready >= want {
				s.StatefulSets.Ready++
			} else {
				sev := "warn"
				if ready == 0 {
					sev = "crit"
				}
				s.attend("StatefulSet", st.Name, sev, "%d of %d replicas ready", ready, want)
			}
		}
	}
	if list, err := apps.DaemonSets(ns).List(ctx, opts); err != nil {
		s.failed("daemonsets", err)
	} else {
		for _, d := range list.Items {
			want, ready := int(d.Status.DesiredNumberScheduled), int(d.Status.NumberReady)
			s.DaemonSets.Total++
			if ready >= want {
				s.DaemonSets.Ready++
			} else {
				s.attend("DaemonSet", d.Name, "warn", "%d of %d pods ready", ready, want)
			}
		}
	}
	if list, err := batch.CronJobs(ns).List(ctx, opts); err != nil {
		s.failed("cronjobs", err)
	} else {
		for _, cj := range list.Items {
			s.CronJobs.Total++
			if cj.Spec.Suspend != nil && *cj.Spec.Suspend {
				s.CronJobs.Suspended++
				continue
			}
			last, ok := cj.Status.LastScheduleTime, cj.Status.LastSuccessfulTime
			switch {
			case last == nil:
			case ok == nil:
				s.CronJobs.Failing++
				s.attend("CronJob", cj.Name, "crit", "has never succeeded")
			case ok.Before(last):
				s.CronJobs.Failing++
				s.attend("CronJob", cj.Name, "crit", "last run failed; last success %s ago", formatAge(time.Since(ok.Time)))
			}
		}
	}
	if list, err := batch.Jobs(ns).List(ctx, opts); err != nil {
		s.failed("jobs", err)
	} else {
		for _, j := range list.Items {
			s.Jobs.Active += int(j.Status.Active)
			for _, cond := range j.Status.Conditions {
				if cond.Type == "Failed" && cond.Status == corev1.ConditionTrue {
					s.Jobs.Failed++
				}
			}
		}
	}
	if list, err := core.Pods(ns).List(ctx, opts); err != nil {
		s.failed("pods", err)
	} else {
		s.snapshotPods(list.Items)
	}
	if list, err := core.PersistentVolumeClaims(ns).List(ctx, opts); err != nil {
		s.failed("persistentvolumeclaims", err)
	} else {
		for _, p := range list.Items {
			s.PVCs.Total++
			switch p.Status.Phase {
			case corev1.ClaimBound:
				s.PVCs.Bound++
			case corev1.ClaimPending:
				s.PVCs.Pending++
				s.attend("PersistentVolumeClaim", p.Name, "warn", "pending, not bound to a volume")
			default:
				s.attend("PersistentVolumeClaim", p.Name, "crit", "%s", strings.ToLower(string(p.Status.Phase)))
			}
		}
	}
}

// snapshotPods counts pods by phase and groups the unhealthy ones by reason,
// so 60 crash-looping pods of one CronJob become one line.
func (s *NSSnapshot) snapshotPods(pods []corev1.Pod) {
	type group struct {
		sev   string
		names []string
	}
	groups := map[string]*group{}
	var order []string
	add := func(reason, sev, name string) {
		g := groups[reason]
		if g == nil {
			g = &group{sev: sev}
			groups[reason] = g
			order = append(order, reason)
		}
		g.names = append(g.names, name)
	}
	for _, p := range pods {
		s.Pods.Total++
		for _, cs := range p.Status.ContainerStatuses {
			s.Pods.Restarts += cs.RestartCount
		}
		switch p.Status.Phase {
		case corev1.PodSucceeded:
			s.Pods.Succeeded++
			continue
		case corev1.PodFailed:
			s.Pods.Failed++
			reason := p.Status.Reason
			if reason == "" {
				reason = "Failed"
			}
			add(reason, "warn", p.Name)
			continue
		case corev1.PodPending:
			s.Pods.Pending++
		case corev1.PodRunning:
			s.Pods.Running++
		}

		// A waiting container says why it isn't running.
		waiting := ""
		for _, cs := range p.Status.ContainerStatuses {
			if cs.State.Waiting != nil && cs.State.Waiting.Reason != "" && cs.State.Waiting.Reason != "ContainerCreating" {
				waiting = cs.State.Waiting.Reason
			}
		}
		ready := podReady(p)
		switch {
		case waiting != "":
			add(waiting, "crit", p.Name)
		case p.Status.Phase == corev1.PodPending && time.Since(p.CreationTimestamp.Time) > snapshotPendingOK:
			add("Pending for over 5 min", "warn", p.Name)
		case p.Status.Phase == corev1.PodRunning && !ready:
			add("Running but not ready", "warn", p.Name)
		}
		if p.Status.Phase == corev1.PodRunning && !ready {
			s.Pods.NotReady++
		}
	}
	for _, reason := range order {
		g := groups[reason]
		shown := g.names
		if len(shown) > 3 {
			shown = shown[:3]
		}
		// One pod: its name is shown, the text is the reason. Several: the
		// name says how many and the text lists a few.
		name, text := g.names[0], reason
		if len(g.names) > 1 {
			name = fmt.Sprintf("%d pods", len(g.names))
			text = fmt.Sprintf("%s: %s", reason, strings.Join(shown, ", "))
			if len(g.names) > 3 {
				text += fmt.Sprintf(" and %d more", len(g.names)-3)
			}
		}
		s.Attention = append(s.Attention, AttentionItem{Kind: "Pod", Name: name, Severity: g.sev, Text: text})
	}
}

func podReady(p corev1.Pod) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// snapshotCluster is the cluster-wide view: nodes and persistent volumes.
func (c *clusterState) snapshotCluster(ctx context.Context, s *NSSnapshot) {
	core := c.client.CoreV1()
	if list, err := core.Nodes().List(ctx, metav1.ListOptions{}); err != nil {
		s.failed("nodes", err)
	} else {
		s.Nodes = &readyCount{}
		for _, n := range list.Items {
			s.Nodes.Total++
			ready := false
			var pressure []string
			for _, cond := range n.Status.Conditions {
				switch {
				case cond.Type == corev1.NodeReady:
					ready = cond.Status == corev1.ConditionTrue
				case cond.Status == corev1.ConditionTrue && strings.HasSuffix(string(cond.Type), "Pressure"):
					pressure = append(pressure, string(cond.Type))
				}
			}
			if ready {
				s.Nodes.Ready++
			} else {
				s.attend("Node", n.Name, "crit", "not ready")
			}
			if len(pressure) > 0 {
				s.attend("Node", n.Name, "warn", "%s", strings.Join(pressure, ", "))
			}
			if n.Spec.Unschedulable {
				s.attend("Node", n.Name, "warn", "cordoned")
			}
		}
	}
	if list, err := core.PersistentVolumes().List(ctx, metav1.ListOptions{}); err != nil {
		s.failed("persistentvolumes", err)
	} else {
		s.PVs = &pvCounts{}
		for _, pv := range list.Items {
			s.PVs.Total++
			switch pv.Status.Phase {
			case corev1.VolumeBound:
				s.PVs.Bound++
			case corev1.VolumeFailed:
				s.PVs.Failed++
				s.attend("PersistentVolume", pv.Name, "crit", "failed")
			}
		}
	}
}

// handleAPISnapshot serves GET /api/snapshot/<namespace>[?refresh=1].
func handleAPISnapshot(w http.ResponseWriter, r *http.Request) {
	ns := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/snapshot/"), "/")
	if ns == "" {
		http.Error(w, "expected /api/snapshot/<namespace>", http.StatusBadRequest)
		return
	}
	c := apiCluster(w, r)
	if c == nil {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	s := c.snapshot(ctx, ns, r.URL.Query().Get("refresh") == "1")
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	json.NewEncoder(w).Encode(s)
}
