package main

import (
	"context"
	"strings"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// Owner resolution groups a failing object with the workload that owns it
// (Pod → ReplicaSet → Deployment, Pod → Job → CronJob, Pod → StatefulSet →
// operator CR, ...). Controllers' naming conventions give a free first guess;
// a background resolver then confirms one representative per problem through
// ownerReferences, at most two GETs per second, and caches the answer.

type objRef struct{ Kind, Name string }

type ownerEntry struct {
	parent  objRef // zero Name when the object has no owner
	missing bool   // lookup failed (object deleted, kind unknown, no access)
	at      time.Time

	// For CronJobs: each run creates new Jobs and Pods, and their events
	// expire after about an hour, so events alone make a CronJob that has
	// failed for days look new. Its status says when it last worked.
	created     time.Time
	lastSuccess time.Time
}

type ownerJob struct{ ns, kind, name string }

const (
	ownerTTL         = 30 * time.Minute
	ownerMissingTTL  = 5 * time.Minute
	ownerMaxDepth    = 4
	ownerQueueMax    = 200
	ownerKnownMax    = 5000
	ownerLookupEvery = 500 * time.Millisecond
)

// ownerState is one cluster's owner lookups. The zero value is ready to use.
type ownerState struct {
	sync.Mutex
	known  map[string]ownerEntry // ns/kind/name → API answer
	kindOf map[string]string     // ns/name → kind, for every owner the API has named
	queue  []ownerJob
	queued map[string]bool
}

// lockInit locks o and creates its maps on first use.
func (o *ownerState) lockInit() {
	o.Lock()
	if o.known == nil {
		o.known = map[string]ownerEntry{}
		o.kindOf = map[string]string{}
		o.queued = map[string]bool{}
	}
}

func ownerKey(ns, kind, name string) string { return ns + "/" + kind + "/" + name }

func (e ownerEntry) fresh() bool {
	ttl := ownerTTL
	if e.missing {
		ttl = ownerMissingTTL
	}
	return time.Since(e.at) < ttl
}

// Generated name suffixes (pod-template-hash, generateName) use this
// alphabet: no vowels and no 0/1/3, so ordinary words never match.
const safeAlphabet = "bcdfghjklmnpqrstvwxz2456789"

func isSafeEncoded(s string, minLen, maxLen int) bool {
	if len(s) < minLen || len(s) > maxLen {
		return false
	}
	for _, c := range s {
		if !strings.ContainsRune(safeAlphabet, c) {
			return false
		}
	}
	return true
}

// inferParent guesses an object's controller from its name alone. The
// returned Kind is empty when the name fits several controllers: a
// DaemonSet's pod and a plain Job's pod are named the same way.
func inferParent(kind, name string) (objRef, bool) {
	prefix, suffix, ok := stripLast(name)
	if !ok || prefix == "" {
		return objRef{}, false
	}
	switch kind {
	case "Pod":
		if isAllDigits(suffix) && len(suffix) <= 4 {
			return objRef{"StatefulSet", prefix}, true
		}
		if isSafeEncoded(suffix, 5, 5) {
			if _, s2, ok := stripLast(prefix); ok {
				if len(s2) >= 8 && len(s2) <= 10 && isAllDigits(s2) {
					return objRef{"Job", prefix}, true
				}
				if isSafeEncoded(s2, 6, 10) {
					return objRef{"ReplicaSet", prefix}, true
				}
			}
			return objRef{"", prefix}, true
		}
	case "Job":
		if len(suffix) >= 8 && len(suffix) <= 10 && isAllDigits(suffix) {
			return objRef{"CronJob", prefix}, true
		}
	case "ReplicaSet":
		if isSafeEncoded(suffix, 6, 10) {
			return objRef{"Deployment", prefix}, true
		}
	}
	return objRef{}, false
}

// ownerChain walks from obj up to its top-level owner and returns the chain,
// object first. inferred is true when any link came from naming conventions
// rather than from the API.
func (c *clusterState) ownerChain(ns string, obj objRef) (chain []objRef, inferred bool) {
	owners := &c.owners
	owners.lockInit()
	defer owners.Unlock()
	chain = []objRef{obj}
	cur := obj
	for i := 0; i < ownerMaxDepth; i++ {
		if e, ok := owners.known[ownerKey(ns, cur.Kind, cur.Name)]; ok && !e.missing && e.fresh() {
			if e.parent.Name == "" {
				break
			}
			cur = e.parent
			chain = append(chain, cur)
			continue
		}
		p, ok := inferParent(cur.Kind, cur.Name)
		if !ok {
			break
		}
		// A sibling may already have told us what kind this controller is.
		if k, ok := owners.kindOf[ns+"/"+p.Name]; ok {
			p.Kind = k
		}
		inferred = true
		cur = p
		chain = append(chain, cur)
	}
	// The top of a chain must be fetched too: it may have an owner the names
	// don't show (a StatefulSet created by an operator's CR), and a CronJob's
	// status says when it last succeeded.
	if len(chain) > 1 {
		top := chain[len(chain)-1]
		if e, ok := owners.known[ownerKey(ns, top.Kind, top.Name)]; !ok || !e.fresh() {
			inferred = true
		}
	}
	return chain, inferred
}

// cronJobFailingSince returns when a CronJob's current failure streak began:
// its last successful run, or its creation if it never succeeded. ok is
// false until the resolver has fetched the CronJob.
func (c *clusterState) cronJobFailingSince(ns, name string) (since time.Time, neverSucceeded, ok bool) {
	owners := &c.owners
	owners.lockInit()
	defer owners.Unlock()
	e, found := owners.known[ownerKey(ns, "CronJob", name)]
	if !found || e.missing || !e.fresh() {
		return time.Time{}, false, false
	}
	if e.lastSuccess.IsZero() {
		return e.created, true, !e.created.IsZero()
	}
	return e.lastSuccess, false, true
}

// requestOwnerConfirmation queues the first unconfirmed link of chain for an
// API lookup. It returns false when the chain can't be confirmed through
// this object (it was deleted, or its kind is unknown), so the caller can try
// another member of the same problem.
func (c *clusterState) requestOwnerConfirmation(ns string, chain []objRef) bool {
	owners := &c.owners
	owners.lockInit()
	defer owners.Unlock()
	for _, node := range chain {
		e, ok := owners.known[ownerKey(ns, node.Kind, node.Name)]
		if ok && e.fresh() {
			if e.missing {
				return false
			}
			continue
		}
		if node.Kind == "" {
			return false
		}
		key := ownerKey(ns, node.Kind, node.Name)
		if !owners.queued[key] && len(owners.queue) < ownerQueueMax {
			owners.queue = append(owners.queue, ownerJob{ns, node.Kind, node.Name})
			owners.queued[key] = true
		}
		return true
	}
	return true
}

// requestRootLookup puts a chain's top object at the front of the queue.
// Used for CronJobs, whose status decides the problem's tier, so their
// lookup shouldn't wait behind owner confirmations that only clear tags.
func (c *clusterState) requestRootLookup(ns string, root objRef) {
	owners := &c.owners
	owners.lockInit()
	defer owners.Unlock()
	key := ownerKey(ns, root.Kind, root.Name)
	if e, ok := owners.known[key]; (ok && e.fresh()) || owners.queued[key] {
		return
	}
	owners.queue = append([]ownerJob{{ns, root.Kind, root.Name}}, owners.queue...)
	owners.queued[key] = true
}

// runOwnerResolver confirms queued owners, at most two API calls per second.
// It is idle unless problems need confirming: at startup and when cached
// answers expire.
func (c *clusterState) runOwnerResolver(ctx context.Context) {
	t := time.NewTicker(ownerLookupEvery)
	defer t.Stop()
	owners := &c.owners
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		owners.lockInit()
		if len(owners.queue) == 0 {
			owners.Unlock()
			continue
		}
		job := owners.queue[0]
		owners.queue = owners.queue[1:]
		owners.Unlock()
		c.resolveOwner(ctx, job)
	}
}

func (c *clusterState) resolveOwner(ctx context.Context, j ownerJob) {
	entry := ownerEntry{at: time.Now()}
	if gvr, err := c.kindToGVR(j.kind); err != nil {
		entry.missing = true
	} else {
		getCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		obj, err := c.dynamic.Resource(gvr).Namespace(j.ns).Get(getCtx, j.name, metav1.GetOptions{})
		cancel()
		if ctx.Err() != nil {
			// Switched away mid-lookup: forget the job so it is retried later.
			c.owners.lockInit()
			delete(c.owners.queued, ownerKey(j.ns, j.kind, j.name))
			c.owners.Unlock()
			return
		}
		if err != nil {
			entry.missing = true
		} else {
			entry.created = obj.GetCreationTimestamp().Time
			if j.kind == "CronJob" {
				if s, ok, _ := unstructured.NestedString(obj.Object, "status", "lastSuccessfulTime"); ok {
					entry.lastSuccess, _ = time.Parse(time.RFC3339, s)
				}
			}
			refs := obj.GetOwnerReferences()
			for _, ref := range refs {
				if ref.Controller != nil && *ref.Controller {
					entry.parent = objRef{ref.Kind, ref.Name}
					break
				}
			}
			if entry.parent.Name == "" && len(refs) > 0 {
				entry.parent = objRef{refs[0].Kind, refs[0].Name}
			}
		}
	}

	key := ownerKey(j.ns, j.kind, j.name)
	owners := &c.owners
	owners.lockInit()
	defer owners.Unlock()
	if len(owners.known) >= ownerKnownMax {
		for k, e := range owners.known {
			if !e.fresh() {
				delete(owners.known, k)
			}
		}
	}
	owners.known[key] = entry
	delete(owners.queued, key)
	if entry.parent.Name == "" {
		return
	}
	owners.kindOf[j.ns+"/"+entry.parent.Name] = entry.parent.Kind
	// Walk straight up the chain instead of waiting for the next page poll,
	// so a CronJob's last success is known within seconds of startup.
	pkey := ownerKey(j.ns, entry.parent.Kind, entry.parent.Name)
	if e, ok := owners.known[pkey]; (!ok || !e.fresh()) && !owners.queued[pkey] {
		owners.queue = append([]ownerJob{{j.ns, entry.parent.Kind, entry.parent.Name}}, owners.queue...)
		owners.queued[pkey] = true
	}
}
