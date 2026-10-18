package main

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestContainerStateLine(t *testing.T) {
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{Containers: []corev1.Container{
			{Name: "indexer", Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("48Mi")}}},
			{Name: "sidecar"},
		}},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
			{Name: "indexer", RestartCount: 8,
				LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "Error", ExitCode: 137}},
				State:                corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff", Message: "back-off 5m0s restarting failed container"}}},
			{Name: "sidecar", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}},
		}},
	}
	want := "Container state: indexer restarted 8 times, last run ended Error (exit code 137, SIGKILL: usually out of memory, memory limit 48Mi), now CrashLoopBackOff."
	if got := containerStateLine(pod); got != want {
		t.Errorf("got  %q\nwant %q", got, want)
	}

	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "indexer",
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff", Message: `Back-off pulling image "nginx:1.27.9-alpine"`}}}}
	want = `Container state: indexer now ImagePullBackOff: Back-off pulling image "nginx:1.27.9-alpine".`
	if got := containerStateLine(pod); got != want {
		t.Errorf("got  %q\nwant %q", got, want)
	}

	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "indexer", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}
	if got := containerStateLine(pod); got != "" {
		t.Errorf("healthy pod: got %q", got)
	}
}

func TestFailingInitContainers(t *testing.T) {
	init := func(cs corev1.ContainerStatus) *corev1.Pod {
		return &corev1.Pod{Status: corev1.PodStatus{InitContainerStatuses: []corev1.ContainerStatus{cs}}}
	}
	waiting := func(r string) corev1.ContainerState {
		return corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: r}}
	}
	for _, c := range []struct {
		cs      corev1.ContainerStatus
		failing bool
	}{
		{corev1.ContainerStatus{Name: "a", State: waiting("PodInitializing")}, false},
		{corev1.ContainerStatus{Name: "a", State: waiting("ContainerCreating")}, false},
		{corev1.ContainerStatus{Name: "a", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "Completed"}}}, false},
		{corev1.ContainerStatus{Name: "a", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}, false},
		{corev1.ContainerStatus{Name: "a", State: waiting("CrashLoopBackOff")}, true},
		{corev1.ContainerStatus{Name: "a", State: waiting("ImagePullBackOff")}, true},
		{corev1.ContainerStatus{Name: "a", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 2}}}, true},
		{corev1.ContainerStatus{Name: "a", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1}}}, true},
	} {
		if got := len(failingInitStatuses(init(c.cs))) == 1; got != c.failing {
			t.Errorf("%+v: failing=%v", c.cs.State, got)
		}
	}
}

func TestInitContainerLogs(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "api-0", Namespace: "billing"},
		Spec: corev1.PodSpec{
			InitContainers: []corev1.Container{{Name: "wait-db"}, {Name: "migrate"}},
			Containers:     []corev1.Container{{Name: "api"}},
		},
		Status: corev1.PodStatus{
			InitContainerStatuses: []corev1.ContainerStatus{
				{Name: "wait-db", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "Completed"}}},
				{Name: "migrate", RestartCount: 3,
					LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "Error", ExitCode: 1}},
					State:                corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}},
			},
			ContainerStatuses: []corev1.ContainerStatus{{Name: "api",
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "PodInitializing"}}}},
		},
	}
	c := testCluster()
	c.client = fake.NewSimpleClientset(pod)
	got, err := fetchLogsForAI(c, "billing", "api-0")
	want := "Container state: migrate (init) restarted 3 times, last run ended Error (exit code 1), now CrashLoopBackOff; api now PodInitializing.\n" +
		"=== init container: migrate ===\nfake logs\n=== container: api ===\nfake logs\n"
	if err != nil || got != want {
		t.Errorf("got  %q\nwant %q (%v)", got, want, err)
	}

	// Init containers that succeeded: exactly as before, no headers.
	pod.Status.InitContainerStatuses = pod.Status.InitContainerStatuses[:1]
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "api", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}
	c.client = fake.NewSimpleClientset(pod)
	if got, _ := fetchLogsForAI(c, "billing", "api-0"); got != "fake logs\n" {
		t.Errorf("healthy init: %q", got)
	}
}
