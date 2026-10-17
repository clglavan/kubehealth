package main

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
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
