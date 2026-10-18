package main

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utiljson "k8s.io/apimachinery/pkg/util/json"
	"k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/yaml"
)

// describeFixture is an object as the dynamic client returns it and the
// describe body KubeHealth must print for it.
type describeFixture struct {
	name      string // test name
	kind      string
	yaml      string
	endpoints string // a Service's Endpoints row
	want      string
}

var fixtureNow = time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)

// fixtureInfo decodes the fixture as the dynamic client would (integers as
// int64) and describes it.
func fixtureInfo(t *testing.T, f describeFixture) ObjectInfo {
	t.Helper()
	j, err := yaml.YAMLToJSON([]byte(f.yaml))
	if err != nil {
		t.Fatalf("%s: %v", f.name, err)
	}
	var raw map[string]interface{}
	if err := utiljson.Unmarshal(j, &raw); err != nil {
		t.Fatalf("%s: %v", f.name, err)
	}
	meta := nmMap(raw, "metadata")
	ns := nmStr(meta, "namespace")
	if ns == "" {
		ns = clusterScopeNS
	}
	info := ObjectInfo{Kind: f.kind, Name: nmStr(meta, "name"), Namespace: ns}
	return describeObject(info, raw, f.endpoints, fixtureNow)
}

const longAnnotation = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
	"0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789X"

var describeFixtures = []describeFixture{
	{
		name: "pod full", kind: "Pod",
		yaml: `
metadata:
  name: api-7d9f6c-x2k4p
  namespace: billing
  creationTimestamp: "2026-10-18T11:46:00Z"
  labels: {app: api, pod-template-hash: 7d9f6c}
  annotations:
    kubectl.kubernetes.io/last-applied-configuration: '{"apiVersion":"v1"}'
    prometheus.io/scrape: "true"
    long: "` + longAnnotation + `"
spec:
  nodeName: worker-1
  serviceAccountName: api
  restartPolicy: Always
  priorityClassName: high
  nodeSelector: {kubernetes.io/arch: amd64, disktype: ssd}
  affinity:
    podAntiAffinity: {preferredDuringSchedulingIgnoredDuringExecution: [{weight: 1, podAffinityTerm: {topologyKey: kubernetes.io/hostname}}]}
    nodeAffinity: {requiredDuringSchedulingIgnoredDuringExecution: {nodeSelectorTerms: [{matchExpressions: [{key: zone, operator: In, values: [a]}]}]}}
  tolerations:
  - {key: node.kubernetes.io/not-ready, operator: Exists, effect: NoExecute, tolerationSeconds: 300}
  - {key: node.kubernetes.io/unreachable, operator: Exists, effect: NoExecute, tolerationSeconds: 300}
  - {key: dedicated, operator: Equal, value: gpu, effect: NoSchedule}
  - {key: spot, operator: Exists, effect: NoSchedule}
  - {key: team, value: a}
  - {operator: Exists, effect: NoExecute}
  - {operator: Exists}
  - {key: node.kubernetes.io/not-ready, operator: Exists, effect: NoExecute, tolerationSeconds: 60}
  securityContext: {runAsNonRoot: true, fsGroup: 2000}
  initContainers:
  - name: migrate
    image: ghcr.io/acme/migrate:1.2
    command: [sh, -c]
    args: ["./migrate up\n  --verbose"]
    envFrom: [{secretRef: {name: db-creds}}]
  containers:
  - name: api
    image: ghcr.io/acme/api:2.0
    resources: {requests: {cpu: 100m, memory: 128Mi}, limits: {memory: 256Mi}}
    ports: [{containerPort: 8080}, {containerPort: 9090, protocol: TCP}, {containerPort: 53, protocol: UDP}]
    livenessProbe: {httpGet: {path: /healthz, port: 8080, scheme: HTTP}, initialDelaySeconds: 5, timeoutSeconds: 1, periodSeconds: 10, successThreshold: 1, failureThreshold: 3}
    readinessProbe: {tcpSocket: {port: 6379}, periodSeconds: 90}
    startupProbe: {exec: {command: [cat, /tmp/ready]}, failureThreshold: 30}
    envFrom: [{configMapRef: {name: app-config}}, {secretRef: {name: extra, optional: true}}]
    env:
    - {name: LITERAL, value: s3cr3t-literal}
    - {name: DB_PASSWORD, valueFrom: {secretKeyRef: {name: db-creds, key: password}}}
    - {name: LOG_LEVEL, valueFrom: {configMapKeyRef: {name: app-config, key: log-level, optional: true}}}
    - {name: POD_IP, valueFrom: {fieldRef: {fieldPath: status.podIP}}}
    - {name: MEM, valueFrom: {resourceFieldRef: {resource: limits.memory}}}
    volumeMounts:
    - {name: cache, mountPath: /var/cache}
    - {name: tls, mountPath: /etc/tls, readOnly: true}
    - {name: kube-api-access-x7k2p, mountPath: /var/run/secrets/kubernetes.io/serviceaccount, readOnly: true}
    securityContext: {runAsUser: 1000, readOnlyRootFilesystem: true, allowPrivilegeEscalation: false, privileged: false, capabilities: {drop: [ALL], add: [NET_ADMIN, SYS_TIME]}}
  - name: proxy
    image: envoyproxy/envoy:v1.30.1
    args: ["` + strings.Repeat("--flag-value ", 20) + `"]
    livenessProbe: {grpc: {port: 9090}}
    readinessProbe: {httpGet: {path: /ready, port: admin, scheme: HTTPS}}
  volumes:
  - {name: cache, emptyDir: {medium: Memory, sizeLimit: 1Gi}}
  - {name: tls, secret: {secretName: api-tls, optional: true}}
  - {name: cfg, configMap: {name: app-config}}
  - {name: data, persistentVolumeClaim: {claimName: data-api-0}}
  - {name: host, hostPath: {path: /var/log}}
  - {name: store, csi: {driver: secrets-store.csi.k8s.io}}
  - {name: share, nfs: {server: 10.0.0.5, path: /exports}}
  - {name: scratch, ephemeral: {volumeClaimTemplate: {spec: {accessModes: [ReadWriteOnce]}}}}
  - {name: podinfo, downwardAPI: {items: [{path: labels, fieldRef: {fieldPath: metadata.labels}}]}}
  - {name: bundle, projected: {sources: [{configMap: {name: a}}]}}
  - {name: repo, gitRepo: {repository: "https://example.com/r.git"}}
  - {name: tmp, emptyDir: {}}
  - {name: kube-api-access-x7k2p, projected: {sources: [{serviceAccountToken: {path: token}}]}}
status:
  phase: Pending
  podIP: 10.244.1.23
  hostIP: 172.18.0.3
  qosClass: Burstable
  initContainerStatuses: [{name: migrate, ready: false, restartCount: 4}]
  containerStatuses: [{name: api, ready: false, restartCount: 0}, {name: proxy, ready: false, restartCount: 0}]
  conditions:
  - {type: Initialized, status: "False", reason: ContainersNotInitialized, message: "containers with incomplete status: [migrate]"}
  - {type: PodScheduled, status: "True"}
`,
		want: `Age: 14m
Labels: app=api pod-template-hash=7d9f6c
Annotations: prometheus.io/scrape=true
Runtime:
  Phase: Pending
  Pod IP: 10.244.1.23
  Node: worker-1
  Host IP: 172.18.0.3
  QoS Class: Burstable
  Service Account: api
  Node Selector: disktype=ssd, kubernetes.io/arch=amd64
  Tolerations: dedicated=gpu:NoSchedule, spot:NoSchedule, team=a, :NoExecute, op=Exists, node.kubernetes.io/not-ready:NoExecute for 60s
  Affinity: nodeAffinity, podAntiAffinity
  Priority Class: high
Init Container: migrate:
  Image: ghcr.io/acme/migrate:1.2
  Command: sh -c
  Args: ./migrate up --verbose
  Env From: secret/db-creds
  Security: runAsNonRoot=true fsGroup=2000
Container: api:
  Image: ghcr.io/acme/api:2.0
  CPU Request: 100m
  Memory Request: 128Mi
  Memory Limit: 256Mi
  Ports: 8080/TCP, 9090/TCP, 53/UDP
  Liveness: http-get http://:8080/healthz delay=5s timeout=1s period=10s #success=1 #failure=3
  Readiness: tcp-socket :6379 delay=0s timeout=1s period=1m30s #success=1 #failure=3
  Startup: exec [cat /tmp/ready] delay=0s timeout=1s period=10s #success=1 #failure=30
  Env From: configmap/app-config, secret/extra (optional)
  Env Refs: DB_PASSWORD=secret/db-creds:password, LOG_LEVEL=configmap/app-config:log-level (optional), POD_IP=field:status.podIP, MEM=resource:limits.memory
  Mounts: /var/cache=cache, /etc/tls=tls (ro)
  Security: runAsNonRoot=true runAsUser=1000 fsGroup=2000 readOnlyRootFilesystem=true allowPrivilegeEscalation=false privileged=false capabilities.drop=ALL capabilities.add=NET_ADMIN,SYS_TIME
Container: proxy:
  Image: envoyproxy/envoy:v1.30.1
  Args: ` + strings.TrimSpace(strings.Repeat("--flag-value ", 16)[:200]) + `…
  Liveness: grpc :9090 delay=0s timeout=1s period=10s #success=1 #failure=3
  Readiness: http-get https://:admin/ready delay=0s timeout=1s period=10s #success=1 #failure=3
  Security: runAsNonRoot=true fsGroup=2000
Volumes:
  cache: emptyDir medium=Memory sizeLimit=1Gi
  tls: secret/api-tls (optional)
  cfg: configmap/app-config
  data: pvc/data-api-0
  host: hostPath /var/log
  store: csi secrets-store.csi.k8s.io
  share: nfs 10.0.0.5:/exports
  scratch: ephemeral
  podinfo: downwardAPI
  bundle: projected
  repo: gitRepo
  tmp: emptyDir
Container Status:
  migrate (init): ready=false  restarts=4
  api: ready=false  restarts=0
  proxy: ready=false  restarts=0
Conditions:
  Initialized: False (ContainersNotInitialized: containers with incomplete status: [migrate])
  PodScheduled: True
`,
	},
	{
		name: "pod minimal", kind: "Pod",
		yaml: `
metadata: {name: web-0, namespace: shop-test}
spec:
  restartPolicy: Always
  containers: [{name: web, image: nginx:1.27}]
  volumes: [{name: kube-api-access-abcde, projected: {sources: []}}]
  tolerations:
  - {key: node.kubernetes.io/not-ready, operator: Exists, effect: NoExecute, tolerationSeconds: 300}
  - {key: node.kubernetes.io/unreachable, operator: Exists, effect: NoExecute, tolerationSeconds: 300}
status: {phase: Running}
`,
		want: `Runtime:
  Phase: Running
Container: web:
  Image: nginx:1.27
`,
	},
	{
		name: "pod evicted", kind: "Pod",
		yaml: `
metadata: {name: report-7c9d4-qq2lm, namespace: jobs}
spec:
  nodeName: worker-2
  containers: [{name: report, image: busybox:1.36}]
status:
  phase: Failed
  reason: Evicted
  message: "The node was low on resource: memory. Threshold quantity: 100Mi, available: 91Mi. Container report was using 1630Mi, request is 0, has larger consumption of memory."
  qosClass: BestEffort
`,
		want: `Runtime:
  Phase: Failed
  Reason: Evicted
  Message: The node was low on resource: memory. Threshold quantity: 100Mi, available: 91Mi. Container report was using 1630Mi, request is 0, has larger consumption of memory.
  Node: worker-2
  QoS Class: BestEffort
Container: report:
  Image: busybox:1.36
`,
	},
	{
		name: "pod job restart policy", kind: "Pod",
		yaml: `
metadata: {name: export-abc12, namespace: jobs}
spec:
  restartPolicy: Never
  containers: [{name: export, image: busybox:1.36, command: [sh, -c, "exit 1"]}]
`,
		want: `Runtime:
  Restart Policy: Never
Container: export:
  Image: busybox:1.36
  Command: sh -c exit 1
`,
	},
	{
		name: "deployment full", kind: "Deployment",
		yaml: `
metadata:
  name: invoice-api
  namespace: billing
  creationTimestamp: "2026-10-16T09:00:00Z"
  labels: {app: invoice-api}
  annotations: {deployment.kubernetes.io/revision: "4"}
spec:
  replicas: 3
  selector:
    matchLabels: {app: invoice-api, tier: backend}
    matchExpressions: [{key: track, operator: NotIn, values: [canary, beta]}, {key: legacy, operator: DoesNotExist}]
  strategy: {type: RollingUpdate}
  template:
    metadata: {labels: {tier: backend, app: invoice-api}}
    spec:
      serviceAccountName: invoice-api
      nodeSelector: {pool: general}
      tolerations: [{key: pool, operator: Equal, value: general, effect: NoSchedule, tolerationSeconds: 120}]
      initContainers: [{name: wait-db, image: busybox:1.36, command: [sh, -c, "until nc -z db 5432; do sleep 2; done"]}]
      containers:
      - name: api
        image: ghcr.io/acme/invoice-api:3.1.0
        ports: [{containerPort: 8080, protocol: TCP}]
        readinessProbe: {httpGet: {path: /ready, port: 8080}, initialDelaySeconds: 10, timeoutSeconds: 2, periodSeconds: 5, successThreshold: 1, failureThreshold: 6}
        envFrom: [{configMapRef: {name: invoice-config}}]
        volumeMounts: [{name: tmp, mountPath: /tmp}]
      volumes: [{name: tmp, emptyDir: {sizeLimit: 512Mi}}]
status:
  readyReplicas: 2
  availableReplicas: 2
  unavailableReplicas: 1
  updatedReplicas: 1
  conditions:
  - {type: Available, status: "True", reason: MinimumReplicasAvailable, message: Deployment has minimum availability.}
  - {type: Progressing, status: "False", reason: ProgressDeadlineExceeded, message: ReplicaSet "invoice-api-7d9f6c" has timed out progressing.}
`,
		want: `Age: 2d
Labels: app=invoice-api
Annotations: deployment.kubernetes.io/revision=4
Replicas:
  Replicas (desired): 3
  Ready: 2
  Available: 2
  Unavailable: 1
  Updated: 1
  Strategy: RollingUpdate
  Selector: app=invoice-api, tier=backend, track notin (canary, beta), !legacy
Pod Template:
  Labels: app=invoice-api, tier=backend
  Service Account: invoice-api
  Node Selector: pool=general
  Tolerations: pool=general:NoSchedule for 120s
Template Init Container: wait-db:
  Image: busybox:1.36
  Command: sh -c until nc -z db 5432; do sleep 2; done
Template Container: api:
  Image: ghcr.io/acme/invoice-api:3.1.0
  Ports: 8080/TCP
  Readiness: http-get http://:8080/ready delay=10s timeout=2s period=5s #success=1 #failure=6
  Env From: configmap/invoice-config
  Mounts: /tmp=tmp
Template Volumes:
  tmp: emptyDir sizeLimit=512Mi
Conditions:
  Available: True (MinimumReplicasAvailable: Deployment has minimum availability.)
  Progressing: False (ProgressDeadlineExceeded: ReplicaSet "invoice-api-7d9f6c" has timed out progressing.)
`,
	},
	{
		name: "deployment minimal", kind: "Deployment",
		yaml: `
metadata: {name: web, namespace: shop-test}
spec:
  replicas: 1
  template:
    spec:
      containers: [{name: web, image: nginx:1.27}]
`,
		want: `Replicas:
  Replicas (desired): 1
  Ready: 0
  Available: 0
  Unavailable: 0
  Updated: 0
Template Container: web:
  Image: nginx:1.27
`,
	},
	{
		name: "replicaset", kind: "ReplicaSet",
		yaml: `
metadata: {name: web-5f7c9, namespace: shop-test, labels: {app: web, pod-template-hash: 5f7c9}}
spec:
  replicas: 2
  selector: {matchLabels: {app: web, pod-template-hash: 5f7c9}}
  template:
    metadata: {labels: {app: web, pod-template-hash: 5f7c9}}
    spec:
      containers: [{name: web, image: nginx:1.27, resources: {limits: {cpu: 500m, memory: 64Mi}}}]
status: {readyReplicas: 1, availableReplicas: 1}
`,
		want: `Labels: app=web pod-template-hash=5f7c9
Replicas:
  Replicas (desired): 2
  Ready: 1
  Available: 1
  Selector: app=web, pod-template-hash=5f7c9
Pod Template:
  Labels: app=web, pod-template-hash=5f7c9
Template Container: web:
  Image: nginx:1.27
  CPU Limit: 500m
  Memory Limit: 64Mi
`,
	},
	{
		name: "statefulset", kind: "StatefulSet",
		yaml: `
metadata: {name: pg, namespace: data}
spec:
  replicas: 3
  serviceName: pg-headless
  selector: {matchLabels: {app: pg}}
  volumeClaimTemplates:
  - metadata: {name: data}
    spec: {storageClassName: standard-rwo, accessModes: [ReadWriteOnce], resources: {requests: {storage: 10Gi}}}
  - metadata: {name: wal}
    spec: {accessModes: [ReadWriteOnce, ReadOnlyMany], resources: {requests: {storage: 2Gi}}}
  template:
    metadata: {labels: {app: pg}}
    spec:
      securityContext: {fsGroup: 999, runAsUser: 999}
      containers:
      - name: postgres
        image: postgres:16.4
        volumeMounts: [{name: data, mountPath: /var/lib/postgresql/data}]
        env: [{name: POSTGRES_PASSWORD, valueFrom: {secretKeyRef: {name: pg-auth, key: password}}}, {name: PGDATA, value: /var/lib/postgresql/data/pgdata}]
status: {readyReplicas: 0, currentReplicas: 1, updatedReplicas: 1}
`,
		want: `Replicas:
  Replicas (desired): 3
  Ready: 0
  Current: 1
  Updated: 1
  Service Name: pg-headless
  Selector: app=pg
  Volume Claim Templates: data (standard-rwo, 10Gi, ReadWriteOnce), wal (2Gi, ReadWriteOnce/ReadOnlyMany)
Pod Template:
  Labels: app=pg
Template Container: postgres:
  Image: postgres:16.4
  Env Refs: POSTGRES_PASSWORD=secret/pg-auth:password
  Mounts: /var/lib/postgresql/data=data
  Security: runAsUser=999 fsGroup=999
`,
	},
	{
		name: "daemonset", kind: "DaemonSet",
		yaml: `
metadata: {name: log-agent, namespace: kube-system}
spec:
  selector: {matchLabels: {name: log-agent}}
  template:
    metadata: {labels: {name: log-agent}}
    spec:
      priorityClassName: system-node-critical
      tolerations: [{operator: Exists, effect: NoSchedule}, {key: node-role.kubernetes.io/control-plane, operator: Exists, effect: NoSchedule}]
      containers:
      - name: agent
        image: fluent/fluent-bit:3.1
        resources: {requests: {memory: 64Mi}, limits: {memory: 128Mi}}
        volumeMounts: [{name: varlog, mountPath: /var/log, readOnly: true}]
      volumes: [{name: varlog, hostPath: {path: /var/log}}]
status: {desiredNumberScheduled: 3, currentNumberScheduled: 3, numberReady: 2, numberAvailable: 2, numberUnavailable: 1}
`,
		want: `Node Schedule:
  Desired: 3
  Current: 3
  Ready: 2
  Available: 2
  Unavailable: 1
  Selector: name=log-agent
Pod Template:
  Labels: name=log-agent
  Tolerations: :NoSchedule, node-role.kubernetes.io/control-plane:NoSchedule
  Priority Class: system-node-critical
Template Container: agent:
  Image: fluent/fluent-bit:3.1
  Memory Request: 64Mi
  Memory Limit: 128Mi
  Mounts: /var/log=varlog (ro)
Template Volumes:
  varlog: hostPath /var/log
`,
	},
	{
		name: "job", kind: "Job",
		yaml: `
metadata: {name: report-28771, namespace: jobs}
spec:
  completions: 1
  backoffLimit: 2
  activeDeadlineSeconds: 600
  template:
    spec:
      restartPolicy: OnFailure
      containers: [{name: report, image: ghcr.io/acme/report:1.0, args: [--date, today]}]
status:
  failed: 3
  startTime: "2026-10-18T07:41:09Z"
  conditions: [{type: Failed, status: "True", reason: BackoffLimitExceeded, message: Job has reached the specified backoff limit}]
`,
		want: `Status:
  Completions: 1
  Active: 0
  Succeeded: 0
  Failed: 3
  Start Time: 2026-10-18T07:41:09Z
  Backoff Limit: 2
  Active Deadline: 600s
Pod Template:
  Restart Policy: OnFailure
Template Container: report:
  Image: ghcr.io/acme/report:1.0
  Args: --date today
Conditions:
  Failed: True (BackoffLimitExceeded: Job has reached the specified backoff limit)
`,
	},
	{
		name: "cronjob", kind: "CronJob",
		yaml: `
metadata: {name: cleanup, namespace: jobs}
spec:
  schedule: "*/15 * * * *"
  suspend: false
  concurrencyPolicy: Forbid
  jobTemplate:
    spec:
      template:
        spec:
          restartPolicy: Never
          serviceAccountName: janitor
          containers: [{name: cleanup, image: bitnami/kubectl:1.34, command: [kubectl, delete, pods, --field-selector=status.phase=Succeeded]}]
status:
  lastScheduleTime: "2026-10-18T10:15:00Z"
  active: [{name: cleanup-28771, namespace: jobs}]
`,
		want: `CronJob:
  Schedule: */15 * * * *
  Suspend: false
  Last Schedule: 2026-10-18T10:15:00Z
  Active Jobs: 1
  Concurrency Policy: Forbid
Pod Template:
  Service Account: janitor
  Restart Policy: Never
Template Container: cleanup:
  Image: bitnami/kubectl:1.34
  Command: kubectl delete pods --field-selector=status.phase=Succeeded
`,
	},
	{
		name: "service", kind: "Service",
		yaml: `
metadata: {name: api, namespace: billing, labels: {app: api}}
spec:
  type: ClusterIP
  clusterIP: 10.96.143.27
  externalIPs: [203.0.113.7, 203.0.113.8]
  ports: [{name: http, port: 80, targetPort: 8080, protocol: TCP}, {port: 9090, targetPort: metrics, protocol: TCP}]
  sessionAffinity: None
  selector: {app: api, tier: web}
`,
		endpoints: "2 ready, 1 not ready: 10.244.1.5:8080, 10.244.2.7:8080, 10.244.3.9:8080",
		want: `Labels: app=api
Service:
  Type: ClusterIP
  Cluster IP: 10.96.143.27
  External IPs: 203.0.113.7, 203.0.113.8
  Ports: http:80→8080/TCP, 9090→metrics/TCP
  Session Affinity: None
  Selector: app=api, tier=web
  Endpoints: 2 ready, 1 not ready: 10.244.1.5:8080, 10.244.2.7:8080, 10.244.3.9:8080
`,
	},
	{
		name: "service load balancer", kind: "Service",
		yaml: `
metadata: {name: edge, namespace: billing}
spec:
  type: LoadBalancer
  clusterIP: 10.96.12.4
  ports: [{name: https, port: 443, targetPort: 8443, protocol: TCP}]
  selector: {app: edge}
status:
  loadBalancer:
    ingress: [{ip: 203.0.113.10}, {hostname: a1b2c3.elb.us-east-1.amazonaws.com}]
`,
		endpoints: "1 ready: 10.244.1.9:8443",
		want: `Service:
  Type: LoadBalancer
  Cluster IP: 10.96.12.4
  Load Balancer: 203.0.113.10, a1b2c3.elb.us-east-1.amazonaws.com
  Ports: https:443→8443/TCP
  Selector: app=edge
  Endpoints: 1 ready: 10.244.1.9:8443
`,
	},
	{
		name: "service load balancer pending", kind: "Service",
		yaml: `
metadata: {name: edge-pending, namespace: billing}
spec:
  type: LoadBalancer
  clusterIP: 10.96.12.5
  externalIPs: [198.51.100.4]
  ports: [{port: 80, targetPort: 8080, protocol: TCP}]
  selector: {app: edge}
status: {loadBalancer: {}}
`,
		endpoints: "none",
		want: `Service:
  Type: LoadBalancer
  Cluster IP: 10.96.12.5
  External IPs: 198.51.100.4
  Ports: 80→8080/TCP
  Selector: app=edge
  Endpoints: none
`,
	},
	{
		name: "service no selector", kind: "Service",
		yaml: `
metadata: {name: external-db, namespace: billing}
spec: {type: ClusterIP, clusterIP: 10.96.0.40, ports: [{port: 5432, targetPort: 5432, protocol: TCP}]}
`,
		endpoints: "none",
		want: `Service:
  Type: ClusterIP
  Cluster IP: 10.96.0.40
  Ports: 5432→5432/TCP
  Selector: none
  Endpoints: none
`,
	},
	{
		name: "pvc pending", kind: "PersistentVolumeClaim",
		yaml: `
metadata: {name: data-pg-0, namespace: data, labels: {app: pg}}
spec: {storageClassName: premium-ssd, accessModes: [ReadWriteOnce], resources: {requests: {storage: 10Gi}}, volumeMode: Filesystem}
status: {phase: Pending}
`,
		want: `Labels: app=pg
Claim:
  Status: Pending
  Storage Class: premium-ssd
  Access Modes: ReadWriteOnce
  Requested: 10Gi
  Volume Mode: Filesystem
`,
	},
	{
		name: "pvc bound", kind: "PersistentVolumeClaim",
		yaml: `
metadata: {name: uploads, namespace: web, annotations: {pv.kubernetes.io/bind-completed: "yes"}}
spec: {storageClassName: efs, accessModes: [ReadWriteMany, ReadOnlyMany], resources: {requests: {storage: 5Gi}}, volumeName: pvc-1f2e, volumeMode: Filesystem}
status: {phase: Bound, capacity: {storage: 5Gi}, accessModes: [ReadWriteMany]}
`,
		want: `Annotations: pv.kubernetes.io/bind-completed=yes
Claim:
  Status: Bound
  Volume: pvc-1f2e
  Storage Class: efs
  Access Modes: ReadWriteMany, ReadOnlyMany
  Requested: 5Gi
  Capacity: 5Gi
  Volume Mode: Filesystem
`,
	},
	{
		name: "pv nfs", kind: "PersistentVolume",
		yaml: `
metadata: {name: archive-nfs}
spec:
  capacity: {storage: 100Gi}
  accessModes: [ReadWriteMany]
  persistentVolumeReclaimPolicy: Retain
  storageClassName: ""
  nfs: {server: 10.0.0.5, path: /exports}
  claimRef: {namespace: reports, name: archive}
status: {phase: Failed, message: "mount failed: exit status 32"}
`,
		want: `Volume:
  Status: Failed
  Claim: reports/archive
  Reclaim Policy: Retain
  Access Modes: ReadWriteMany
  Capacity: 100Gi
  Source: nfs 10.0.0.5:/exports
  Message: mount failed: exit status 32
`,
	},
	{
		name: "pv csi", kind: "PersistentVolume",
		yaml: `
metadata: {name: pvc-0a1b2c}
spec:
  capacity: {storage: 20Gi}
  accessModes: [ReadWriteOnce]
  persistentVolumeReclaimPolicy: Delete
  storageClassName: gp3
  csi: {driver: ebs.csi.aws.com, volumeHandle: vol-0abc123def4567890, fsType: ext4}
  nodeAffinity:
    required:
      nodeSelectorTerms:
      - matchExpressions: [{key: topology.kubernetes.io/zone, operator: In, values: [us-east-1a]}]
status: {phase: Available}
`,
		want: `Volume:
  Status: Available
  Reclaim Policy: Delete
  Storage Class: gp3
  Access Modes: ReadWriteOnce
  Capacity: 20Gi
  Source: csi ebs.csi.aws.com vol-0abc123def4567890
  Node Affinity: topology.kubernetes.io/zone in (us-east-1a)
`,
	},
	{
		name: "node cordoned", kind: "Node",
		yaml: `
metadata: {name: worker-2, labels: {kubernetes.io/hostname: worker-2}}
spec:
  unschedulable: true
  taints:
  - {key: node.kubernetes.io/unschedulable, effect: NoSchedule}
  - {key: dedicated, value: gpu, effect: NoExecute}
status:
  nodeInfo: {kubeletVersion: v1.34.1}
  allocatable: {cpu: "4", memory: 16Gi, pods: "110", ephemeral-storage: 100Gi, hugepages-2Mi: "0", nvidia.com/gpu: "1"}
  conditions:
  - {type: Ready, status: "True", reason: KubeletReady, message: kubelet is posting ready status}
  - {type: MemoryPressure, status: "False", reason: KubeletHasSufficientMemory, message: kubelet has sufficient memory available}
`,
		want: `Labels: kubernetes.io/hostname=worker-2
Node:
  Unschedulable: true
  Taints: node.kubernetes.io/unschedulable:NoSchedule, dedicated=gpu:NoExecute
  Kubelet Version: v1.34.1
  Allocatable: cpu=4, memory=16Gi, pods=110, nvidia.com/gpu=1
Conditions:
  Ready: True (KubeletReady: kubelet is posting ready status)
  MemoryPressure: False (KubeletHasSufficientMemory: kubelet has sufficient memory available)
`,
	},
	{
		name: "node minimal", kind: "Node",
		yaml: `
metadata: {name: worker-1}
spec: {unschedulable: false}
status: {nodeInfo: {kubeletVersion: v1.34.1}}
`,
		want: `Node:
  Kubelet Version: v1.34.1
`,
	},
	{
		name: "hpa", kind: "HorizontalPodAutoscaler",
		yaml: `
metadata: {name: web, namespace: shop-test}
spec:
  scaleTargetRef: {apiVersion: apps/v1, kind: Deployment, name: web}
  minReplicas: 2
  maxReplicas: 10
  metrics:
  - {type: Resource, resource: {name: cpu, target: {type: Utilization, averageUtilization: 70}}}
  - {type: Resource, resource: {name: memory, target: {type: AverageValue, averageValue: 500Mi}}}
  - {type: Pods, pods: {metric: {name: http_requests_per_second}, target: {type: AverageValue, averageValue: "100"}}}
status:
  currentReplicas: 2
  desiredReplicas: 2
  currentMetrics:
  - {type: Resource, resource: {name: cpu, current: {averageUtilization: 85, averageValue: 425m}}}
  - {type: Pods, pods: {metric: {name: http_requests_per_second}, current: {averageValue: "42"}}}
  conditions:
  - {type: AbleToScale, status: "True", reason: ReadyForNewScale, message: recommended size matches current size}
  - {type: ScalingActive, status: "False", reason: FailedGetResourceMetric, message: "the HPA was unable to compute the replica count: failed to get memory utilization"}
`,
		want: `Autoscaler:
  Target: Deployment/web
  Min Replicas: 2
  Max Replicas: 10
  Current Replicas: 2
  Desired Replicas: 2
  Metrics: cpu: 85%/70%, memory: <unknown>/500Mi, http_requests_per_second: 42/100
Conditions:
  AbleToScale: True (ReadyForNewScale: recommended size matches current size)
  ScalingActive: False (FailedGetResourceMetric: the HPA was unable to compute the replica count: failed to get memory utilization)
`,
	},
	{
		name: "ingress", kind: "Ingress",
		yaml: `
metadata: {name: shop, namespace: shop-test, annotations: {cert-manager.io/cluster-issuer: letsencrypt}}
spec:
  ingressClassName: nginx
  defaultBackend: {service: {name: fallback, port: {number: 80}}}
  tls: [{hosts: [shop.example.com, www.shop.example.com], secretName: tls-shop}]
  rules:
  - host: shop.example.com
    http:
      paths:
      - {path: /api, pathType: Prefix, backend: {service: {name: api, port: {number: 8080}}}}
      - {path: /, pathType: Prefix, backend: {service: {name: web, port: {name: http}}}}
  - http:
      paths:
      - {path: /healthz, pathType: Exact, backend: {service: {name: health, port: {number: 8081}}}}
status: {loadBalancer: {ingress: [{ip: 203.0.113.10}, {hostname: lb.example.net}]}}
`,
		want: `Annotations: cert-manager.io/cluster-issuer=letsencrypt
Ingress:
  Class: nginx
  Rules: shop.example.com/api → api:8080, shop.example.com/ → web:http, /healthz → health:8081
  Default Backend: fallback:80
  TLS: tls-shop (shop.example.com, www.shop.example.com)
  Load Balancer: 203.0.113.10, lb.example.net
`,
	},
	{
		name: "ingress minimal", kind: "Ingress",
		yaml: `
metadata: {name: bare, namespace: shop-test}
spec:
  rules: [{host: bare.example.com, http: {paths: [{path: /, pathType: Prefix, backend: {service: {name: bare, port: {number: 80}}}}]}}]
status: {loadBalancer: {}}
`,
		want: `Ingress:
  Rules: bare.example.com/ → bare:80
`,
	},
	{
		name: "configmap", kind: "ConfigMap",
		yaml: `
metadata: {name: app-config, namespace: billing}
data: {log-level: debug, DATABASE_URL: "postgres://user:hunter2@db/app"}
binaryData: {cert.der: AAEC}
`,
		want: `Data:
  Keys: DATABASE_URL, cert.der, log-level
`,
	},
	{
		name: "configmap empty", kind: "ConfigMap",
		yaml: `
metadata: {name: empty, namespace: billing}
`,
		want: ``,
	},
	{
		name: "secret", kind: "Secret",
		yaml: `
metadata: {name: db-creds, namespace: billing}
type: Opaque
data: {password: aHVudGVyMg==, username: YXBw}
`,
		want: `Data:
  Type: Opaque
  Keys: password, username
`,
	},
	{
		name: "serviceaccount irsa", kind: "ServiceAccount",
		yaml: `
metadata:
  name: uploader
  namespace: media
  annotations: {eks.amazonaws.com/role-arn: "arn:aws:iam::111122223333:role/uploader"}
secrets: [{name: uploader-token}]
`,
		want: `Annotations: eks.amazonaws.com/role-arn=arn:aws:iam::111122223333:role/uploader
`,
	},
	{
		name: "certificate", kind: "Certificate",
		yaml: `
metadata: {name: shop-tls, namespace: shop-test}
spec: {secretName: tls-shop, dnsNames: [shop.example.com], issuerRef: {name: letsencrypt, kind: ClusterIssuer}}
status:
  conditions:
  - {type: Ready, status: "False", reason: Failed, message: "The certificate request has failed to complete and will be retried"}
  - {type: Issuing, status: "False"}
`,
		want: `Conditions:
  Ready: False (Failed: The certificate request has failed to complete and will be retried)
  Issuing: False
`,
	},
}

func TestDescribeFixtures(t *testing.T) {
	for _, f := range describeFixtures {
		info := fixtureInfo(t, f)
		if got := describeBody(info); got != f.want {
			t.Errorf("%s:\n--- got\n%s--- want\n%s", f.name, got, f.want)
		}
		want := "Describe " + info.Namespace + "/" + f.kind + "/" + info.Name + ":\n" + f.want
		if got := rcaFormatDescribe(info); got != want {
			t.Errorf("%s: rcaFormatDescribe differs from describeBody", f.name)
		}
		// Nothing secret: literal env values and ConfigMap/Secret values.
		for _, secret := range []string{"s3cr3t-literal", "hunter2", "aHVudGVyMg==", "YXBw", "debug"} {
			if strings.Contains(describeBody(info), secret) {
				t.Errorf("%s: describe shows the value %q", f.name, secret)
			}
		}
		// Rows only when non-empty, no empty sections.
		for _, s := range info.Sections {
			if len(s.Rows) == 0 {
				t.Errorf("%s: empty section %q", f.name, s.Title)
			}
			for _, r := range s.Rows {
				if r.Value == "" {
					t.Errorf("%s: empty row %s/%s", f.name, s.Title, r.Key)
				}
			}
		}
	}
}

// TestDescribeDump writes every fixture's describe text next to its
// ObjectInfo as plain JSON, so external tools that reproduce KubeHealth's
// prompts can be checked against it byte for byte:
//
//	KH_DESCRIBE_DUMP=/tmp/describe.jsonl go test -run TestDescribeDump
func TestDescribeDump(t *testing.T) {
	path := os.Getenv("KH_DESCRIBE_DUMP")
	if path == "" {
		t.Skip("KH_DESCRIBE_DUMP not set")
	}
	out, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	enc := json.NewEncoder(out)
	enc.SetEscapeHTML(false)
	for _, f := range describeFixtures {
		info := fixtureInfo(t, f)
		d := map[string]any{"age": info.Age, "labels": info.Labels, "annotations": info.Annotations}
		var secs []map[string]any
		for _, s := range info.Sections {
			rows := [][]string{}
			for _, r := range s.Rows {
				rows = append(rows, []string{r.Key, r.Value})
			}
			secs = append(secs, map[string]any{"title": s.Title, "rows": rows})
		}
		d["sections"] = secs
		var conds []map[string]string
		for _, c := range info.Conditions {
			conds = append(conds, map[string]string{"type": c.Type, "status": c.Status, "reason": c.Reason, "message": c.Message})
		}
		d["conditions"] = conds
		if err := enc.Encode(map[string]any{"fixture": f.name, "ns": info.Namespace, "kind": info.Kind,
			"name": info.Name, "d": d, "text": rcaFormatDescribe(info)}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestDescribeAnnotationsFiltered(t *testing.T) {
	info := fixtureInfo(t, describeFixtures[0])
	if len(info.Annotations) != 1 || info.Annotations["prometheus.io/scrape"] != "true" {
		t.Errorf("annotations: %v; want only prometheus.io/scrape", info.Annotations)
	}
}

func ptr[T any](v T) *T { return &v }

func TestFormatEndpoints(t *testing.T) {
	slice := func(port int32, eps ...discoveryv1.Endpoint) discoveryv1.EndpointSlice {
		return discoveryv1.EndpointSlice{Ports: []discoveryv1.EndpointPort{{Port: ptr(port)}}, Endpoints: eps}
	}
	ep := func(ip string, ready *bool) discoveryv1.Endpoint {
		return discoveryv1.Endpoint{Addresses: []string{ip}, Conditions: discoveryv1.EndpointConditions{Ready: ready}}
	}
	cases := []struct {
		slices []discoveryv1.EndpointSlice
		want   string
	}{
		{nil, "none"},
		{[]discoveryv1.EndpointSlice{{}}, "none"},
		{[]discoveryv1.EndpointSlice{slice(8080, ep("10.244.2.7", ptr(false)), ep("10.244.1.5", nil)), slice(8080, ep("10.244.3.9", ptr(true)))},
			"2 ready, 1 not ready: 10.244.1.5:8080, 10.244.3.9:8080, 10.244.2.7:8080"},
		{[]discoveryv1.EndpointSlice{slice(5432, ep("10.0.0.1", ptr(false)))}, "0 ready, 1 not ready: 10.0.0.1:5432"},
		{[]discoveryv1.EndpointSlice{slice(80, ep("10.0.0.1", nil), ep("10.0.0.2", nil), ep("10.0.0.3", nil),
			ep("10.0.0.4", nil), ep("10.0.0.5", nil), ep("10.0.0.6", nil), ep("10.0.0.1", nil))},
			"6 ready: 10.0.0.1:80, 10.0.0.2:80, 10.0.0.3:80, 10.0.0.4:80, 10.0.0.5:80"},
		{[]discoveryv1.EndpointSlice{{Endpoints: []discoveryv1.Endpoint{ep("10.0.0.9", nil)}}}, "1 ready: 10.0.0.9"},
	}
	for _, c := range cases {
		if got := formatEndpoints(c.slices); got != c.want {
			t.Errorf("got %q; want %q", got, c.want)
		}
	}
}

// A Service's endpoints come from the EndpointSlices labelled with its name.
func TestServiceEndpoints(t *testing.T) {
	mk := func(name, svc, ip string) *discoveryv1.EndpointSlice {
		return &discoveryv1.EndpointSlice{
			ObjectMeta:  metav1.ObjectMeta{Name: name, Namespace: "billing", Labels: map[string]string{discoveryv1.LabelServiceName: svc}},
			AddressType: discoveryv1.AddressTypeIPv4,
			Ports:       []discoveryv1.EndpointPort{{Port: ptr(int32(8080))}},
			Endpoints:   []discoveryv1.Endpoint{{Addresses: []string{ip}}},
		}
	}
	c := testCluster()
	c.client = fake.NewSimpleClientset(mk("api-abc", "api", "10.244.1.5"), mk("other-xyz", "other", "10.244.9.9"))
	if got := c.serviceEndpoints("billing", "api"); got != "1 ready: 10.244.1.5:8080" {
		t.Errorf("api: %q", got)
	}
	if got := c.serviceEndpoints("billing", "missing"); got != "none" {
		t.Errorf("no slices: %q", got)
	}
}
