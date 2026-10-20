import http from "./http";

export interface LoginRequest {
  username: string;
  password: string;
}

export interface Principal {
  username: string;
  displayName: string;
  role: "ADMIN" | "OPERATOR" | "VIEWER";
  permissions: string[];
}
export interface LoginResponse {
  token: string;
  expiresAtMillis: number;
  principal: Principal;
}

export interface ManagementPage<T> {
  items: T[];
  offset: number;
  limit: number;
  total: number;
}

export interface ManagementOverview {
  partial: boolean;
  namespace: string;
  streamCount: number;
  streamLength: number;
  groupCount: number;
  pendingCount: number;
  taskCount: number;
  taskFailureCount: number;
  deadLetterCount: number;
}

export interface PressureSnapshot {
  paused: boolean;
  cpuLoad: number;
  databaseUsage: number;
  sampledAtMillis: number;
  reasons: string[];
}

export interface DeadLetterAlert {
  namespace: string;
  level: "OK" | "WARNING" | "CRITICAL" | "UNKNOWN";
  retainedCount: number;
  recentCount: number;
  oldestAtMillis: number;
  checkedAtMillis: number;
  message: string;
}

export interface RuntimeInstance {
  instanceId: string;
  runId: string;
  application: string;
  startedAtMillis: number;
  lastHeartbeatAtMillis: number;
  leaseMillis: number;
  queueConcurrency: number;
  scheduleConcurrency: number;
  generation: number;
  draining: boolean;
  components: RuntimeComponent[];
  pressure: PressureSnapshot;
}

export interface RuntimeComponent {
  kind: string;
  id: string;
  destination: string;
  group: string;
  definition: string;
  capacity: number;
  active: number;
  buffered: number;
  bufferCapacity: number;
}

export interface JobExecution {
  executionId: string;
  jobId: string;
  scheduledAt: number;
  attempt: number;
  status: string;
  instanceId: string;
  runId: string;
  updatedAt: number;
  retryAt: number;
  detail: string;
}

export interface StreamConsumer {
  name: string;
  pending: number;
  idleTimeMillis: number;
  inactiveMillis: number;
}

export interface StreamGroup {
  name: string;
  consumers: number;
  pending: number;
  lag: number;
  lastDeliveredId: string;
  pendingTotal: number;
  pendingLowestId: string;
  pendingHighestId: string;
  consumerViews: StreamConsumer[];
}

export interface StreamView {
  destination: string;
  displayName?: string;
  redisKey: string;
  length: number;
  groups: StreamGroup[];
  lastGeneratedId: string;
}

export interface StreamMessageView {
  id: string;
  fields: Record<string, string>;
}

export interface TaskResult {
  executionId: string;
  runId: string;
  attempt: number;
  status: string;
  taskId: string;
  scheduledAt: string;
  startedAt: string;
  completedAt: string;
  instanceId: string;
  successful: boolean;
  detail: string;
}

export interface TaskView {
  taskId: string;
  displayName?: string;
  redisKey: string;
  definition: string;
  nextAtMillis: number;
  paused: boolean;
  executionStatus: string;
  executionId: string;
  route: string;
}

export interface DeadLetterView {
  mode: "STREAM" | "BROADCAST" | "RELIABLE_BROADCAST";
  instanceId: string;
  originalMessageId: string;
  firstFailedAt: string;

  destination: string;
  displayName?: string;
  group: string;
  messageId: string;
  payload: string;
  createdAt: string;
  attempts: number;
  error: string;
  failedAt: string;
}

export interface ManagementCommandResponse {
  changed: boolean;
}

export const managementApi = {
  clearCache(namespace: string, role: "RUNTIME" | "CACHE") {
    return http.post<{ deleted: number }>("/cache/clear", undefined, { params: { namespace, role, confirmed: true } });
  },
  me: () => http.get<Principal>("/auth/me"),
  logout: () => http.post("/auth/logout"),
  deadLetterAlert(namespace: string) {
    return http.get<DeadLetterAlert>("/alerts/dead-letters", {
      params: { namespace }
    });
  },
  attempts(namespace: string, taskId: string, offset = 0, limit = 50) {
    return http.get<TaskResult[]>(`/tasks/${encodeURIComponent(taskId)}/attempts`, {
      params: { namespace, offset, limit }
    });
  },
  executions(namespace: string, taskId: string, offset = 0, limit = 50) {
    return http.get<JobExecution[]>(`/tasks/${encodeURIComponent(taskId)}/executions`, {
      params: { namespace, offset, limit }
    });
  },
  pauseTask(namespace: string, taskId: string, paused: boolean) {
    return http.post<ManagementCommandResponse>(`/tasks/${encodeURIComponent(taskId)}/paused`, undefined, {
      params: { namespace, paused }
    });
  },
  retryExecution(namespace: string, taskId: string, executionId: string) {
    return http.post<ManagementCommandResponse>(
      `/tasks/${encodeURIComponent(taskId)}/executions/${encodeURIComponent(executionId)}/retry`,
      undefined,
      { params: { namespace } }
    );
  },
  drainInstance(namespace: string, instanceId: string, draining: boolean) {
    return http.post<ManagementCommandResponse>(`/instances/${encodeURIComponent(instanceId)}/draining`, undefined, {
      params: { namespace, draining }
    });
  },
  login(data: LoginRequest) {
    return http.post<LoginResponse>("/auth/login", data);
  },
  namespaces() {
    return http.get<string[]>("/namespaces");
  },
  overview(namespace: string) {
    return http.get<ManagementOverview>("/overview", { params: { namespace } });
  },
  instances(namespace: string) {
    return http.get<RuntimeInstance[]>("/instances", { params: { namespace } });
  },
  streams(namespace: string, offset = 0, limit = 50) {
    return http.get<ManagementPage<StreamView>>("/streams", {
      params: { namespace, offset, limit }
    });
  },
  stream(namespace: string, destination: string) {
    return http.get<StreamView>(`/streams/${encodeURIComponent(destination)}`, {
      params: { namespace }
    });
  },
  streamMessages(namespace: string, destination: string, limit = 50) {
    return http.get<StreamMessageView[]>(`/streams/${encodeURIComponent(destination)}/messages`, {
      params: { namespace, limit }
    });
  },
  deleteStreamMessage(namespace: string, destination: string, messageId: string) {
    return http.delete<ManagementCommandResponse>(
      `/streams/${encodeURIComponent(destination)}/messages/${encodeURIComponent(messageId)}`,
      { params: { namespace } }
    );
  },
  tasks(namespace: string, offset = 0, limit = 50) {
    return http.get<ManagementPage<TaskView>>("/tasks", {
      params: { namespace, offset, limit }
    });
  },
  triggerTask(namespace: string, taskId: string) {
    return http.post<ManagementCommandResponse>(`/tasks/${encodeURIComponent(taskId)}/trigger`, undefined, {
      params: { namespace }
    });
  },
  deleteTaskResults(namespace: string, taskId: string) {
    return http.delete<ManagementCommandResponse>(`/tasks/${encodeURIComponent(taskId)}/results`, {
      params: { namespace }
    });
  },
  deadLetters(namespace: string, offset = 0, limit = 50) {
    return http.get<ManagementPage<DeadLetterView>>("/dead-letters", {
      params: { namespace, offset, limit }
    });
  },
  deleteDeadLetter(namespace: string, destination: string, group: string, messageId: string, broadcastInstance = "") {
    return http.delete<ManagementCommandResponse>(
      `/dead-letters/${encodeURIComponent(destination)}/groups/${encodeURIComponent(group)}/messages/${encodeURIComponent(messageId)}`,
      { params: { namespace, broadcastInstance } }
    );
  },
  retryDeadLetter(namespace: string, destination: string, group: string, messageId: string, broadcastInstance = "") {
    return http.post<ManagementCommandResponse>(
      `/dead-letters/${encodeURIComponent(destination)}/groups/${encodeURIComponent(group)}/messages/${encodeURIComponent(messageId)}/retry`,
      undefined,
      { params: { namespace, broadcastInstance } }
    );
  }
};
