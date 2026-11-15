/** Closed UI models for existing wired handlers. The server revalidates operation admission. */
export type ActionText = readonly [zh: string, en: string];
export interface ActionField {
  key: string;
  label: ActionText;
  kind: "text" | "integer" | "choice" | "reference";
  maximum?: number;
  minimum?: number;
  choices?: readonly string[];
  initial?: string;
  fixed?: boolean;
  patch?: boolean;
}

const text = (key: string, zh: string, en: string, maximum = 128): ActionField =>
  ({ key, label: [zh, en], kind: "text", maximum });
const integer = (key: string, zh: string, en: string, minimum: number, maximum = Number.MAX_SAFE_INTEGER): ActionField =>
  ({ key, label: [zh, en], kind: "integer", minimum, maximum });
const choice = (key: string, zh: string, en: string, choices: readonly string[], initial = choices[0]): ActionField =>
  ({ key, label: [zh, en], kind: "choice", choices, initial });
const patch = (field: ActionField): ActionField => ({ ...field, patch: true });
const namespace = () => text("namespace", "命名空间", "Namespace");
const pod = () => text("pod", "Pod", "Pod");
const uid = () => text("expected_uid", "预期 Pod UID", "Expected Pod UID");
const workload = () => text("workload", "工作负载", "Workload");
const generation = () => integer("expected_generation", "预期配置代次", "Expected generation", 1);
const version = () => integer("expected_version", "预期版本", "Expected version", 0);

export const ACTION_IDS = [
  "observability.logger_level_ttl.v1", "broker.config.patch_allowlisted.v1",
  "topic.config.patch_allowlisted.v1", "subscription_group.patch_allowlisted.v1",
  "proxy.scale_out_one.v1", "proxy.restart_one.v1", "proxy.rollout_image_canary.v1",
  "telemetry.collector.restart_one.v1", "security.credential_rotate_overlap.v1",
] as const;
export type TypedActionId = typeof ACTION_IDS[number];
export interface ActionDefinition {
  id: TypedActionId;
  version: "1.0.0";
  title: ActionText;
  intent: ActionText;
  fields: readonly ActionField[];
}

export const ACTIONS: readonly ActionDefinition[] = [
  {
    id: ACTION_IDS[0], version: "1.0.0", title: ["临时日志级别", "Temporary logger level"],
    intent: ["指定 Logger 临时调整，TTL 后恢复；不读取消息内容。", "Temporarily adjust one Logger, then restore after its TTL; message content is excluded."],
    fields: [
      { ...choice("component", "组件", "Component", ["broker"]), fixed: true },
      { ...text("logger", "Logger", "Logger"), initial: "rocketmq_broker::processor" },
      choice("level", "日志级别", "Log level", ["DEBUG", "INFO"]),
      { ...integer("ttl_seconds", "有效秒数", "Duration in seconds", 60, 900), initial: "60" },
    ],
  },
  {
    id: ACTION_IDS[1], version: "1.0.0", title: ["Broker 允许项配置", "Allowlisted Broker configuration"],
    intent: ["仅提交显式选中的配置项，并绑定预期代次。", "Request only explicitly selected fields, bound to the expected generation."],
    fields: [text("broker", "Broker 地址选择器", "Broker address selector"), generation(),
      patch(integer("send_message_thread_pool_nums", "发送线程数", "Send thread count", 1, 512)),
      patch(integer("pull_message_thread_pool_nums", "拉取线程数", "Pull thread count", 1, 512)),
      patch(integer("flush_delay_offset_interval_ms", "延迟位点刷新间隔（ms）", "Delay-offset flush interval (ms)", 1000, 60000)),
      patch(integer("max_client_event_count", "客户端事件数上限", "Maximum client event count", 1, 10000))],
  },
  {
    id: ACTION_IDS[2], version: "1.0.0", title: ["Topic 允许项配置", "Allowlisted Topic configuration"],
    intent: ["只涉及选定 Topic 的允许项，保留版本前置条件。", "Change allowlisted fields of the selected Topic with an explicit version precondition."],
    fields: [text("topic", "Topic", "Topic", 255), version(),
      patch(integer("read_queue_nums", "读队列数", "Read queue count", 1, 128)),
      patch(integer("write_queue_nums", "写队列数", "Write queue count", 1, 128)),
      patch(choice("order", "顺序 Topic", "Ordered Topic", ["true", "false"]))],
  },
  {
    id: ACTION_IDS[3], version: "1.0.0", title: ["订阅组允许项配置", "Allowlisted subscription-group configuration"],
    intent: ["只调整选定订阅组的重试和超时允许项。", "Adjust only allowlisted retry and timeout settings for one subscription group."],
    fields: [text("group", "订阅组", "Subscription group", 255), version(),
      patch(integer("retry_max_times", "最大重试次数", "Maximum retries", 1, 16)),
      patch(integer("retry_queue_nums", "重试队列数", "Retry queue count", 1, 8)),
      patch(integer("consume_timeout_minutes", "消费超时（分钟）", "Consume timeout (minutes)", 1, 1440))],
  },
  {
    id: ACTION_IDS[4], version: "1.0.0", title: ["Proxy 扩一个副本", "Add one Proxy replica"],
    intent: ["预期副本数核对通过后仅增加一个；不提供缩容输入。", "Request exactly one additional replica after checking the expected count; scale-in is excluded."],
    fields: [namespace(), workload(), integer("expected_replicas", "预期副本数", "Expected replicas", 1, 999)],
  },
  {
    id: ACTION_IDS[5], version: "1.0.0", title: ["Proxy 单实例重启", "Restart one Proxy instance"],
    intent: ["绑定命名空间、Pod 和 UID；排空与健康条件由服务端检查。", "Bind one namespace, Pod and UID; the server checks drain and health conditions."],
    fields: [namespace(), pod(), uid()],
  },
  {
    id: ACTION_IDS[6], version: "1.0.0", title: ["Proxy 单副本金丝雀镜像", "One-replica Proxy image canary"],
    intent: ["仅固定摘要镜像和一个金丝雀副本，不接受 tag、命令或环境补丁。", "Use a digest-pinned image and exactly one canary; tags, commands and environment patches are excluded."],
    fields: [namespace(), workload(), text("container", "容器", "Container"), generation(),
      text("image_digest", "镜像摘要（sha256）", "Image digest (sha256)"),
      { ...integer("canary_replicas", "金丝雀副本数", "Canary replicas", 1, 1), initial: "1", fixed: true }],
  },
  {
    id: ACTION_IDS[7], version: "1.0.0", title: ["遥测采集器单实例重启", "Restart one telemetry collector"],
    intent: ["只重启声明的采集器实例，不修改流水线配置或凭据。", "Restart the declared collector instance without changing pipeline configuration or credentials."],
    fields: [namespace(), pod(), uid(), choice("pipeline", "遥测流水线", "Telemetry pipeline", ["metrics", "logs", "traces", "combined"])],
  },
  {
    id: ACTION_IDS[8], version: "1.0.0", title: ["凭据重叠轮换", "Overlapping credential rotation"],
    intent: ["仅接收已有受管凭据引用；保持旧版本重叠，不输入或显示密钥值。", "Use an existing managed secret reference, retain the overlap, and never enter or display secret values."],
    fields: [text("credential_set", "凭据集合标识", "Credential-set identifier"),
      text("active_version", "当前版本标识", "Active version identifier"),
      text("candidate_version", "候选版本标识", "Candidate version identifier"),
      { ...text("candidate_secret_ref", "受管凭据引用", "Managed secret reference", 255), kind: "reference" },
      integer("overlap_seconds", "重叠秒数", "Overlap seconds", 60, 3600),
      text("validation_probe_topic", "专用探测 Topic（SRE_PROBE_）", "Dedicated probe Topic (SRE_PROBE_)", 255)],
  },
];

export const actionText = (text: ActionText, english: boolean) => text[english ? 1 : 0];
export const actionFieldKey = (field: ActionField) => `${field.patch ? "patch." : ""}${field.key}`;
export const actionDefinition = (id: string, version = "1.0.0") =>
  ACTIONS.find((action) => action.id === id && action.version === version);
