import { actionDefinition, actionFieldKey, type ActionDefinition, type ActionField, type TypedActionId } from "./actionDefinitions";

export interface ActionDraft {
  actionId: TypedActionId;
  resource: string;
  values: Record<string, string>;
}
type Scalar = string | number | boolean;
export type SafeActionParameters = Record<string, Scalar | Record<string, Scalar>>;

export function initialActionDraft(actionId: TypedActionId, resource = ""): ActionDraft {
  const action = actionDefinition(actionId)!;
  return {
    actionId, resource,
    values: Object.fromEntries(action.fields.filter((field) => !field.patch && field.initial !== undefined)
      .map((field) => [actionFieldKey(field), field.initial!])),
  };
}

/** A UI value builder only. Current server facts still decide every operation. */
export function buildActionParameters(draft: ActionDraft): SafeActionParameters | undefined {
  const action = actionDefinition(draft.actionId);
  if (!action) return undefined;
  const keys = new Set(action.fields.map(actionFieldKey));
  if (Object.keys(draft.values).some((key) => !keys.has(key))) return undefined;
  const parameters: SafeActionParameters = {};
  const patch: Record<string, Scalar> = {};
  for (const field of action.fields) {
    const value = draft.values[actionFieldKey(field)];
    if (field.patch && value === undefined) continue;
    const parsed = parseField(field, value);
    if (parsed === undefined) return undefined;
    if (field.patch) patch[field.key] = parsed;
    else parameters[field.key] = parsed;
  }
  if (action.fields.some((field) => field.patch)) {
    if (!Object.keys(patch).length) return undefined;
    parameters.patch = patch;
  }
  return validSpecial(action.id, parameters) ? parameters : undefined;
}

export function validActionResource(value: string) {
  return value.length > 0 && value.length <= 512 && !/[\s*?]/u.test(value) && !value.includes("://") &&
    [...value].every((char) => char.charCodeAt(0) >= 32 && char.charCodeAt(0) !== 127);
}

/** Reject unknown fields/versions instead of printing unreviewed JSON values. */
export function reviewedActionParameters(action: ActionDefinition, value: unknown): SafeActionParameters | undefined {
  if (!record(value)) return undefined;
  const outer = new Set(action.fields.filter((field) => !field.patch).map((field) => field.key));
  const patchFields = action.fields.filter((field) => field.patch);
  if (patchFields.length) outer.add("patch");
  if (Object.keys(value).some((key) => !outer.has(key))) return undefined;
  if (patchFields.length && (!record(value.patch) || Object.keys(value.patch).some((key) => !patchFields.some((field) => field.key === key)))) return undefined;
  const values: Record<string, string> = {};
  for (const field of action.fields) {
    const raw = field.patch ? (record(value.patch) ? value.patch[field.key] : undefined) : value[field.key];
    if (field.patch && raw === undefined) continue;
    if (field.kind === "integer" ? typeof raw !== "number" : field.patch && field.key === "order" ? typeof raw !== "boolean" : typeof raw !== "string") return undefined;
    values[actionFieldKey(field)] = String(raw);
  }
  return buildActionParameters({ actionId: action.id, resource: "", values });
}

function parseField(field: ActionField, value: string | undefined): Scalar | undefined {
  if (value === undefined || !value.length || value.trim() !== value) return undefined;
  if (field.fixed && value !== field.initial) return undefined;
  if (field.kind === "integer") {
    const number = /^\d+$/u.test(value) ? Number(value) : NaN;
    return Number.isSafeInteger(number) && number >= field.minimum! && number <= field.maximum! ? number : undefined;
  }
  if (field.kind === "choice") {
    if (!field.choices?.includes(value)) return undefined;
    return field.patch && field.key === "order" ? value === "true" : value;
  }
  if (value.length > field.maximum! || ![...value].every((char) => /[A-Za-z0-9_.:/%@+-]/u.test(char) || char === "[" || char === "]")) return undefined;
  if (field.kind === "reference" && (!/^(?:vault|kubernetes):\/\/[A-Za-z0-9_./:-]+$/u.test(value) || value.endsWith("/"))) return undefined;
  return value;
}

function validSpecial(id: TypedActionId, values: SafeActionParameters) {
  if (id === "observability.logger_level_ttl.v1" &&
    (!String(values.logger).startsWith("rocketmq_broker::") ||
      ["message", "body", "security", "auth", "acl", "secret", "payload", "store"].some((word) => String(values.logger).toLowerCase().includes(word)))) return false;
  if (id === "proxy.rollout_image_canary.v1" && !/^sha256:[a-f0-9]{64}$/u.test(String(values.image_digest))) return false;
  if (id === "security.credential_rotate_overlap.v1" &&
    (values.active_version === values.candidate_version || !String(values.validation_probe_topic).startsWith("SRE_PROBE_"))) return false;
  return true;
}

function record(value: unknown): value is Record<string, unknown> {
  return Boolean(value && typeof value === "object" && !Array.isArray(value));
}
