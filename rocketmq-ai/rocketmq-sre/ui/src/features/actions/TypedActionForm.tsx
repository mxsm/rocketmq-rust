import { Input } from "@/components/ui/input";
import { useActionFormMessages } from "@/i18n/actionFormMessages";

import { ACTIONS, actionDefinition, actionFieldKey, actionText, type TypedActionId } from "./actionDefinitions";
import { buildActionParameters, validActionResource, type ActionDraft } from "./actionParameters";

export function TypedActionForm({ value, onChange, onActionChange }: {
  value: ActionDraft;
  onChange: (change: Partial<ActionDraft>) => void;
  onActionChange: (action: TypedActionId) => void;
}) {
  const { m, english } = useActionFormMessages();
  const action = actionDefinition(value.actionId)!;
  const fieldChange = (key: string, fieldValue: string | undefined) => {
    const values = { ...value.values };
    if (fieldValue === undefined) delete values[key];
    else values[key] = fieldValue;
    onChange({ values });
  };
  return <>
    <p className="muted">{m.modelHint}</p>
    <div className="action-candidate-form">
      <label className="form-field action-type-selector">
        <span>{m.actionType}</span>
        <select value={value.actionId} onChange={(event) => {
          const next = ACTIONS.find((item) => item.id === event.target.value);
          if (next) onActionChange(next.id);
        }}>
          {ACTIONS.map((item) => <option key={item.id} value={item.id}>{actionText(item.title, english)}</option>)}
        </select>
      </label>
      <label className="form-field">
        <span>{action.id === ACTIONS[0].id ? m.loggerTarget : m.target}</span>
        <Input maxLength={512} value={value.resource} onChange={(event) => onChange({ resource: event.target.value })} />
      </label>
      {action.fields.map((field) => {
        const key = actionFieldKey(field);
        const label = actionText(field.label, english);
        const enabled = !field.patch || value.values[key] !== undefined;
        return <div className="typed-action-field" key={key}>
          {field.patch && <label className="typed-action-include">
            <input type="checkbox" checked={enabled} onChange={(event) =>
              fieldChange(key, event.target.checked ? field.initial ?? "" : undefined)} />
            {m.include} {label}
          </label>}
          <label className="form-field">
            <span>{label}</span>
            {field.kind === "choice" && !field.fixed ? <select value={value.values[key] ?? ""} disabled={!enabled}
              onChange={(event) => fieldChange(key, event.target.value)}>
              {field.choices!.map((choice) => <option key={choice} value={choice}>{choice}</option>)}
            </select> : <Input
              autoComplete={field.kind === "reference" ? "off" : undefined}
              type={field.kind === "integer" ? "number" : field.kind === "reference" ? "password" : "text"}
              min={field.minimum} max={field.kind === "integer" ? field.maximum : undefined}
              maxLength={field.kind !== "integer" ? field.maximum : undefined}
              readOnly={field.fixed} disabled={!enabled} value={value.values[key] ?? ""}
              onChange={(event) => fieldChange(key, event.target.value)}
            />}
          </label>
          {field.kind === "reference" && <small className="muted">{m.referenceHint}</small>}
        </div>;
      })}
    </div>
    <p className="typed-action-intent">{actionText(action.intent, english)} · {action.id} / {action.version}</p>
    {(!buildActionParameters(value) || !validActionResource(value.resource.trim())) && <p className="muted">{m.invalid}</p>}
  </>;
}
