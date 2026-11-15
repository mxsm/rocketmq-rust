import { Input } from "@/components/ui/input";
import {
  Select, SelectContent, SelectItem, SelectTrigger, SelectValue,
} from "@/components/ui/select";
import { useActionReadinessMessages } from "@/i18n/actionReadinessMessages";

export interface LoggerActionFields {
  resource: string;
  logger: string;
  level: "DEBUG" | "INFO";
  ttlSeconds: number;
}

export function LoggerActionForm({
  value,
  onChange,
}: {
  value: LoggerActionFields;
  onChange: (change: Partial<LoggerActionFields>) => void;
}) {
  const m = useActionReadinessMessages();
  return (
    <div className="action-candidate-form">
      <label className="form-field">
        <span>{m.target}</span>
        <Input
          maxLength={512}
          onChange={(event) => onChange({ resource: event.target.value })}
          placeholder={m.targetPlaceholder}
          value={value.resource}
        />
      </label>
      <label className="form-field">
        <span>{m.logger}</span>
        <Input
          maxLength={128}
          onChange={(event) => onChange({ logger: event.target.value })}
          value={value.logger}
        />
      </label>
      <label className="form-field">
        <span>{m.level}</span>
        <Select
          onValueChange={(level: LoggerActionFields["level"]) => onChange({ level })}
          value={value.level}
        >
          <SelectTrigger><SelectValue /></SelectTrigger>
          <SelectContent>
            <SelectItem value="DEBUG">DEBUG</SelectItem>
            <SelectItem value="INFO">INFO</SelectItem>
          </SelectContent>
        </Select>
      </label>
      <label className="form-field">
        <span>{m.ttl}</span>
        <Input
          max={300}
          min={60}
          onChange={(event) => onChange({ ttlSeconds: Number(event.target.value) })}
          type="number"
          value={value.ttlSeconds}
        />
      </label>
    </div>
  );
}
