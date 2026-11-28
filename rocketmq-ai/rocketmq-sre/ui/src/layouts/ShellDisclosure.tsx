import { useEffect, useRef, type ReactNode } from "react";

interface DisclosureProps {
  className: string;
  label: string;
  summary: ReactNode;
  children: ReactNode;
}

export function ShellDisclosure({ className, label, summary, children }: DisclosureProps) {
  const details = useRef<HTMLDetailsElement>(null);
  useEffect(() => {
    const closeOutside = (event: PointerEvent) => {
      const panel = details.current;
      if (panel?.open && event.target instanceof Node && !panel.contains(event.target))
        panel.open = false;
    };
    document.addEventListener("pointerdown", closeOutside);
    return () => document.removeEventListener("pointerdown", closeOutside);
  }, []);
  return (
    <details
      className={`shell-disclosure ${className}`}
      name="workspace-controls"
      ref={details}
      onBlur={(event) => {
        if (
          event.relatedTarget instanceof Node &&
          !event.currentTarget.contains(event.relatedTarget)
        ) {
          event.currentTarget.open = false;
        }
      }}
      onKeyDown={(event) => {
        if (event.key === "Escape" && event.currentTarget.open) {
          event.preventDefault();
          event.currentTarget.open = false;
          event.currentTarget.querySelector("summary")?.focus();
        }
      }}
    >
      <summary aria-label={label} title={label}>
        {summary}
      </summary>
      <div className="shell-disclosure-body">{children}</div>
    </details>
  );
}
