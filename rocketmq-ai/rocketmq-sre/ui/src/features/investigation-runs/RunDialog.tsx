import { useEffect, useRef, type ReactNode } from "react";
import { createPortal } from "react-dom";
import { X } from "lucide-react";

import { Button } from "@/components/ui/button";
import { useRunMessages } from "@/i18n/runMessages";

export function RunDialog({
  title,
  children,
  onClose,
}: {
  title: string;
  children: ReactNode;
  onClose: () => void;
}) {
  const panel = useRef<HTMLDivElement>(null);
  const m = useRunMessages();
  const close = useRef(onClose);
  useEffect(() => {
    close.current = onClose;
  }, [onClose]);
  useEffect(() => {
    const previous =
      document.activeElement instanceof HTMLElement
        ? document.activeElement
        : undefined;
    const overflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    const focusable = () => [
      ...(panel.current?.querySelectorAll<HTMLElement>(
        'button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex="0"]',
      ) ?? []),
    ];
    (focusable()[0] ?? panel.current)?.focus();
    const keydown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        event.preventDefault();
        close.current();
      }
      if (event.key !== "Tab") return;
      const elements = focusable();
      const first = elements[0];
      const last = elements.at(-1);
      if (
        !first ||
        (event.shiftKey &&
          (document.activeElement === first ||
            !panel.current?.contains(document.activeElement)))
      ) {
        event.preventDefault();
        (last ?? panel.current)?.focus();
      } else if (
        !event.shiftKey &&
        (document.activeElement === last ||
          !panel.current?.contains(document.activeElement))
      ) {
        event.preventDefault();
        first.focus();
      }
    };
    const focusin = (event: FocusEvent) => {
      if (!panel.current?.contains(event.target as Node))
        (focusable()[0] ?? panel.current)?.focus();
    };
    document.addEventListener("keydown", keydown);
    document.addEventListener("focusin", focusin);
    return () => {
      document.body.style.overflow = overflow;
      document.removeEventListener("keydown", keydown);
      document.removeEventListener("focusin", focusin);
      if (previous?.isConnected) previous.focus();
    };
  }, []);
  return createPortal(
    <div className="run-dialog-overlay">
      <div
        ref={panel}
        role="dialog"
        aria-modal="true"
        aria-label={title}
        tabIndex={-1}
        className="run-dialog"
      >
        <div className="run-dialog-heading">
          <h2>{title}</h2>
          <Button
            aria-label={m.close}
            variant="ghost"
            size="sm"
            onClick={onClose}
          >
            <X size={16} />
          </Button>
        </div>
        {children}
      </div>
    </div>,
    document.body,
  );
}
