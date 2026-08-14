import type { ReactNode, RefObject } from 'react';
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle } from './ui/Sheet';

interface EntitySheetProps {
  open: boolean;
  title: string;
  description?: string;
  onOpenChange: (open: boolean) => void;
  restoreFocusRef?: RefObject<HTMLElement | null>;
  children: ReactNode;
}

export default function EntitySheet({ open, title, description, onOpenChange, restoreFocusRef, children }: EntitySheetProps) {
  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent
        className="entity-sheet"
        onCloseAutoFocus={(event) => {
          if (!restoreFocusRef?.current) return;
          event.preventDefault();
          restoreFocusRef.current.focus();
        }}
      >
        <SheetHeader className="entity-sheet-header">
          <SheetTitle>{title}</SheetTitle>
          {description ? <SheetDescription>{description}</SheetDescription> : null}
        </SheetHeader>
        <div className="entity-sheet-body">{children}</div>
      </SheetContent>
    </Sheet>
  );
}
