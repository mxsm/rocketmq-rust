import { useRef } from 'react';

// Entity details also open from ordinary buttons after an asynchronous request.
export function useDialogFocusReturn(
    onOpenAutoFocus?: (event: Event) => void,
    onCloseAutoFocus?: (event: Event) => void,
) {
    const returnTarget = useRef<HTMLElement | null>(null);
    return {
        onOpenAutoFocus(event: Event) {
            const active = document.activeElement;
            returnTarget.current = active instanceof HTMLElement && active !== document.body ? active : null;
            onOpenAutoFocus?.(event);
        },
        onCloseAutoFocus(event: Event) {
            onCloseAutoFocus?.(event);
            if (!event.defaultPrevented && returnTarget.current?.isConnected) {
                event.preventDefault();
                returnTarget.current.focus();
            }
            returnTarget.current = null;
        },
    };
}
