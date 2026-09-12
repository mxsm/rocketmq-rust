import type { DlqMessageExportPayload } from './types/dlq.types';

export function downloadDlqCsv(payload: DlqMessageExportPayload) {
    const url = URL.createObjectURL(new Blob([payload.content], { type: payload.mimeType }));
    const link = document.createElement('a');
    link.href = url;
    link.download = payload.fileName;
    document.body.appendChild(link);
    try { link.click(); }
    finally { link.remove(); window.setTimeout(() => URL.revokeObjectURL(url), 1_000); }
}
