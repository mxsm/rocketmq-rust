const units = ['bytes', 'KB', 'MB', 'GB', 'TB', 'PB'];
const number = new Intl.NumberFormat(undefined, { maximumFractionDigits: 2 });

export function diagnosticBytes(value: number | null | undefined): { display: string; exact: string | undefined } {
    if (value == null) return { display: 'Not measured', exact: undefined };
    if (!Number.isSafeInteger(value) || value < 0) return { display: 'Unknown / unavailable', exact: undefined };
    const unit = value === 0 ? 0 : Math.min(Math.floor(Math.log10(value) / 3), units.length - 1);
    return { display: `${number.format(value / 1000 ** unit)} ${units[unit]}`, exact: `${value.toLocaleString()} bytes` };
}

export function diagnosticTime(value: number | null | undefined): string {
    if (value == null) return 'Not observed';
    const date = new Date(value);
    return Number.isFinite(value) && Number.isFinite(date.getTime()) ? date.toLocaleString() : 'Unknown / unavailable';
}

export function observationAge(checkedAt: number | null | undefined, now: number): 'current' | 'stale' | 'unknown' {
    if (checkedAt == null || !Number.isFinite(checkedAt) || !Number.isFinite(now) || checkedAt > now) return 'unknown';
    return now - checkedAt >= 60_000 ? 'stale' : 'current';
}

export const diagnosticCount = (value: number | null | undefined, unit = '') => value != null && Number.isSafeInteger(value) && value >= 0 ? `${value.toLocaleString()}${unit ? ` ${unit}` : ''}` : 'Unknown / unavailable';
