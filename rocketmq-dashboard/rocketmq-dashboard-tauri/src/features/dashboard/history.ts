export function localHistoryDay(date: string): { beginMs: number; endMs: number } {
    const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(date);
    if (!match) throw new Error('Select a valid local calendar date.');
    const [year, month, day] = match.slice(1).map(Number);
    const begin = new Date(year, month - 1, day);
    if (begin.getFullYear() !== year || begin.getMonth() !== month - 1 || begin.getDate() !== day || begin.getTime() < 0) throw new Error('Invalid history date.');
    return { beginMs: begin.getTime(), endMs: new Date(year, month - 1, day + 1).getTime() };
}

export const todayLocal = () => {
    const date = new Date();
    return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;
};
