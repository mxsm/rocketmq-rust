export function changedBrokerConfig(text: string, original: Record<string, string>): Record<string, string> {
    const parsed: unknown = JSON.parse(text);
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) throw new Error('Configuration must be a JSON object of string values.');
    const entries = Object.entries(parsed);
    for (const [key, value] of entries) {
        if (!key || /[\s=:# !\\\x00-\x1f\x7f]/.test(key) || typeof value !== 'string' || /[\r\n\0]/.test(value)) {
            throw new Error('Use non-empty property names and single-line string values.');
        }
    }
    if (Object.keys(original).some(key => !Object.prototype.hasOwnProperty.call(parsed, key))) throw new Error('Removing configuration keys is not supported. Restore the missing keys.');
    return Object.fromEntries(entries.filter(([key, value]) => original[key] !== value)) as Record<string, string>;
}
