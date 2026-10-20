function relativeLuminance(hex: string) {
  const channels = hex.slice(1).match(/.{2}/g)?.map((channel) => Number.parseInt(channel, 16) / 255) ?? [];
  const [red, green, blue] = channels.map((channel) => (
    channel <= 0.03928 ? channel / 12.92 : ((channel + 0.055) / 1.055) ** 2.4
  ));
  return (0.2126 * red) + (0.7152 * green) + (0.0722 * blue);
}

/** WCAG contrast ratio of two opaque `#rrggbb` colors. */
export function contrastRatio(background: string, foreground: string) {
  const [lighter, darker] = [relativeLuminance(background), relativeLuminance(foreground)].sort((left, right) => right - left);
  return (lighter + 0.05) / (darker + 0.05);
}

export function findStyleRule(selector: string) {
  return Array.from(document.styleSheets)
    .flatMap((styleSheet) => Array.from(styleSheet.cssRules))
    .find((rule): rule is CSSStyleRule => rule instanceof CSSStyleRule && rule.selectorText === selector);
}

export function readToken(name: string) {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

export function resolveCustomPropertyColor(value: string) {
  const variable = value.match(/^var\((--[^)]+)\)$/)?.[1];
  return variable ? readToken(variable) : value;
}
