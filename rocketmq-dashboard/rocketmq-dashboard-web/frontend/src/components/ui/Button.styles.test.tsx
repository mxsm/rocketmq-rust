import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import '../../styles/tokens.css';
import '../../styles/components.css';
import { Button } from './Button';

function relativeLuminance(hex: string) {
  const channels = hex.slice(1).match(/.{2}/g)?.map((channel) => Number.parseInt(channel, 16) / 255) ?? [];
  const [red, green, blue] = channels.map((channel) => (
    channel <= 0.03928 ? channel / 12.92 : ((channel + 0.055) / 1.055) ** 2.4
  ));
  return (0.2126 * red) + (0.7152 * green) + (0.0722 * blue);
}

function contrastRatio(background: string, foreground: string) {
  const [lighter, darker] = [relativeLuminance(background), relativeLuminance(foreground)].sort((left, right) => right - left);
  return (lighter + 0.05) / (darker + 0.05);
}

function findStyleRule(selector: string) {
  return Array.from(document.styleSheets)
    .flatMap((styleSheet) => Array.from(styleSheet.cssRules))
    .find((rule): rule is CSSStyleRule => rule instanceof CSSStyleRule && rule.selectorText === selector);
}

function resolveCustomPropertyColor(value: string) {
  const variable = value.match(/^var\((--[^)]+)\)$/)?.[1];
  return variable ? getComputedStyle(document.documentElement).getPropertyValue(variable).trim() : value;
}

describe('destructive button styling', () => {
  it('uses an accessible dark-red foreground pairing at rest and on hover', () => {
    render(<Button variant="destructive">Delete endpoint</Button>);

    const foreground = getComputedStyle(document.documentElement).getPropertyValue('--foreground').trim();
    const defaultBackground = getComputedStyle(document.documentElement).getPropertyValue('--destructive-control-default').trim();
    const hoverBackground = getComputedStyle(document.documentElement).getPropertyValue('--destructive-control-hover').trim();
    const styles = getComputedStyle(screen.getByRole('button', { name: 'Delete endpoint' }));
    const destructiveRule = findStyleRule('.ui-button-destructive');
    const destructiveHoverRule = findStyleRule('.ui-button-destructive:hover');
    expect(findStyleRule('.ui-button-destructive')?.style.color).toBe('var(--foreground)');
    expect(destructiveRule?.style.background).toBe('var(--destructive-control-default)');
    expect(destructiveHoverRule?.style.background).toBe('var(--destructive-control-hover)');
    expect(styles.color).toBe('var(--foreground)');
    expect(foreground).toBe('#f4f7fa');
    expect(defaultBackground).toBe('#b91c1c');
    expect(hoverBackground).toBe('#991b1b');
    expect(resolveCustomPropertyColor(destructiveRule?.style.background ?? '')).toBe(defaultBackground);
    expect(resolveCustomPropertyColor(destructiveHoverRule?.style.background ?? '')).toBe(hoverBackground);
    expect(resolveCustomPropertyColor(styles.color)).toBe(foreground);
    expect(contrastRatio(defaultBackground, foreground)).toBeGreaterThanOrEqual(4.5);
    expect(contrastRatio(hoverBackground, foreground)).toBeGreaterThanOrEqual(4.5);
  });
});
