import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import '../../styles/tokens.css';
import '../../styles/components.css';
import { contrastRatio, findStyleRule, readToken, resolveCustomPropertyColor } from '../../test/color';
import { Button } from './Button';

describe('destructive button styling', () => {
  it('uses an accessible light-on-red foreground pairing at rest and on hover', () => {
    render(<Button variant="destructive">Delete endpoint</Button>);

    const foreground = readToken('--destructive-foreground');
    const defaultBackground = readToken('--destructive-control-default');
    const hoverBackground = readToken('--destructive-control-hover');
    const styles = getComputedStyle(screen.getByRole('button', { name: 'Delete endpoint' }));
    const destructiveRule = findStyleRule('.ui-button-destructive');
    const destructiveHoverRule = findStyleRule('.ui-button-destructive:hover');
    expect(destructiveRule?.style.color).toBe('var(--destructive-foreground)');
    expect(destructiveRule?.style.background).toBe('var(--destructive-control-default)');
    expect(destructiveHoverRule?.style.background).toBe('var(--destructive-control-hover)');
    expect(styles.color).toBe('var(--destructive-foreground)');
    expect(foreground).toBe('#ffffff');
    expect(defaultBackground).toBe('#e80054');
    expect(hoverBackground).toBe('#c4004a');
    expect(resolveCustomPropertyColor(destructiveRule?.style.background ?? '')).toBe(defaultBackground);
    expect(resolveCustomPropertyColor(destructiveHoverRule?.style.background ?? '')).toBe(hoverBackground);
    expect(resolveCustomPropertyColor(styles.color)).toBe(foreground);
    expect(contrastRatio(defaultBackground, foreground)).toBeGreaterThanOrEqual(4.5);
    expect(contrastRatio(hoverBackground, foreground)).toBeGreaterThanOrEqual(4.5);
  });
});
