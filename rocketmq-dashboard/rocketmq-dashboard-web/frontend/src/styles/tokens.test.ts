import { describe, expect, it } from 'vitest';
import './tokens.css';
import { contrastRatio, readToken } from '../test/color';

const MIN_TEXT_CONTRAST = 4.5;

describe('color token contrast', () => {
  it.each([
    ['--foreground', '--background'],
    ['--foreground', '--card'],
    ['--muted-foreground', '--card'],
    ['--primary-foreground', '--primary'],
    ['--primary-foreground', '--primary-hover'],
    ['--selection-foreground', '--selection'],
    ['--destructive-foreground', '--destructive-control-default'],
    ['--destructive-foreground', '--destructive-control-hover'],
    ['--link', '--card'],
    ['--destructive', '--card']
  ])('%s is readable on %s', (foregroundToken, backgroundToken) => {
    const foreground = readToken(foregroundToken);
    const background = readToken(backgroundToken);

    expect(foreground).toMatch(/^#[0-9a-f]{6}$/);
    expect(background).toMatch(/^#[0-9a-f]{6}$/);
    expect(contrastRatio(background, foreground)).toBeGreaterThanOrEqual(MIN_TEXT_CONTRAST);
  });
});
